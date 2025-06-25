import logging
import json
import shutil
import hashlib
import io
import threading
import time
import gc
from collections import Counter
from datetime import datetime
from tqdm import tqdm
from pathlib import Path
from typing import Dict, Optional, List
from PIL import Image
from datasets import Dataset, load_from_disk
from datasets.features import Image as ImageFeature
from functools import partial

from utils.logging import setup_logging

class NASDataProcessor:
    
    def __init__(
        self,
        base_path: str = "/mnt/AI_NAS/datalake/",
        log_level: str = "INFO",
        num_proc: int = 4,
        batch_size: int = 1000,  # map()의 배치 크기
    ):
        # 경로 설정
        self.base_path = Path(base_path)
        self.staging_path = self.base_path / "staging"
        self.staging_pending_path = self.staging_path / "pending"
        self.staging_processing_path = self.staging_path / "processing"
        self.staging_failed_path = self.staging_path / "failed"
        
        self.catalog_path = self.base_path / "catalog"
        self.assets_path = self.base_path / "assets"
        
        self.num_proc = num_proc
        self.batch_size = batch_size
        
        # LocalDataManager와 동일
        self.image_data_key = 'image'  # 기본 이미지 컬럼 키
        self.file_path_key = 'file_path'  # 기본 파일 경로 컬럼 키
        
        self._check_path_and_setup_logging(log_level)
        
        self.existing_hashes = set()
        self.cache_built = False
        self.cache_lock = threading.Lock()
        
        # 처리 실패 추적용
        self.processing_failed = False
        self.failure_lock = threading.Lock()
        self.error_messages = []
        
        self.logger.info(f"🚀 NASDataProcessor 초기화 (병렬: {self.num_proc}, 배치: {batch_size})")
 
    def get_status(self) -> Dict:
        """간단한 상태 조회"""
        return {
            "pending": len(list(self.staging_pending_path.glob("*"))) if self.staging_pending_path.exists() else 0,
            "processing": len(list(self.staging_processing_path.glob("*"))) if self.staging_processing_path.exists() else 0,
            "failed": len(list(self.staging_failed_path.glob("*"))) if self.staging_failed_path.exists() else 0
        }
    
    def process_all_pending(self) -> Dict:
        """모든 Pending 데이터 처리 (에러 정보 포함)"""
        self.logger.info("🔄 Pending 데이터 처리 시작")
        
        if not self.staging_pending_path.exists():
            return {
                "success": 0, 
                "failed": 0, 
                "message": "Pending 디렉토리 없음",
                "errors": [],
                "success_details": [],
                "failed_details": []
            }
        
        pending_dirs = [
            d for d in self.staging_pending_path.iterdir()
            if d.is_dir() and (d / "upload_metadata.json").exists()
        ]
        
        if not pending_dirs:
            return {
                "success": 0, 
                "failed": 0, 
                "message": "처리할 데이터 없음",
                "errors": [],
                "success_details": [],
                "failed_details": []
            }
        
        self.logger.info(f"📦 처리 대상: {len(pending_dirs)}개")
        
        success_count = 0
        failed_count = 0
        success_details = []
        failed_details = []
        error_summary = []
        
        for pending_dir in pending_dirs:
            processing_dir = None
            dir_name = pending_dir.name
            
            try:
                # processing으로 이동
                processing_dir = self.staging_processing_path / dir_name
                shutil.move(str(pending_dir), str(processing_dir))
                
                # 처리 실패 플래그 초기화
                self.processing_failed = False
                self.error_messages = []
                
                # 처리
                self._process_single_directory(processing_dir)
                
                # 처리 중 에러가 있었는지 확인
                if self.processing_failed or self.error_messages:
                    # 내부 처리 실패
                    error_msg = "; ".join(self.error_messages) if self.error_messages else "처리 중 알 수 없는 오류"
                    raise Exception(f"내부 처리 실패: {error_msg}")
                
                # 성공 시 정리
                shutil.rmtree(processing_dir)
                success_count += 1
                
                success_info = {
                    "directory": dir_name,
                    "status": "success",
                    "timestamp": datetime.now().isoformat(),
                }
                success_details.append(success_info)
                
                self.logger.info(f"✅ 완료: {dir_name}")
                
            except Exception as e:
                failed_count += 1
                error_msg = str(e)
                
                # 상세 에러 정보 수집
                error_info = {
                    "directory": dir_name,
                    "error": error_msg,
                    "error_type": type(e).__name__,
                    "timestamp": datetime.now().isoformat(),
                }
                
                # 추가 에러 컨텍스트 수집
                try:
                    if processing_dir and processing_dir.exists():
                        error_info["processing_dir_exists"] = True
                        # 메타데이터 파일 확인
                        metadata_file = processing_dir / "upload_metadata.json"
                        if metadata_file.exists():
                            error_info["metadata_exists"] = True
                            try:
                                with open(metadata_file, 'r', encoding='utf-8') as f:
                                    metadata = json.load(f)
                                    error_info["metadata_info"] = {
                                        "file_count": len(metadata.get("files", [])),
                                        "upload_time": metadata.get("upload_time", "unknown")
                                    }
                            except:
                                error_info["metadata_read_error"] = True
                        else:
                            error_info["metadata_exists"] = False
                    else:
                        error_info["processing_dir_exists"] = False
                        
                except Exception as context_error:
                    error_info["context_collection_error"] = str(context_error)
                
                failed_details.append(error_info)
                error_summary.append(f"{dir_name}: {error_msg}")
                
                self.logger.error(f"❌ 실패: {dir_name} - {error_msg}")
                
                # Failed 디렉토리로 이동
                if processing_dir and processing_dir.exists():
                    failed_dir = self.staging_failed_path / dir_name
                    failed_dir.parent.mkdir(mode=0o775, parents=True, exist_ok=True)
                    try:
                        # 에러 정보를 파일로 저장
                        error_file = failed_dir.parent / f"{dir_name}_error.json"
                        shutil.move(str(processing_dir), str(failed_dir))
                        
                        # 에러 정보 저장
                        with open(error_file, 'w', encoding='utf-8') as f:
                            json.dump(error_info, f, ensure_ascii=False, indent=2)
                            
                    except Exception as move_error:
                        move_error_msg = str(move_error)
                        error_info["move_error"] = move_error_msg
                        self.logger.error(f"Failed 디렉토리 이동 실패: {move_error_msg}")
        remain_processing_dirs = [
            d for d in self.staging_processing_path.iterdir()
            if d.is_dir() and not (d / "upload_metadata.json").exists()
        ]
        for remain_dir in remain_processing_dirs:
            try:
                shutil.rmtree(remain_dir)
                self.logger.info(f"✅ 처리 중 디렉토리 정리: {remain_dir.name}")
            except Exception as e:
                self.logger.error(f"❌ 처리 중 디렉토리 정리 실패: {remain_dir.name} - {str(e)}")
        most_common_errors = []
        if failed_details:
            error_types = [detail.get("error_type", "Unknown") for detail in failed_details]
            most_common = Counter(error_types).most_common(3)
            most_common_errors = [{"error_type": error_type, "count": count} for error_type, count in most_common]
        # 결과 구성
        result = {
            "success": success_count,
            "failed": failed_count,
            "total_processed": success_count + failed_count,
            "success_details": success_details,
            "failed_details": failed_details,
            "errors": error_summary,
            "summary": {
                "success_rate": f"{(success_count/(success_count + failed_count)*100):.1f}%" if (success_count + failed_count) > 0 else "0%",
                "most_common_errors": most_common_errors,
                "processing_time": datetime.now().isoformat(),
            }
        }
        
        if failed_count > 0:
            result["message"] = f"처리 완료: {success_count}개 성공, {failed_count}개 실패"
        else:
            result["message"] = f"모든 데이터 처리 성공: {success_count}개"
        
        return result
    
    def _check_path_and_setup_logging(self, log_level: str = "INFO"):
        
        required_paths = {
            'base': self.base_path,
            'staging': self.staging_path,
            'staging/pending': self.staging_pending_path,
            'staging/processing': self.staging_processing_path, 
            'staging/failed': self.staging_failed_path,
            'catalog': self.catalog_path,
            'assets': self.assets_path,
        }
        
        missing_paths = []
        for path_name, path_obj in required_paths.items():
            if not path_obj.exists():
                missing_paths.append(f"  - {path_name}: {path_obj}")
        
        if missing_paths:
            missing_list = '\n'.join(missing_paths)
            raise FileNotFoundError(f"❌ 필수 디렉토리가 없습니다:\n{missing_list}")
        setup_logging(
            user_id="processor",
            log_level=log_level, 
            base_path=str(self.base_path)
        )
        self.logger = logging.getLogger(__name__)
        self.logger.debug("✅ 모든 필수 디렉토리 확인 완료")
   
    def _process_single_directory(self, processing_dir: Path):
        """단일 디렉토리 처리 - datasets 라이브러리 활용"""
        # 메타데이터 읽기
        metadata_file = processing_dir / "upload_metadata.json"
        if not metadata_file.exists():
            raise ValueError("메타데이터 파일 없음")
        
        with open(metadata_file, encoding='utf-8') as f:
            metadata = json.load(f)
        
        # datasets로 로드
        dataset_obj = load_from_disk(str(processing_dir))
        self.logger.info(f"{processing_dir.name} 데이터셋 로드 완료: {len(dataset_obj)}개 행")
        self.logger.debug(f"데이터셋 컬럼: {dataset_obj.column_names}")
        
        provider = metadata['provider']
        dataset_name = metadata['dataset']
        assets_base = self.assets_path / f"provider={provider}" / f"dataset={dataset_name}"
        # 해시 캐시 구축 (공통)
        self._build_hash_cache(assets_base)

        # 이미지 처리
        if metadata.get('has_images', False) and self.image_data_key in dataset_obj.column_names:
            dataset_obj = self._process_images_with_map(dataset_obj, metadata, assets_base)
        
        # 파일 처리
        if metadata.get('has_files', False) and self.file_path_key in dataset_obj.column_names:
            dataset_obj = self._process_files_with_map(dataset_obj, metadata, assets_base)
        
        # Catalog에 저장
        self._save_to_catalog(dataset_obj, metadata)
        
        # 메모리 정리
        del dataset_obj
        gc.collect()
    
    def _process_images_with_map(self, dataset_obj: Dataset, metadata: Dict, assets_base: Path) -> Dataset:
        """이미지 처리 (PIL Image/bytes → hash.jpg)"""
        total_images = len(dataset_obj)
        self.logger.info(f"🖼️ 이미지 처리 시작: {self.image_data_key} ({total_images}개)")

        shard_config = self._get_shard_config(total_images)
        self.logger.info(f"🔧 샤딩 설정: {shard_config}")
        
        dataset_obj = dataset_obj.cast_column(self.image_data_key, ImageFeature())
        assets_base.mkdir(mode=0o775, parents=True, exist_ok=True)
        process_batch_func = partial(
            self._process_image_batch,
            assets_base=assets_base,
            shard_config=shard_config
        )

        try:
            processed_dataset = dataset_obj.map(
                process_batch_func,
                batched=True,
                batch_size=self.batch_size,
                num_proc=min(self.num_proc, total_images // self.batch_size + 1),  # 최소 1개 프로세스
                remove_columns=[self.image_data_key],  # 원본 이미지 컬럼 제거
                desc="🖼️ 이미지 처리",
                load_from_cache_file=False,  # 캐시 비활성화로 메모리 절약
            )
            self.logger.debug(f"처리된 데이터셋 컬럼: {processed_dataset.column_names}")
            # 처리 중 실패가 있었는지 확인
            if self.processing_failed:
                error_summary = f"이미지 처리 실패: {'; '.join(self.error_messages[:5])}"
                raise RuntimeError(error_summary)
                
            self.logger.info(f"✅ 이미지 변환 완료: {len(processed_dataset)}개")
            return processed_dataset
            
        except Exception as e:
            self.logger.error(f"❌ datasets.map() 처리 실패: {e}")
            raise
        
        
    def _process_files_with_map(self, dataset_obj: Dataset, metadata: Dict, assets_base: Path) -> Dataset:
        """파일 처리 (staging/assets → final/assets + hash)"""
        print(metadata)
        total_files = len(dataset_obj)
        self.logger.info(f"📄 파일 처리 시작: {self.file_path_key} ({total_files}개)")
        
        shard_config = self._get_shard_config(total_files)
        self.logger.info(f"🔧 샤딩 설정: {shard_config}")
        assets_base.mkdir(mode=0o775, parents=True, exist_ok=True)
        process_batch_func = partial(
            self._process_file_batch,
            assets_base=assets_base,
            shard_config=shard_config,
        )
        
        try:
            processed_dataset = dataset_obj.map(
                process_batch_func,
                batched=True,
                batch_size=self.batch_size,
                num_proc=min(self.num_proc, total_files // self.batch_size + 1),  # 최소 1개 프로세스
                remove_columns=[self.file_path_key],  # 원본 파일 경로 컬럼 제거
                desc="📄 파일 이동",
                load_from_cache_file=False,
            )
            
            self.logger.info(f"✅ 파일 이동 완료: {len(processed_dataset)}개")
            return processed_dataset
        except Exception as e:
            self.logger.error(f"❌ 파일 처리 실패: {e}")
            raise
    
    def _process_image_batch(self, batch: Dict, assets_base: Path, shard_config: Dict) -> Dict:
        """배치 단위 이미지 처리 (PIL Image/bytes → hash.jpg)"""
        images = batch[self.image_data_key]
        self.logger.debug(f"배치 처리: {len(images)}개 이미지")        
        image_hashes = []
        image_paths = []
        
        saved_count = 0
        duplicate_count = 0
        
        for idx, image_data in enumerate(images):
            try:
                # 실패 시 중단
                if self.processing_failed:
                    break
                
                if image_data is None:
                    image_hashes.append(None)
                    image_paths.append(None)
                    continue
                
                # PIL Image 처리
                pil_image = image_data if hasattr(image_data, 'save') else Image.open(io.BytesIO(image_data))
                
                image_hash  = self._get_image_hash(pil_image)
                image_path = self._get_level_path(assets_base, shard_config, image_hash)
                if image_hash in self.existing_hashes:
                    duplicate_count += 1    
                else:
                    image_path.parent.mkdir(mode=0o775, parents=True, exist_ok=True)
                    
                    if pil_image.mode != 'RGB':
                        pil_image = pil_image.convert('RGB')
                    pil_image.save(str(image_path), 'JPEG', quality=95)
                    
                    # 캐시에 추가 (thread-safe)
                    with self.cache_lock:
                        self.existing_hashes.add(image_hash)
                    
                    saved_count += 1
                
                relative_path = str(image_path.relative_to(self.assets_path))
                image_hashes.append(image_hash)
                image_paths.append(relative_path)
                
            except Exception as e:
                # 이미지 처리 실패 시 전체 실패로 마킹
                with self.failure_lock:
                    if not self.processing_failed:
                        self.processing_failed = True
                        error_msg = f"이미지 {idx} 처리 실패: {str(e)}"
                        self.error_messages.append(error_msg)
                        self.logger.error(f"❌ {error_msg}")
                
                # 실패 즉시 중단
                raise RuntimeError(f"이미지 처리 실패: {str(e)}")
        
        # 로그 출력 (배치별)
        if saved_count > 0 or duplicate_count > 0:
            self.logger.debug(f"배치 처리: 저장={saved_count}, 중복={duplicate_count}")
        
        return {
            "path": image_paths,
            "hash": image_hashes,
        }
        
    def _process_file_batch(self, batch: Dict, assets_base: Path, shard_config: Dict) -> Dict:
        """배치 단위 파일 처리 (staging/assets → final/assets + hash)"""
        
        file_paths = batch[self.file_path_key]
        self.logger.debug(f"배치 파일 처리: {len(file_paths)}개")
        file_hashes = []
        new_file_paths = []
        
        saved_count = 0
        duplicate_count = 0
        # print iterdir staging_pending_path
        for idx, file_path in enumerate(file_paths):
            try:
                if self.processing_failed:
                    break
                
                if file_path is None:
                    file_hashes.append(None)
                    file_paths.append(None)
                    continue
                    
                file_path = self.staging_processing_path / file_path
                if not file_path.exists():
                    raise FileNotFoundError(f"파일이 존재하지 않습니다: {file_path}")
                
                file_hash = self._get_file_hash(file_path)
                new_file_path = self._get_level_path(assets_base, shard_config, file_hash)
                if file_hash in self.existing_hashes:
                    duplicate_count += 1
                else:
                    new_file_path.parent.mkdir(mode=0o775, parents=True, exist_ok=True)
                    shutil.move(str(file_path), str(new_file_path))
                    with self.cache_lock:
                        self.existing_hashes.add(file_hash)
                    saved_count += 1
                relative_path = str(new_file_path.relative_to(self.assets_path))
                file_hashes.append(file_hash)
                new_file_paths.append(relative_path)
                    
            except Exception as e:
                with self.failure_lock:
                    if not self.processing_failed:
                        self.processing_failed = True
                        error_msg = f"파일 {idx} 처리 실패: {str(e)}"
                        self.error_messages.append(error_msg)
                        self.logger.error(f"❌ {error_msg}")
                
                # 실패 즉시 중단
                raise RuntimeError(f"파일 처리 실패: {str(e)}")
        
        if saved_count > 0 or duplicate_count > 0:
            self.logger.debug(f"배치 파일 처리: 저장={saved_count}, 중복={duplicate_count}")

        return {
            "path": new_file_paths,
            "hash": file_hashes
        }
        
    def _build_hash_cache(self, assets_base: Path):
        """기존 이미지 해시 캐시 구축"""
        if self.cache_built:
            return
            
        with self.cache_lock:
            if self.cache_built:
                return
            start_time = time.time()
            
            # 모든 .jpg 파일에서 해시 추출
            for image_file in assets_base.rglob("*.jpg"):
                hash_from_filename = image_file.stem
                if len(hash_from_filename) == 64:  # SHA256 길이 검증
                    self.existing_hashes.add(hash_from_filename)
            
            build_time = time.time() - start_time
            self.logger.info(f"🔍 기존 이미지 해시 캐시 구축 완료: {len(self.existing_hashes)}개, 시간: {build_time:.2f}초")
            self.cache_built = True
    
    @staticmethod
    def _get_image_hash(pil_image: Image.Image) -> str:
        """이미지 해시 계산"""
        if pil_image.mode != 'RGB':
            pil_image = pil_image.convert('RGB')
        
        img_buffer = io.BytesIO()
        pil_image.save(img_buffer, format='JPEG', quality=95)
        jpeg_bytes = img_buffer.getvalue()
        return hashlib.sha256(jpeg_bytes).hexdigest()
    @staticmethod
    def _get_file_hash(file_path: Path) -> str:
        """파일 해시 계산 (SHA256)"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    @staticmethod
    def _get_shard_config(total_images: int) -> Dict:
        
        if total_images < 10000:
            # 샤딩 없음
            return {"levels": 0, "dirs": 1}
        elif total_images < 2500000:  # 256 * 10000
            # 1단계: xx/ (256개 폴더)
            return {"levels": 1, "dirs": 256}
        else:
            # 2단계: xx/xx/ (65536개 폴더)  
            return {"levels": 2, "dirs": 65536}
    @staticmethod
    def _get_level_path(base_path: Path, shard_config: Dict, image_hash: str) -> Path:
        
        levels = shard_config["levels"]
        
        if levels == 0:
            return base_path / f"{image_hash}.jpg"
        elif levels == 1:
            return base_path / image_hash[:2] / f"{image_hash}.jpg"
        elif levels == 2:  
            return base_path / image_hash[:2] / image_hash[2:4] / f"{image_hash}.jpg"
    
    def _save_to_catalog(self, dataset_obj: Dataset, metadata: Dict):
        """Catalog에 저장"""
        provider = metadata['provider']
        dataset_name = metadata['dataset']
        task = metadata['task']
        variant = metadata['variant']
        
        # Catalog 경로
        catalog_dir = (
            self.catalog_path /
            f"provider={provider}" /
            f"dataset={dataset_name}" /
            f"task={task}" /
            f"variant={variant}"
        )
        catalog_dir.mkdir(mode=0o775, parents=True, exist_ok=True)
        
        # Parquet 저장 (datasets 내장 최적화)
        parquet_file = catalog_dir / "data.parquet"
        dataset_obj.to_parquet(str(parquet_file))
        
        # 메타데이터 저장
        metadata_file = catalog_dir / "_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        # 파일 크기 로그
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        self.logger.info(f"💾 저장 완료: {parquet_file.name} ({file_size_mb:.1f}MB, {len(dataset_obj)}행)")
        
if __name__ == "__main__":
    # datasets.map() 활용 버전
    processor = NASDataProcessor(
        batch_size=1000,    # map()의 배치 크기
        num_proc=4         # 병렬 처리 수
    )
    
    # 상태 확인
    status = processor.get_status()
    print(f"현재 상태: {status}")
    
    # 처리 실행
    result = processor.process_all_pending()
    print(f"처리 결과: {result}")