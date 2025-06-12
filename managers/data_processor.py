import logging
import json
import shutil
import hashlib
import io
import threading
import time
import os
from datetime import datetime
from tqdm import tqdm
from pathlib import Path
from typing import Dict, Optional, List
import gc

from datasets import Dataset, load_from_disk
from datasets.features import Image as ImageFeature
from PIL import Image

class NASDataProcessor:
    
    def __init__(
        self,
        base_path: str = "/mnt/AI_NAS/datalake/migrate_test",
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
        self.image_column_candidates = ["image", "image_bytes"]
        
        self._setup_console_logging(log_level)
        self._check_path_and_setup_logging()
        
        self.existing_hashes = set()
        self.cache_built = False
        self.cache_lock = threading.Lock()
        
        # 처리 실패 추적용
        self.processing_failed = False
        self.failure_lock = threading.Lock()
        self.error_messages = []
        
        self.logger.info(f"🚀 OptimizedNASDataProcessor 초기화 (병렬: {self.num_proc}, 배치: {batch_size})")

    def _setup_console_logging(self, log_level: str) -> logging.Logger:
        """기본 로깅 설정"""
        
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(console_handler)
        
        if log_level.upper() == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    def _check_path_and_setup_logging(self):
        
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
            
        self.logger.info("✅ 모든 필수 디렉토리 확인 완료")
        
        log_dir = self.base_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        date_str = datetime.now().strftime("%Y%m%d")
        user = os.getenv('USER', 'unknown')
        log_file = log_dir / f"DataProcessor_{date_str}_{user}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)
        self.logger.info(f"📝 파일 로깅 활성화: {log_file}")
        self.logger.info(f"🚀 DataProcessor 초기화 완료")
    
    def get_status(self) -> Dict:
        """간단한 상태 조회"""
        return {
            "pending": len(list(self.staging_pending_path.glob("*"))) if self.staging_pending_path.exists() else 0,
            "processing": len(list(self.staging_processing_path.glob("*"))) if self.staging_processing_path.exists() else 0,
            "failed": len(list(self.staging_failed_path.glob("*"))) if self.staging_failed_path.exists() else 0
        }
    
    def process_all_pending(self) -> Dict:
        """모든 Pending 데이터 처리"""
        self.logger.info("🔄 Pending 데이터 처리 시작")
        
        if not self.staging_pending_path.exists():
            return {"success": 0, "failed": 0, "message": "Pending 디렉토리 없음"}
        
        pending_dirs = [d for d in self.staging_pending_path.iterdir() if d.is_dir()]
        
        if not pending_dirs:
            return {"success": 0, "failed": 0, "message": "처리할 데이터 없음"}
        
        self.logger.info(f"📦 처리 대상: {len(pending_dirs)}개")
        
        success_count = 0
        failed_count = 0
        
        for pending_dir in pending_dirs:
            processing_dir = None
            try:
                # processing으로 이동
                processing_dir = self.staging_processing_path / pending_dir.name
                shutil.move(str(pending_dir), str(processing_dir))
                
                # 처리 실패 플래그 초기화
                self.processing_failed = False
                self.error_messages = []
                
                # 처리
                self._process_single_directory(processing_dir)
                
                # 성공 시 정리
                shutil.rmtree(processing_dir)
                success_count += 1
                
                self.logger.info(f"✅ 완료: {pending_dir.name}")
                
            except Exception as e:
                failed_count += 1
                self.logger.error(f"❌ 실패: {pending_dir.name} - {e}")
                
                # 실패 시 failed로 이동
                if processing_dir and processing_dir.exists():
                    failed_dir = self.staging_failed_path / pending_dir.name
                    failed_dir.parent.mkdir(exist_ok=True)
                    try:
                        shutil.move(str(processing_dir), str(failed_dir))
                    except Exception as move_error:
                        self.logger.error(f"Failed 디렉토리 이동 실패: {move_error}")
                        if processing_dir.exists():
                            shutil.rmtree(processing_dir)
        
        result = {"success": success_count, "failed": failed_count}
        self.logger.info(f"✅ 처리 완료: {result}")
        return result
    
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
        self.logger.info(f"📂 데이터 로드: {len(dataset_obj)}행")
        
        # 이미지 처리 (Raw 데이터인 경우)
        if metadata.get('data_type') == 'raw' and metadata.get('has_images', False):
            dataset_obj = self._process_images_with_map(dataset_obj, metadata)
        
        # Catalog에 저장
        self._save_to_catalog(dataset_obj, metadata)
        
        # 메모리 정리
        del dataset_obj
        gc.collect()
    
    def _process_images_with_map(self, dataset_obj: Dataset, metadata: Dict) -> Dataset:
        """datasets.map()을 활용한 이미지 처리"""
        # 이미지 컬럼 찾기
        image_column = None
        for col in dataset_obj.column_names:
            if col.lower() in self.image_column_candidates:
                image_column = col
                break
        
        if not image_column:
            raise ValueError("이미지 컬럼을 찾을 수 없음")
        
        total_images = len(dataset_obj)
        self.logger.info(f"🖼️ 이미지 처리 시작: {image_column} ({total_images}개)")
        
        # Assets 경로 설정
        provider = metadata['provider']
        dataset_name = metadata['dataset']
        self.assets_base = self.assets_path / f"provider={provider}" / f"dataset={dataset_name}"
        self.assets_base.mkdir(parents=True, exist_ok=True)
        
        self.shard_config = self._get_shard_config(total_images)
        self.logger.info(f"🔧 샤딩 설정: {self.shard_config['info']}")
        
        # 해시 캐시 구축
        self._build_hash_cache(self.assets_base)
        
        # Image feature로 캐스팅
        dataset_obj = dataset_obj.cast_column(image_column, ImageFeature())
        
        # datasets.map()으로 배치 처리
        try:
            processed_dataset = dataset_obj.map(
                self._process_image_batch,
                batched=True,
                batch_size=self.batch_size,
                num_proc=self.num_proc,
                remove_columns=[image_column],  # 원본 이미지 컬럼 제거
                desc="🖼️ 이미지 처리",
                load_from_cache_file=False,  # 캐시 비활성화로 메모리 절약
            )
            
            # 처리 중 실패가 있었는지 확인
            if self.processing_failed:
                error_summary = f"이미지 처리 실패: {'; '.join(self.error_messages[:5])}"
                raise RuntimeError(error_summary)
                
            self.logger.info(f"✅ 이미지 변환 완료: {len(processed_dataset)}개")
            return processed_dataset
            
        except Exception as e:
            self.logger.error(f"❌ datasets.map() 처리 실패: {e}")
            raise
    
    def _process_image_batch(self, batch: Dict) -> Dict:
        """배치 단위 이미지 처리 함수 (datasets.map용)"""
        # 이미지 컬럼 이름 찾기
        image_column = None
        for col in batch.keys():
            if col.lower() in self.image_column_candidates:
                image_column = col
                break
        
        if not image_column:
            raise ValueError("이미지 컬럼을 찾을 수 없음")
        
        images = batch[image_column]
        batch_size = len(images)
        
        # 결과 저장용
        image_hashes = []
        image_paths = []
        
        saved_count = 0
        duplicate_count = 0
        
        for idx, image_data in enumerate(images):
            try:
                # 실패 플래그 확인
                if self.processing_failed:
                    break
                
                if image_data is None:
                    image_hashes.append(None)
                    image_paths.append(None)
                    continue
                
                # PIL Image 처리
                pil_image = image_data if hasattr(image_data, 'save') else Image.open(io.BytesIO(image_data))
                
                # 해시 계산
                image_hash = self._get_image_hash(pil_image)
                
                # 중복 체크
                if image_hash in self.existing_hashes:
                    duplicate_count += 1
                    image_path = self._get_image_path(self.assets_base, image_hash, self.shard_config)
                    relative_path = str(image_path.relative_to(self.assets_path))
                else:
                    # 새 이미지 저장
                    image_path = self._get_image_path(self.assets_base, image_hash, self.shard_config)
                    image_path.parent.mkdir(parents=True, exist_ok=True)
                    
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
            "image_hash": image_hashes,
            "image_path": image_paths
        }
    
    def _build_hash_cache(self, assets_base: Path):
        """기존 이미지 해시 캐시 구축"""
        if self.cache_built:
            return
            
        with self.cache_lock:
            if self.cache_built:
                return
            
            self.logger.info("🔍 기존 이미지 해시 캐시 구축 중...")
            start_time = time.time()
            
            # 모든 .jpg 파일에서 해시 추출
            for image_file in assets_base.rglob("*.jpg"):
                hash_from_filename = image_file.stem
                if len(hash_from_filename) == 64:  # SHA256 길이 검증
                    self.existing_hashes.add(hash_from_filename)
            
            build_time = time.time() - start_time
            self.logger.info(f"✅ 해시 캐시 구축 완료: {len(self.existing_hashes)}개 ({build_time:.2f}초)")
            self.cache_built = True
            
    def _get_image_hash(self, pil_image: Image.Image) -> str:
        """이미지 해시 계산"""
        if pil_image.mode != 'RGB':
            pil_image = pil_image.convert('RGB')
        
        # JPEG 변환 후 해시
        img_buffer = io.BytesIO()
        pil_image.save(img_buffer, format='JPEG', quality=95)
        jpeg_bytes = img_buffer.getvalue()
        
        return hashlib.sha256(jpeg_bytes).hexdigest()
    
    def _get_shard_config(self, total_images: int) -> Dict:
        """샤딩 설정"""
        if total_images < 1000:
            return {"levels": 0, "info": "샤딩 없음"}
        elif total_images < 50000:
            return {"levels": 1, "info": "1단계 샤딩 (xx/)"}
        else:
            return {"levels": 2, "info": "2단계 샤딩 (xx/xx/)"}

    def _get_image_path(self, base_path: Path, image_hash: str, shard_config: Dict) -> Path:
        """샤딩 설정에 따른 경로"""
        levels = shard_config["levels"]
        if levels == 0:
            return base_path / f"{image_hash}.jpg"
        elif levels == 1:
            return base_path / image_hash[:2] / f"{image_hash}.jpg"
        elif levels == 2:  
            return base_path / image_hash[:2] / image_hash[2:4] / f"{image_hash}.jpg"
        else:
            raise ValueError(f"잘못된 샤딩 레벨: {levels}")
    
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
        catalog_dir.mkdir(parents=True, exist_ok=True)
        
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