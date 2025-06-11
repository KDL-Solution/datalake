import logging
import os
import uuid
import json
import shutil
import pandas as pd

from pathlib import Path
from datetime import datetime
from datasets import Dataset, load_from_disk
from datasets.features import Image as ImageFeature
from typing import Dict, Optional
from PIL import Image

import io
import hashlib
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from managers.schema_manager import SchemaManager

class DataManager:
    def __init__(
        self, 
        base_path: str = "/mnt/AI_NAS/datalake/migrate_test",
        log_level: str = "INFO",
        num_proc: int = 8, # 병렬 처리 프로세스 수
    ):
        self.base_path = Path(base_path)
        self.staging_path = self.base_path / "staging"
        self.staging_pending_path = self.staging_path / "pending"
        self.staging_processing_path = self.staging_path / "processing"
        self.staging_failed_path = self.staging_path / "failed"
        self.catalog_path = self.base_path / "catalog"
        self.assets_path  = self.base_path / "assets"
        self.archive_path = self.base_path / "archive"
        self.schema_path = self.base_path / "config" / "schema.yaml"
        self.num_proc = num_proc
        self.image_column_candidates = ['image', 'image_bytes']
        
        
        self._setup_console_logging(log_level)
        self._check_path_and_setup_logging()
        
        self.schema_manager = SchemaManager(
            config_path=self.schema_path,
            create_default=True
        )
      
    def upload_raw_data(
        self,
        data_file: str,
        provider: str,
        dataset: str,
        dataset_description: str = "", # 데이터셋 설명
        original_source: str = "", # 원본 소스 URL 
    ):
        task = "raw"
        variant = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.logger.info(f"📥 Raw data 업로드 시작: {provider}/{dataset}")
        
        if not self.schema_manager.validate_provider(provider):
            raise ValueError(f"❌ 지원하지 않는 provider입니다: {provider}")
        
        self._cleanup_existing_pending(provider, dataset, task, is_raw=True)
        
        dataset_obj, has_images = self._load_data(data_file, process_images=True)
        
        metadata = self._create_metadata(
            provider=provider,
            dataset=dataset,
            task=task,
            variant=variant,
            total_rows=len(dataset_obj),
            data_type="raw",
            source_task=None,  # 원본 작업이므로 None
            has_images=has_images,
            dataset_description=dataset_description,
            original_source=original_source,
        )
        
        staging_dir = self._save_to_staging(dataset_obj, metadata)
        self.logger.info(f"✅ Task 데이터 업로드 완료: {staging_dir}")
        
        return staging_dir

    def upload_task_data(
        self,
        data_file: str,
        provider: str,
        dataset: str,
        task: str,
        variant: str,
        dataset_description: str = "",
        source_task: str = None,
        **kwargs
    ) -> str:
        """Task 데이터 업로드 (기존 catalog에서 특정 task 추출, 이미지 참조만)"""
        self.logger.info(f"📥 Task data 업로드 시작: {provider}/{dataset}/{task}/{variant}")
        
        # 1. Provider 검증
        if not self.schema_manager.validate_provider(provider):
            raise ValueError(f"❌ 지원하지 않는 provider입니다: {provider}")
        
        # 2. Task 메타데이터 검증
        is_valid, error_msg = self.schema_manager.validate_task_metadata(task, kwargs)
        if not is_valid:
            raise ValueError(f"❌ Task 메타데이터 검증 실패: {error_msg}")
        
        # 기존 pending 데이터 정리
        self._cleanup_existing_pending(provider, dataset, task, variant=variant, is_raw=False)
        
        # 데이터 로드 및 컬럼 변환 (이미지 제외)
        dataset_obj, _ = self._load_data(data_file, process_images=False)
        
        # 메타데이터 생성
        metadata = self._create_metadata(
            provider=provider,
            dataset=dataset,
            task=task,
            variant=variant,
            dataset_description=dataset_description,
            source_task=source_task,
            has_images=False,  # 이미지는 참조만
            total_rows=len(dataset_obj),
            data_type='task',
            **kwargs
        )
        
        # Staging에 저장
        staging_dir = self._save_to_staging(dataset_obj, metadata)
        self.logger.info(f"✅ Task 데이터 업로드 완료: {staging_dir}")
        
        return staging_dir
    
    def _create_metadata(
        self,
        provider: str,
        dataset: str,
        task: str,
        variant: str,
        total_rows: int,
        data_type: str,
        source_task: Optional[str],
        has_images: bool = False,
        dataset_description: str = "",
        original_source: str = "",
        # **kwargs
    ) -> Dict:
        """메타데이터 생성"""
        metadata = {
            'provider': provider,
            'dataset': dataset,
            'task': task,
            'variant': variant,
            'data_type': data_type,
            'dataset_description': dataset_description,
            'original_source': original_source,
            'source_task': source_task,
            'has_images': has_images,
            'total_rows': total_rows,
            'uploaded_by': os.getenv('USER', 'unknown'),
            'uploaded_at': datetime.now().isoformat(),
            'file_id': str(uuid.uuid4())[:8],
        }
        # metadata.update(kwargs)
        self.logger.debug(f"📄 메타데이터: {metadata}")
        return metadata

    def _cleanup_existing_pending(
        self, 
        provider: str, 
        dataset: str, 
        task: str, 
        variant: str = None, 
        is_raw: bool = True,
    ):
        """같은 provider/dataset/task 조합의 기존 pending 데이터 정리"""
        pending_path = self.staging_path / "pending"
        
        if not pending_path.exists():
            return
        
        existing_dirs = []
        
        for pending_dir in pending_path.iterdir():
            if not pending_dir.is_dir():
                continue
                
            # 메타데이터로 정확히 확인
            try:
                metadata_file = pending_dir / "upload_metadata.json"
                if metadata_file.exists():
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    basic_match = (
                        metadata.get('provider') == provider and 
                        metadata.get('dataset') == dataset and 
                        metadata.get('task') == task
                    )
                    
                    should_cleanup = False
                    if is_raw:
                        should_cleanup = basic_match
                    else:
                        should_cleanup = basic_match and metadata.get('variant') == variant
                        
                    if should_cleanup:
                        existing_dirs.append(pending_dir)
                        
            except Exception as e:
                self.logger.warning(f"⚠️ 메타데이터 읽기 실패: {pending_dir} - {e}")
                continue
        
        # 기존 데이터 삭제
        if existing_dirs:
            self.logger.info(f"🗑️  기존 pending 데이터 정리: {len(existing_dirs)}개 발견")
            self.logger.debug("삭제할 디렉토리 목록:")
            self.logger.debug("\n".join(str(d) for d in existing_dirs))
            
            try:
                response = input("\n🗑️  위 pending 데이터를 삭제하시겠습니까? (y/N): ").strip().lower()
                if response not in ['y', 'yes']:
                    self.logger.info("❌ 사용자가 삭제를 취소했습니다.")
                    raise ValueError("사용자가 삭제를 취소했습니다.")
            except KeyboardInterrupt:
                self.logger.info("❌ 사용자가 삭제를 취소했습니다.")
                raise ValueError("사용자가 삭제를 취소했습니다.")
                
            for existing_dir in existing_dirs:
                try:
                    shutil.rmtree(existing_dir)
                    self.logger.info(f"🗑️ 삭제 완료: {existing_dir.name}")
                except Exception as e:
                    self.logger.error(f"❌ 삭제 실패: {existing_dir.name} - {e}")
            
            self.logger.info(f"✅ 기존 pending 데이터 정리 완료: {len(existing_dirs)}개 삭제")
        else:
            self.logger.debug("📭 정리할 기존 pending 데이터 없음")
    
    def _load_data(self, data_file: str,process_images: bool = False) -> Dataset:
        """데이터 파일을 로드하는 메서드"""
        data_path = Path(data_file).resolve()
        if not data_path.exists():
            raise FileNotFoundError(f"❌ 데이터 파일이 존재하지 않습니다: {data_path}")
        
        self.logger.info(f"📂 데이터 파일 로드 중: {data_path}")   
        
        if data_path.is_dir():
            try:
                dataset_obj = load_from_disk(str(data_path))
                self.logger.info(f"✅ datasets 폴더 로드 완료: {len(dataset_obj)} 행")
            except Exception as e:
                raise ValueError(f"❌ datasets 폴더 로드 실패: {e}")   
        elif data_path.suffix == '.parquet':
            try:
                df = pd.read_parquet(data_path)
                dataset_obj = Dataset.from_pandas(df)
                self.logger.info(f"✅ Parquet 파일 로드 완료: {len(df)} 행")
            except Exception as e:
                raise ValueError(f"❌ Parquet 파일 로드 실패: {e}")
        else:
            raise ValueError(f"❌ 지원하지 않는 파일 형식: {data_path.suffix}")
        
        self.logger.info(f"✅ 데이터 파일 로드 완료: {data_file}")
        
        column_names = dataset_obj.column_names
        self.logger.info(f"데이터셋 컬럼: {column_names}")
                
       # 통합된 컬럼 타입 변환 처리 (JSON dumps + 이미지)
        dataset_obj = self._process_cast_columns(dataset_obj)
        
        if process_images:
            dataset_obj, has_images = self._process_images(dataset_obj)
        else:
            has_images = False
            self.logger.debug("📄 이미지 컬럼 처리 생략")
        
        return dataset_obj, has_images

    def _process_cast_columns(self, dataset_obj: Dataset):
        
        self.logger.info("🔍 JSON 변환 대상 컬럼 검사 시작")
        
        json_cast_columns = []
        
        for key in dataset_obj.column_names:
            sample_value = dataset_obj[0][key]
            
            if isinstance(sample_value, (dict, list)):
                json_cast_columns.append(key)
                self.logger.info(f"📝 JSON 변환 대상 컬럼 발견: '{key}' (타입: {type(sample_value).__name__})")
        
        # JSON dumps 처리
        if json_cast_columns:
            dataset_obj = self._apply_json_transform(dataset_obj, json_cast_columns)
        else:
            self.logger.info("📄 JSON 변환 대상 컬럼 없음")
        
        return dataset_obj
    
    def _process_images(self, dataset_obj: Dataset):
        """이미지 컬럼 처리"""
        self.logger.info("🔍 이미지 컬럼 검사 시작")
        
        image_column = None
        has_images = False
        
        # 이미지 컬럼 찾기
        for key in dataset_obj.column_names:
            if key in self.image_column_candidates:
                image_column = key
                has_images = True
                self.logger.info(f"🖼️ 이미지 컬럼 발견: '{key}'")
                break
        
        # 이미지 컬럼 변환
        if has_images and image_column:
            try:
                dataset_obj = dataset_obj.cast_column(image_column, ImageFeature())
                self.logger.info(f"✅ 이미지 컬럼 '{image_column}'을 PIL Image로 변환 완료")
            except Exception as e:
                self.logger.error(f"❌ 이미지 컬럼 변환 실패: {e}")
                raise ValueError(f"❌ 이미지 컬럼 '{image_column}'을 PIL Image로 변환하는 데 실패했습니다.")
        else:
            self.logger.info("📄 이미지 컬럼 없음")
        
        return dataset_obj, has_images
    
    def _apply_json_transform(self, dataset_obj: Dataset, json_cast_columns: list) -> Dataset:
        """JSON 변환 적용"""
        self.logger.info(f"🔄 {len(json_cast_columns)}개 컬럼을 JSON으로 변환 중: {json_cast_columns}")
        
        def json_transform(x):
            if isinstance(x, (dict, list)):
                return json.dumps(x, ensure_ascii=False)
            return x
        
        try:
            dataset_obj = dataset_obj.map(
                lambda x: {col: json_transform(x[col]) for col in json_cast_columns},
                num_proc=self.num_proc,
                desc="JSON 변환 중",
            )
            self.logger.info(f"✅ JSON 변환 완료: {json_cast_columns}")
            return dataset_obj
        except Exception as e:
            self.logger.error(f"❌ JSON 변환 실패: {e}")
            raise ValueError(f"❌ JSON 변환 중 오류 발생: {e}")

    def _save_to_staging(self, dataset: Dataset, metadata: dict):
        """데이터를 staging 폴더에 저장"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_id = metadata['file_id']
        dataset_name = metadata['dataset']
        task = metadata['task']
        variant = metadata['variant']
        user = metadata['uploaded_by']
        
        staging_dirname = f"{dataset_name}_{task}_{variant}_{file_id}_{timestamp}_{user}"
        staging_dir= self.staging_path / "pending" / staging_dirname
        
        try:
            dataset.save_to_disk(str(staging_dir))
            metadata_file = staging_dir / "upload_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
                
            self.logger.info(f"📦 datasets 저장 완료: {staging_dir}")
            return str(staging_dir)
        except Exception as e:
            if staging_dir.exists():
                shutil.rmtree(staging_dir)
            raise ValueError(f"❌ datasets 저장 실패: {e}")
        
    def process_pending_data(self) -> Dict[str, int]:
        """pending 폴더의 모든 데이터를 catalog로 이동"""
        self.logger.info("🔄 Pending 데이터 처리 시작")
        
        pending_path = self.staging_path / "pending"
        processing_path = self.staging_path / "processing"
        failed_path = self.staging_path / "failed"
        
        # pending 폴더에서 처리 대상 찾기
        pending_dirs = [d for d in pending_path.iterdir() if d.is_dir()]
        
        if not pending_dirs:
            self.logger.info("📭 처리할 pending 데이터가 없습니다")
            return {"success": 0, "failed": 0}
        
        results = {"success": 0, "failed": 0}
        
        for pending_dir in pending_dirs:
            try:
                self.logger.info(f"📦 처리 중: {pending_dir.name}")
                
                # processing으로 이동
                processing_dir = processing_path / pending_dir.name
                shutil.move(str(pending_dir), str(processing_dir))
                
                # catalog로 이동 처리
                self._move_to_catalog(processing_dir)
                
                # 성공 시 processing 폴더 정리
                shutil.rmtree(processing_dir)
                
                results["success"] += 1
                self.logger.info(f"✅ 처리 완료: {pending_dir.name}")
                
            except Exception as e:
                self.logger.error(f"❌ 처리 실패: {pending_dir.name} - {e}")
                
                # 실패 시 failed로 이동
                if processing_dir.exists():
                    failed_dir = failed_path / pending_dir.name
                    failed_dir.parent.mkdir(exist_ok=True)
                    shutil.move(str(processing_dir), str(failed_dir))
                    
                    # 에러 로그 저장
                    error_log = failed_dir / "error.log"
                    with open(error_log, 'w', encoding='utf-8') as f:
                        f.write(f"Error: {str(e)}\n")
                        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                
                results["failed"] += 1
        
        self.logger.info(f"🎯 처리 결과: 성공={results['success']}, 실패={results['failed']}")
        return results
    
    def _setup_console_logging(self, log_level: str):
        """콘솔 로깅 설정"""
    
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
            'archive': self.archive_path
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
        log_file = log_dir / f"staging_manager_{date_str}_{user}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)
        self.logger.info(f"📝 파일 로깅 활성화: {log_file}")
        self.logger.info(f"🚀 StagingManager 초기화 완료")
        
    def process_pending_data(self) -> Dict[str, int]:
        """pending 폴더의 모든 데이터를 catalog로 이동"""
        self.logger.info("🔄 Pending 데이터 처리 시작")
        
        # pending 폴더에서 처리 대상 찾기
        pending_dirs = [d for d in self.staging_pending_path.iterdir() if d.is_dir()]
        
        if not pending_dirs:
            self.logger.info("📭 처리할 pending 데이터가 없습니다")
            return {"success": 0, "failed": 0}
        
        results = {"success": 0, "failed": 0}
        
        for pending_dir in pending_dirs:
            try:
                self.logger.info(f"📦 처리 중: {pending_dir.name}")
                
                # processing으로 이동
                processing_dir = self.staging_processing_path / pending_dir.name
                shutil.move(str(pending_dir), str(processing_dir))
                
                # catalog로 이동 처리
                self._move_to_catalog(processing_dir)
                
                # 성공 시 processing 폴더 정리
                shutil.rmtree(processing_dir)
                
                results["success"] += 1
                self.logger.info(f"✅ 처리 완료: {pending_dir.name}")
                
            except Exception as e:
                self.logger.error(f"❌ 처리 실패: {pending_dir.name} - {e}")
                
                # 실패 시 failed로 이동
                if processing_dir.exists():
                    failed_dir = self.staging_failed_path / pending_dir.name
                    failed_dir.parent.mkdir(exist_ok=True)
                    shutil.move(str(processing_dir), str(failed_dir))
                    
                    # 에러 로그 저장
                    error_log = failed_dir / "error.log"
                    with open(error_log, 'w', encoding='utf-8') as f:
                        f.write(f"Error: {str(e)}\n")
                        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                
                results["failed"] += 1
        
        self.logger.info(f"🎯 처리 결과: 성공={results['success']}, 실패={results['failed']}")
        return results

    def _move_to_catalog(self, staging_dir: Path):
        self.logger.info(f"📁 Catalog로 이동: {staging_dir.name}")
        
        # 1. metadata 읽기
        metadata_file = staging_dir / "upload_metadata.json"
        if not metadata_file.exists():
            raise FileNotFoundError(f"메타데이터 파일이 없습니다: {metadata_file}")
        
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        catalog_target_path = self._get_catalog_path(metadata)
        catalog_target_path.mkdir(parents=True, exist_ok=True)
                
        assets_target_path = None
        if metadata.get('data_type') == 'raw' and metadata.get('has_images', False):
            provider = metadata['provider']
            dataset = metadata['dataset']
            assets_target_path = (
                self.assets_path / 
                f"provider={provider}" / 
                f"dataset={dataset}"
            )
        try:
            if assets_target_path:
                self._extract_and_save_images(staging_dir, assets_target_path, metadata)
            self._convert_and_save_data(staging_dir, catalog_target_path, metadata)
        except Exception as e:
            self.logger.error(f"❌ Catalog로 이동 실패, 롤백 중: {e}")
            if catalog_target_path.exists():
                try:
                    shutil.rmtree(catalog_target_path)
                    self.logger.info(f"🔄 Catalog 롤백 완료: {catalog_target_path}")
                except Exception as rollback_e:
                    self.logger.error(f"❌ Catalog 롤백 실패: {rollback_e}")
            if assets_target_path and assets_target_path.exists(): 
                try:
                    shutil.rmtree(assets_target_path)
                    self.logger.info(f"🔄 Assets 롤백 완료: {assets_target_path}")
                except Exception as rollback_e:
                    self.logger.error(f"❌ Assets 롤백 실패: {rollback_e}")
            raise            
        
        self.logger.info(f"✅ Catalog 저장 완료: {catalog_target_path}")
        
    def _get_catalog_path(self, metadata: Dict) -> Path:
        """metadata 기반으로 catalog 경로 생성"""
        provider = metadata['provider']
        dataset = metadata['dataset']
        task = metadata['task']
        variant = metadata['variant']
        
        catalog_path = (
            self.catalog_path / 
            f"provider={provider}" / 
            f"dataset={dataset}" / 
            f"task={task}" / 
            f"variant={variant}"
        )    
        return catalog_path
    
    def _convert_and_save_data(self, staging_dir: Path, target_path: Path, metadata: Dict):
        """Arrow 데이터를 Parquet으로 변환하여 저장"""
        self.logger.info("🔄 Arrow → Parquet 변환 중")
        
        try:
            # Arrow 데이터 로드
            dataset_obj = load_from_disk(str(staging_dir))
            
            if metadata.get("data_type") == "task":
                dataset_obj = self._add_metadata_columns(dataset_obj, metadata)
            
            # Parquet으로 저장
            parquet_file = target_path / "data.parquet"
            dataset_obj.to_parquet(str(parquet_file))
            
            # 메타데이터 복사
            metadata_source = staging_dir / "upload_metadata.json"
            metadata_target = target_path / "_metadata.json"
            shutil.copy(str(metadata_source), str(metadata_target))
            
            self.logger.info(f"💾 Parquet 저장 완료: {parquet_file}")
            
        except Exception as e:
            raise ValueError(f"데이터 변환 실패: {e}")
        
    def _add_metadata_columns(self, dataset_obj: Dataset, metadata: Dict):
        """Task 데이터에 메타데이터 컬럼 추가"""
        self.logger.info("📝 Task 메타데이터 컬럼 추가 중")
        
        required_fields = self.schema_manager.get_required_fields(metadata['task'])
            
        # 데이터셋 길이
        num_rows = len(dataset_obj)
        
        # 필수 필드들만 컬럼으로 추가
        added_columns = []
        
        values = []
        for field in required_fields:
            value = metadata.get(field)
            if value is None:
                self.logger.warning(f"⚠️ 필수 필드 '{field}'가 메타데이터에 없습니다. 추가하지 않습니다.")
                raise ValueError(f"필수 필드 '{field}'를 추가해주세요.")
            values.append(value)
        for value in values:
            column_data = [metadata[field]] * num_rows
            dataset_obj = dataset_obj.add_column(field, column_data)
            added_columns.append(f"{field}={metadata[field]}")
            self.logger.debug(f"📝 컬럼 추가: {field} = {metadata[field]}")
        
        if added_columns:
            self.logger.info(f"✅ 필수 필드 컬럼 추가 완료: {', '.join(added_columns)}")
        else:
            self.logger.info("📝 추가할 필수 필드 컬럼 없음")
            
        return dataset_obj
        
    def _extract_and_save_images(self, staging_dir: Path, assets_target_path: Path, metadata: Dict):
        """Arrow 데이터에서 이미지 추출하여 assets 폴더에 저장"""
        self.logger.info("🖼️ 이미지 추출 및 저장 시작")
        assets_target_path.mkdir(parents=True, exist_ok=True)
        try:
            # Arrow 데이터 로드
            dataset_obj = load_from_disk(str(staging_dir))
            
            # 이미지 컬럼 찾기
            image_column = None
            for col in dataset_obj.column_names:
                if col in self.image_column_candidates:
                    image_column = col
                    break
            
            if not image_column:
                self.logger.warning("⚠️ 이미지 컬럼을 찾을 수 없습니다")
                raise ValueError("이미지 컬럼이 없습니다. 'image' 또는 'image_bytes' 컬럼이 필요합니다.")
            
            self.logger.info(f"📷 이미지 컬럼 '{image_column}'에서 이미지 추출 중")
            
            # 이미지 추출 및 저장
            saved_count = 0
            for idx, row in enumerate(dataset_obj):
                try:
                    image_data = row[image_column]
                    
                    # PIL Image 객체인지 확인
                    if hasattr(image_data, 'save'):  # PIL Image
                        pil_image = image_data
                    else:
                        # bytes 데이터라면 PIL Image로 변환
                        if isinstance(image_data, bytes):
                            pil_image = Image.open(io.BytesIO(image_data))
                        else:
                            self.logger.warning(f"⚠️ 지원하지 않는 이미지 타입: {type(image_data)}")
                            continue
                    
                    # 이미지를 bytes로 변환하여 해시 계산
                    img_bytes = self._pil_to_bytes(pil_image)
                    image_hash = hashlib.md5(img_bytes).hexdigest()
                    
                    # 저장 경로
                    image_filename = f"{image_hash}.jpg"
                    image_path = assets_target_path / image_filename
                    
                    # 이미 존재하지 않으면 저장
                    if not image_path.exists():
                        # RGB 모드로 변환 (JPEG 저장을 위해)
                        if pil_image.mode != 'RGB':
                            pil_image = pil_image.convert('RGB')
                        
                        pil_image.save(str(image_path), 'JPEG', quality=95)
                        saved_count += 1
                        
                        if saved_count % 100 == 0:  # 진행 상황 로그
                            self.logger.info(f"📷 이미지 저장 중... {saved_count}개 완료")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ 이미지 {idx} 처리 실패: {e}")
                    continue
            
            self.logger.info(f"✅ 이미지 추출 완료: {saved_count}개 저장, 경로: {assets_target_path}")
                
        except Exception as e:
            self.logger.error(f"❌ 이미지 추출 실패: {e}")
            raise ValueError(f"이미지 추출 중 오류 발생: {e}")

if __name__ == "__main__":
    manager = DataManager(log_level="DEBUG")
    
    manager.upload_raw_data(
        data_file="/home/kai/workspace/DeepDocs_Project/datalake/managers/table_test",
        provider="example_provider",
        dataset="example_dataset",
        dataset_description="This is a sample dataset for testing.",
        original_source="https://example.com/original_source"
    )
    
    manager.process_pending_data()