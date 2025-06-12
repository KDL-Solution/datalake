import logging
import os
import uuid
import json
import shutil
import pandas as pd
import requests 
import time 

from pathlib import Path
from datetime import datetime
from datasets import Dataset, load_from_disk
from datasets.features import Image as ImageFeature
from typing import Dict, Optional, List
from PIL import Image

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from managers.schema_manager import SchemaManager
from managers.logger import setup_logging

class LocalDataManager:
    def __init__(
        self, 
        base_path: str = "/mnt/AI_NAS/datalake/migrate_test",
        nas_api_url: str = "http://192.168.20.62:8000",
        log_level: str = "INFO",
        num_proc: int = 8, # 병렬 처리 프로세스 수
        auto_process: bool = True, # NAS 자동 처리 활성화 여부
        polling_interval: int = 10, # NAS 상태 조회 주기 (초)
        schema_manager: Optional[SchemaManager] = None,
    ):
        self.base_path = Path(base_path)
        self.nas_api_url = nas_api_url.rstrip('/')
        self.auto_process = auto_process
        self.polling_interval = polling_interval
        
        # 필수 디렉토리 설정
        self.staging_path = self.base_path / "staging"
        self.staging_pending_path = self.staging_path / "pending"
        self.staging_processing_path = self.staging_path / "processing"
        self.staging_failed_path = self.staging_path / "failed"
        self.catalog_path = self.base_path / "catalog"
        self.assets_path  = self.base_path / "assets"
        
        
        self.num_proc = num_proc
        self.image_data_candidates = ['image', 'image_bytes']
        self.image_data_key = 'image'  # 기본 이미지 컬럼 키
        self.file_path_candidates = ['image_path', 'file', 'file_path']
        self.file_path_key = 'file_path'  # 기본 파일 경로 컬럼 키
        
        self._check_path_and_setup_logging(log_level)
        
        self._check_nas_api_connection()

        self.schema_manager = schema_manager if schema_manager else SchemaManager(
            base_path=self.base_path, 
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
        
        self.logger.info(f"📥 Raw data 업로드 시작: {provider}/{dataset}")
        
        if not self.schema_manager.validate_provider(provider):
            raise ValueError(f"❌ 지원하지 않는 provider입니다: {provider}")
        
        self._cleanup_existing_pending(provider, dataset, task, is_raw=True)
        
        dataset_obj, file_info = self._load_data(data_file, process_assets=True)
        
        variant = file_info['variant']
        has_images = bool(file_info['image_columns'])
        has_files = bool(file_info['file_columns'])
        
        metadata = self._create_metadata(
            provider=provider,
            dataset=dataset,
            task=task,
            variant=variant,
            total_rows=len(dataset_obj),
            data_type="raw",
            source_task=None,  # 원본 작업이므로 None
            has_images=has_images,
            has_files=has_files,
            dataset_description=dataset_description,
            original_source=original_source,
        )
        
        staging_dir = self._save_to_staging(dataset_obj, metadata, file_info)
        self.logger.info(f"✅ Task 데이터 업로드 완료: {staging_dir}")
        
        job_id = None
        if self.auto_process:
            job_id = self.trigger_nas_processing()
            if job_id:
                self.logger.info(f"🔄 자동 처리 시작됨: {job_id}")
        
        return staging_dir, job_id

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
        dataset_obj, _ = self._load_data(data_file, process_assets=False)
        
        # 메타데이터 생성
        metadata = self._create_metadata(
            provider=provider,
            dataset=dataset,
            task=task,
            variant=variant,
            dataset_description=dataset_description,
            source_task=source_task,
            has_images=False,  # 이미지는 참조만
            has_files=False,  # 파일 경로는 없음
            total_rows=len(dataset_obj),
            data_type='task',
            **kwargs
        )
        
        # Staging에 저장
        staging_dir = self._save_to_staging(dataset_obj, metadata)
        self.logger.info(f"✅ Task 데이터 업로드 완료: {staging_dir}")
        
        job_id = None
        if self.auto_process:
            job_id = self.trigger_nas_processing()
            if job_id:
                self.logger.info(f"🔄 자동 처리 시작됨: {job_id}")
        
        return staging_dir, job_id
    
    def get_nas_status(self) -> Optional[Dict]:
        """NAS 서버 상태 조회"""
        try:
            response = requests.get(f"{self.nas_api_url}/status", timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"❌ 상태 조회 실패: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ NAS API 연결 실패: {e}")
            return None
        
    def list_nas_jobs(self) -> Optional[List[Dict]]:
        """모든 작업 목록 조회"""
        try:
            response = requests.get(f"{self.nas_api_url}/jobs", timeout=10)
            if response.status_code == 200:
                return response.json().get('jobs', [])
            else:
                self.logger.error(f"❌ 작업 목록 조회 실패: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ NAS API 연결 실패: {e}")
            return None
        
    def show_nas_dashboard(self):
        """NAS 상태 대시보드 출력"""
        print("\n" + "="*60)
        print("📊 NAS Data Processing Dashboard")
        print("="*60)
        
        # 상태 조회
        status = self.get_nas_status()
        if status:
            print(f"📦 Pending: {status['pending']}개")
            print(f"🔄 Processing: {status['processing']}개")
            print(f"❌ Failed: {status['failed']}개")
            print(f"🖥️ Server Status: {status['server_status']}")
            print(f"⏰ Last Updated: {status['last_updated']}")
        else:
            print("❌ NAS 서버 상태 조회 실패")
        
        # 작업 목록
        jobs = self.list_nas_jobs()
        if jobs:
            print(f"\n📋 Recent Jobs ({len(jobs)}개):")
            for job in jobs[-5:]:  # 최근 5개만
                status_emoji = {"running": "🔄", "completed": "✅", "failed": "❌"}.get(job['status'], "❓")
                print(f"  {status_emoji} {job['job_id']} - {job['status']} ({job['started_at']})")
        
        print("="*60 + "\n")
        
    def trigger_nas_processing(self) -> Optional[str]:
        """NAS에서 처리 시작"""
        self.logger.info("🔄 NAS 처리 요청 중...")
        start_time = time.time()
        try:
            response = requests.post(
                f"{self.nas_api_url}/process", 
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )
            elapsed = time.time() - start_time
            self.logger.debug(f"⏱️ NAS 처리 요청 시간: {elapsed:.2f}초")
            if response.status_code == 200:
                result = response.json()
                job_id = result.get('job_id')
                status = result.get('status')
                message = result.get('message', '')
                
                if status == 'already_running':
                    self.logger.info("🔄 이미 처리 중인 작업이 있습니다")
                    return job_id
                elif status == 'started':
                    self.logger.info(f"✅ 처리 작업 시작됨: {job_id}")
                    return job_id
                else:
                    self.logger.warning(f"⚠️ 알 수 없는 상태: {status}, 메시지: {message}")
                    return job_id
            else:                    
                self.logger.error(f"❌ 처리 시작 실패: {response.status_code}")
                try:
                    error_detail = response.json().get('detail', response.text)
                    self.logger.error(f"오류 상세: {error_detail}")
                except:
                    self.logger.error(f"응답 내용: {response.text}")
                return None
            
        except requests.exceptions.Timeout:
            elapsed = time.time() - start_time
            self.logger.error(f"❌ API 요청 타임아웃 ({elapsed:.2f}초)")
            return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"❌ NAS 서버 연결 실패: {self.nas_api_url}")
            return None
        except requests.exceptions.RequestException as e:
            elapsed = time.time() - start_time
            self.logger.error(f"❌ API 요청 실패 ({elapsed:.2f}초): {e}")
            return None
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """작업 상태 조회"""
        try:
            response = requests.get(f"{self.nas_api_url}/jobs/{job_id}", timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                self.logger.warning(f"⚠️ 작업을 찾을 수 없음: {job_id}")
                return None
            else:
                self.logger.error(f"❌ 작업 상태 조회 실패: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ NAS API 연결 실패: {e}")
            return None
        
    def wait_for_job_completion(self, job_id: str, timeout: int = 3600) -> Dict:
        """작업 완료까지 대기 (폴링)"""
        self.logger.info(f"⏳ 작업 완료 대기 중: {job_id}")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            job_status = self.get_job_status(job_id)
            if not job_status:
                raise RuntimeError(f"작업 상태 조회 실패: {job_id}")
            
            status = job_status.get('status')
            
            if status == 'completed':
                result = job_status.get('result', {})
                self.logger.info(f"✅ 작업 완료: {job_id}")
                self.logger.info(f"📊 처리 결과: 성공={result.get('success', 0)}, 실패={result.get('failed', 0)}")
                return job_status
                
            elif status == 'failed':
                error = job_status.get('error', 'Unknown error')
                self.logger.error(f"❌ 작업 실패: {job_id}, 오류: {error}")
                raise RuntimeError(f"작업 실패: {error}")
                
            elif status == 'running':
                self.logger.debug(f"🔄 작업 진행 중: {job_id}")
                time.sleep(self.polling_interval)
            else:
                self.logger.warning(f"⚠️ 알 수 없는 작업 상태: {status}")
                time.sleep(self.pooling_interval)
        
        raise TimeoutError(f"작업 완료 대기 시간 초과: {job_id}")    
        
    def _check_nas_api_connection(self):
        """NAS API 서버 연결 확인"""
        try:
            response = requests.get(f"{self.nas_api_url}/health", timeout=5)
            if response.status_code == 200:
                self.logger.info(f"✅ NAS API 서버 연결 확인: {self.nas_api_url}")
            else:
                self.logger.warning(f"⚠️ NAS API 서버 응답 이상: {response.status_code}")
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"⚠️ NAS API 서버 연결 실패: {e}")
            self.logger.warning("🔄 로컬 모드로 동작합니다 (자동 처리 비활성화)")
            self.auto_process = False
    
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
        has_files: bool = False,
        dataset_description: str = "",
        original_source: str = "",
        **kwargs
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
            'has_files': has_files,
            'total_rows': total_rows,
            'uploaded_by': os.getenv('USER', 'unknown'),
            'uploaded_at': datetime.now().isoformat(),
            'file_id': str(uuid.uuid4())[:8],
        }
        metadata.update(kwargs) # task의 추가 필드
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
    
    def _load_data(self, data_file: str, process_assets: bool = False) -> Dataset:
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
        
        if process_assets:
            # 파일 컬럼 분석 및 variant 결정
            file_info = self._detect_file_columns_and_variant(dataset_obj)
            dataset_obj = self._normalize_column_names(dataset_obj, file_info)

            self.logger.info(f"📄 파일 분석 결과: variant={file_info['variant']}, "
                           f"이미지컬럼={file_info['image_columns']}, "
                           f"파일컬럼={file_info['file_columns']}, "
                           f"확장자={file_info['extensions']}")
        else:
            file_info = {'image_columns': [], 'file_columns': [], 'variant': 'text', 'extensions': set()}
            self.logger.debug("📄 Assets 컬럼 처리 생략")
        
        return dataset_obj, file_info
    
    def _detect_file_columns_and_variant(self, dataset_obj: Dataset) -> Dict:
        """파일 컬럼들을 찾고 확장자 기반으로 variant 결정"""
        result = {
            'image_columns': [],
            'file_columns': [],
            'extensions': set(),
            'variant': 'text'
        }
        for key in dataset_obj.column_names:
            sample_value = dataset_obj[0][key]
            
            # PIL Image나 bytes 데이터인 경우
            if key in self.image_data_candidates:
                if hasattr(sample_value, 'save') or isinstance(sample_value, bytes):
                    result['image_columns'].append(key)
                    continue
            
            # 경로 기반 파일인 경우
            if key in self.file_path_candidates:
                if isinstance(sample_value, str) and Path(sample_value).exists():
                    ext = Path(sample_value).suffix.lower()
                    result['extensions'].add(ext)
                    result['file_columns'].append(key)
        
        if len(result['image_columns']) > 1:
            raise ValueError(f"❌ 이미지 컬럼이 2개 이상입니다: {result['image_columns']}. "
                             f"하나의 컬럼만 사용해주세요.")
        if len(result['file_columns']) > 1:
            raise ValueError(f"❌ 파일 컬럼이 2개 이상입니다: {result['file_columns']}. "
                             f"하나의 컬럼만 사용해주세요.")
            
        result['image_columns'] = result['image_columns'][:1]
        result['file_columns'] = result['file_columns'][:1]
        # variant 결정
        has_image_data = bool(result['image_columns'])
        has_file_paths = bool(result['file_columns'])
        extensions = result['extensions']
        
        if has_image_data and has_file_paths:
            result['variant'] = "mixed"
        elif has_image_data:
            result['variant'] = "image"
        elif has_file_paths:
            # 확장자 기반으로 구체적 분류
            if any(ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'] for ext in extensions):
                result['variant'] = "image"
            else:
                result['variant'] = "files"
        else:
            result['variant'] = "text"
        
        return result

    def _normalize_column_names(self, dataset_obj: Dataset, file_info: Dict) -> Dataset:
        """컬럼명을 표준화 (image_columns → image, file_columns → file_path)"""
        
        # 이미지 컬럼 표준화
        if len(file_info['image_columns']):
            image_col = file_info['image_columns'][0]
            self.logger.info(f"🔄 이미지 컬럼 표준화: {image_col} → {self.image_data_key}")
            
            # 첫 번째 이미지 컬럼을 표준 컬럼으로 사용
            
            if image_col != self.image_data_key:
                # 컬럼명 변경
                dataset_obj = dataset_obj.rename_column(image_col, self.image_data_key)
        
        # 파일 컬럼 표준화
        if len(file_info['file_columns']):
            file_col = file_info['file_columns'][0]
            self.logger.info(f"🔄 파일 컬럼 표준화: {file_col} → {self.file_path_key}")
            
            
            # 컬럼명 변경 (필요한 경우)
            if file_col != self.file_path_key:
                dataset_obj = dataset_obj.rename_column(file_col, self.file_path_key)
        return dataset_obj
    
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

    def _save_to_staging(self, dataset_obj: Dataset, metadata: dict, file_info: Optional[Dict] = None) -> str:
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
            if metadata.get('data_type') == 'raw' and file_info:
                # 메타데이터 업데이트
                staging_assets_dir = staging_dir / "assets"
            
                if len(file_info['file_columns']):
                    dataset_obj  = self._copy_file_path_to_staging(
                        dataset_obj, staging_assets_dir
                    )
                
            if metadata.get('data_type') == 'task':
                dataset_obj = self._add_metadata_columns(dataset_obj, metadata)
                
            dataset_obj.save_to_disk(str(staging_dir))
            
            metadata_file = staging_dir / "upload_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
                
            self.logger.info(f"📦 datasets 저장 완료: {staging_dir}")
            return str(staging_dir)
        except Exception as e:
            if staging_dir.exists():
                shutil.rmtree(staging_dir)
            raise 
    
    def _copy_file_path_to_staging(self, dataset_obj: Dataset, staging_assets_dir: Path):
        """파일 경로를 staging으로 복사"""
        sample_value = dataset_obj[0][self.file_path_key]
        
        if isinstance(sample_value, str) and Path(sample_value).exists():
            def copy_file(example, idx):
                original_path = Path(example[self.file_path_key]).resolve()
                if original_path.exists():
                    ext = original_path.suffix or ""
                    prefix = "file"
                    new_filename = f"{prefix}_{idx:06d}{ext}"
                    target_path = staging_assets_dir / new_filename
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    shutil.copy2(original_path, target_path)
                    
                    example[self.file_path_key] = f"assets/{new_filename}"
                    
                return example
            
            dataset_obj = dataset_obj.map(copy_file, with_indices=True)
            return dataset_obj
        else:
            self.logger.warning(f"⚠️ 파일 경로 컬럼 '{self.file_path_key}'가 유효하지 않거나 존재하지 않습니다: {sample_value}")
            raise ValueError(f"파일 경로 컬럼 '{self.file_path_key}'가 유효하지 않거나 존재하지 않습니다.")
    
    def _check_path_and_setup_logging(self, log_level: str):
        
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
        setup_logging(log_level=log_level, base_path=str(self.base_path))
        self.logger = logging.getLogger(__name__)
        self.logger.debug("✅ 모든 필수 디렉토리 확인 완료")
        
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
    
            
if __name__ == "__main__":
    manager = LocalDataManager(
        log_level="DEBUG",
        )
    
    manager.show_nas_dashboard()
    manager.show_schema_info()
    
    manager.add_provider("example_provider")
    manager.remove_provider("example_provider")
    manager.add_provider("example_provider")
    manager.add_task("example_task", required_fields=["field1", "field2"], allowed_values={"field1": ["value1", "value2"]})
    staging_dir, job_id = manager.upload_raw_data(
        data_file="/home/kai/workspace/DeepDocs_Project/datalake/managers/sample_data_1",
        provider="example_provider",
        dataset="example_dataset",
        dataset_description="This is a sample dataset for testing.",
        original_source="https://example.com/original_source"
    )
    
    if job_id:
        try:
            job_status = manager.wait_for_job_completion(job_id, timeout=600)
            print(f"작업 완료: {job_status}")
        except Exception as e:
            print(f"작업 대기 중 오류 발생: {e}")
    
    
    
    
