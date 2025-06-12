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

class LocalDataManager:
    def __init__(
        self, 
        base_path: str = "/mnt/AI_NAS/datalake/migrate_test",
        nas_api_url: str = "http://192.168.20.62:8000",
        log_level: str = "INFO",
        num_proc: int = 8, # 병렬 처리 프로세스 수
        auto_process: bool = True, # NAS 자동 처리 활성화 여부
        polling_interval: int = 10, # NAS 상태 조회 주기 (초)
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
        self.schema_path = self.base_path / "config" / "schema.yaml"
        
        self.num_proc = num_proc
        self.image_column_candidates = ['image', 'image_bytes']
        
        
        self._setup_console_logging(log_level)
        self._check_path_and_setup_logging()
        
        self._check_nas_api_connection()

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
 
    def add_provider(self, provider: str) -> bool:
        """새로운 Provider 추가"""
        if self.schema_manager.add_provider(provider):
            self.logger.info(f"✅ Provider '{provider}' 추가 완료")
            return True
        else:
            self.logger.warning(f"⚠️ Provider '{provider}'는 이미 존재합니다")
            return False
    
    def add_task(
        self, 
        task: str, 
        required_fields: Optional[List[str]] = None, 
        allowed_values: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """새로운 Task 추가"""
        if self.schema_manager.add_task(task, required_fields, allowed_values):
            self.logger.info(f"✅ Task '{task}' 추가 완료")
            if required_fields:
                    self.logger.info(f"  📝 필수 필드: {required_fields}")
            if allowed_values:
                self.logger.info(f"  🔧 허용 값: {allowed_values}")
            return True
        else:
            self.logger.warning(f"⚠️ Task '{task}'는 이미 존재합니다")
            return False
        
    def update_task(
        self, 
        task: str, 
        required_fields: Optional[List[str]] = None, 
        allowed_values: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """기존 Task 업데이트"""
        if self.schema_manager.update_task(task, required_fields, allowed_values):
            self.logger.info(f"✅ Task '{task}' 업데이트 완료")
            if required_fields:
                self.logger.info(f"  📝 필수 필드: {required_fields}")
            if allowed_values:
                self.logger.info(f"  🔧 허용 값: {allowed_values}")
            return True
        else:
            self.logger.warning(f"⚠️ Task '{task}'는 존재하지 않습니다")
            return False
    
    def remove_provider(self, provider: str) -> bool:
        """Provider 제거"""
        if self.schema_manager.remove_provider(provider):
            self.logger.info(f"✅ Provider '{provider}' 제거 완료")
            return True
        else:
            self.logger.warning(f"⚠️ Provider '{provider}'는 존재하지 않습니다")
            return False
    
    def remove_task(self, task: str) -> bool:
        """Task 제거"""
        if self.schema_manager.remove_task(task):
            self.logger.info(f"✅ Task '{task}' 제거 완료")
            return True
        else:
            self.logger.warning(f"⚠️ Task '{task}'는 존재하지 않습니다")
            return False
        
    def list_providers(self) -> List[str]:
        """모든 Provider 목록 조회"""
        return self.schema_manager.get_all_providers()
    
    def list_tasks(self) -> Dict[str, Dict]:
        """모든 Task 목록 조회"""
        return self.schema_manager.get_all_tasks()
    
    def show_schema_info(self):
        """스키마 정보 대시보드 출력"""
        print("\n" + "="*60)
        print("📋 Schema Configuration Dashboard")
        print("="*60)
        
        # Providers
        providers = self.list_providers()
        print(f"\n🏢 Providers ({len(providers)}개):")
        for provider in providers:
            print(f"  • {provider}")
        
        # Tasks
        tasks = self.list_tasks()
        print(f"\n📝 Tasks ({len(tasks)}개):")
        for task_name, task_config in tasks.items():
            print(f"  • {task_name}")
            
            required_fields = task_config.get('required_fields', [])
            if required_fields:
                print(f"    📝 필수 필드: {', '.join(required_fields)}")
            
            allowed_values = task_config.get('allowed_values', {})
            if allowed_values:
                print(f"    🔧 허용 값:")
                for field, values in allowed_values.items():
                    print(f"      - {field}: {', '.join(values)}")
        
        print("="*60 + "\n")
        
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
        log_file = log_dir / f"DataManager_{date_str}_{user}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)
        self.logger.info(f"📝 파일 로깅 활성화: {log_file}")
        self.logger.info(f"🚀 DataManager 초기화 완료")
    
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
    
    
    
    
