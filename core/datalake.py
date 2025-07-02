import logging
import os
import uuid
import json
import shutil
import pandas as pd
import requests 
import time 
import subprocess
import psutil
from pathlib import Path
from datetime import datetime
from datasets import Dataset, load_from_disk
from datasets import Image as DatasetImage
from typing import Dict, Optional, List, Union
from PIL import Image

from core.schema import SchemaManager
from core.collections import CollectionManager
from utils.logging import setup_logging
from clients.duckdb_client import DuckDBClient


class DatalakeClient:
    def __init__(
        self, 
        user_id: str = None, # 사용자 ID (필수)
        base_path: str = "/mnt/AI_NAS/datalake/",
        server_url: str = "http://192.168.20.62:8091",
        log_level: str = "INFO",
        num_proc: int = 8, # 병렬 처리 프로세스 수
        table_name: str = "catalog",
        create_dirs: bool = False, # 초기 디렉토리 생성 여부
    ):
        if not user_id:
            raise ValueError("user_id는 필수 입니다. 예: DatalakeClient(user_id='user_123')")
        
        self.user_id = user_id
        self.server_url = server_url.rstrip('/')
        
        self.base_path = Path(base_path)
        self.staging_path = self.base_path / "staging"
        self.staging_pending_path = self.staging_path / "pending"
        self.staging_processing_path = self.staging_path / "processing"
        self.staging_failed_path = self.staging_path / "failed"
        self.catalog_path = self.base_path / "catalog"
        self.assets_path  = self.base_path / "assets"
        self.collections_path = self.base_path / "collections"
        self.config_path = self.base_path / "config" / "schema.yaml"
        self.duckdb_path = self.base_path / "users" / f"{self.user_id}.duckdb"
        
        self.num_proc = num_proc
        self.table_name = table_name
        self.image_data_candidates = ['image', 'image_bytes']
        self.image_data_key = 'image'  # 기본 이미지 컬럼 키
        self.file_path_candidates = ['image_path', 'file', 'file_path']
        self.file_path_key = 'file_path'  # 기본 파일 경로 컬럼 키
        
    
        
        self._initialize(log_level, create_dirs=create_dirs)
        self._check_server_connection()
               
        self.schema_manager = SchemaManager(
            config_path=self.config_path,
            create_default=True
        )
        self.collection_manager = CollectionManager(
            collections_path=self.collections_path,
        )
        
    def _initialize(self, log_level: str, create_dirs: bool = False):
        required_paths = {
            'base': self.base_path,
            'staging': self.staging_path,
            'staging/pending': self.staging_pending_path,
            'staging/processing': self.staging_processing_path, 
            'staging/failed': self.staging_failed_path,
            'catalog': self.catalog_path,
            'assets': self.assets_path,
            'collections': self.collections_path,
        }
        if create_dirs:
            for path_name, path_obj in required_paths.items():
                if not path_obj.exists():
                    path_obj.mkdir(mode=0o777, parents=True, exist_ok=True)
                    self.logger.debug(f"📁 디렉토리 생성: {path_name}")
        else:
            missing_paths = []
            for path_name, path_obj in required_paths.items():
                if not path_obj.exists():
                    missing_paths.append(f"  - {path_name}: {path_obj}")
            
            if missing_paths:
                missing_list = '\n'.join(missing_paths)
                raise FileNotFoundError(f"❌ 필수 디렉토리가 없습니다:\n{missing_list}")
            
        setup_logging(
            user_id=self.user_id,
            log_level=log_level, 
            base_path=str(self.base_path)
        )
        self.logger = logging.getLogger(__name__)
        self.logger.debug("✅ 모든 필수 디렉토리 확인 완료")

    def _check_server_connection(self):
        """서버 연결 확인"""
        try:
            response = requests.get(f"{self.server_url}/health", timeout=5)
            if response.status_code == 200:
                self.logger.debug(f"✅ 서버 연결 성공: {self.server_url}")
            else:
                self.logger.warning(f"⚠️ 서버 응답 이상: {response.status_code}")
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"⚠️ 서버 연결 실패: {e}")
            raise ConnectionError(f"서버 연결 실패: {e}")
        
    def upload_raw(
        self,
        data_file: Union[str, Path, pd.DataFrame, Dataset],
        provider: str,
        dataset: str,
        dataset_description: str = "", # 데이터셋 설명
        original_source: str = "", # 원본 소스 URL 
        overwrite: bool = False, # 기존 pending 데이터 제거 여부
    ) -> bool:
        task = "raw"

        self.logger.info(f"📥 Raw data 업로드 시작: {provider}/{dataset}")

        if not self.schema_manager.validate_provider(provider):
            raise ValueError(f"❌ 지원하지 않는 provider입니다: {provider}")

        if not self._handle_existing_data(provider, dataset, task, overwrite=overwrite, is_raw=True):
            self.logger.warning(f"⚠️ 기존 데이터가 존재합니다: {provider}/{dataset}/{task}")
            self.logger.info("💡 overwrite=True로 설정하면 기존 데이터를 덮어쓸 수 있습니다")
            return False
        
        dataset_obj, file_info = self._load_data(data_file)

        metadata = self._create_metadata(
            provider=provider,
            dataset=dataset,
            task=task,
            variant=file_info['type'],
            total_rows=len(dataset_obj),
            data_type="raw",
            has_images=file_info['has_image_data'],
            has_files= file_info['has_file_paths'],
            dataset_description=dataset_description,
            original_source=original_source,
        )
        
        staging_dir = self._save_to_staging(dataset_obj, metadata)
        self.logger.info(f"✅ Task 데이터 업로드 완료: {staging_dir}")
        
        return True
    
    def upload_task(
        self,
        data_file: Union[str, Path, pd.DataFrame, Dataset],
        provider: str,
        dataset: str,
        task: str,
        variant: str,
        dataset_description: str = "",
        overwrite: bool = False,
        meta: Optional[Dict] = None,
    ) -> bool:
        self.logger.info(f"📥 Task data 업로드 시작: {provider}/{dataset}/{task}/{variant}")
        
        if not self._check_raw_data_exists(provider, dataset):
            self.logger.warning(f"⚠️ Raw 데이터가 없습니다: {provider}/{dataset}")
            self.logger.info("💡 upload_raw()로 원본 데이터를 먼저 업로드하는 것을 권장합니다.")

        if not self.schema_manager.validate_provider(provider):
            raise ValueError(f"❌ 지원하지 않는 provider입니다: {provider}")
        
        is_valid, error_msg = self.schema_manager.validate_task_metadata(task, meta)
        if not is_valid:
            raise ValueError(f"❌ Task 메타데이터 검증 실패: {error_msg}")

        if not self._handle_existing_data(provider, dataset, task, variant, overwrite=overwrite, is_raw=False):
            self.logger.warning(f"⚠️ 기존 데이터가 존재합니다: {provider}/{dataset}/{task}/{variant}")
            self.logger.info("💡 overwrite=True로 설정하면 기존 데이터를 덮어쓸 수 있습니다")
            return False
        
        dataset_obj, file_info = self._load_data(data_file)

        columns_to_remove = [key for key in meta.keys()
                            if key in dataset_obj.column_names]
        
        if columns_to_remove:
            dataset_obj = dataset_obj.remove_columns(columns_to_remove)
            self.logger.info(f"🗑️ 기존 메타데이터 컬럼 제거: {columns_to_remove}")
        
        # 메타데이터 생성
        metadata = self._create_metadata(
            provider=provider,
            dataset=dataset,
            task=task,
            variant=variant,
            dataset_description=dataset_description,
            has_images=file_info['has_image_data'],
            has_files=file_info['has_file_paths'],
            total_rows=len(dataset_obj),
            data_type='task',
            meta=meta,
        )
        
        # Staging에 저장
        staging_dir = self._save_to_staging(dataset_obj, metadata)
        self.logger.info(f"✅ Task 데이터 업로드 완료: {staging_dir}")
        
        return True
    
    def get_server_status(self) -> Optional[Dict]:
        """서버 상태 조회"""
        try:
            response = requests.get(f"{self.server_url}/status", timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"❌ 상태 조회 실패: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 서버 연결 실패: {e}")
            return None
        
    def list_server_jobs(self) -> Optional[List[Dict]]:
        """모든 작업 목록 조회"""
        try:
            response = requests.get(f"{self.server_url}/jobs", timeout=10)
            if response.status_code == 200:
                return response.json().get('jobs', [])
            else:
                self.logger.error(f"❌ 작업 목록 조회 실패: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 서버 연결 실패: {e}")
            return None
        
    def show_server_dashboard(self):
        """서버 상태 대시보드 출력"""
        print("\n" + "="*60)
        print("📊 Data Processing Server Dashboard")
        print("="*60)

        # 상태 조회
        status = self.get_server_status()
        if status:
            print(f"📦 Pending: {status['pending']}개")
            print(f"🔄 Processing: {status['processing']}개")
            print(f"❌ Failed: {status['failed']}개")
            print(f"🖥️ Server Status: {status['server_status']}")
            print(f"⏰ Last Updated: {status['last_updated']}")
        else:
            print("❌ 서버 상태 조회 실패")
        
        # 작업 목록
        jobs = self.list_server_jobs()
        if jobs:
            print(f"\n📋 Recent Jobs ({len(jobs)}개):")
            for job in jobs[-5:]:  # 최근 5개만
                status_emoji = {"running": "🔄", "completed": "✅", "failed": "❌"}.get(job['status'], "❓")
                print(f"  {status_emoji} {job['job_id']} - {job['status']} ({job['started_at']})")

        print("="*60 + "\n")
        
    def trigger_processing(self) -> Optional[str]:
        """서버 처리 요청"""
        self.logger.info("🔄 서버 처리 요청 중...")
        start_time = time.time()
        try:
            response = requests.post(
                f"{self.server_url}/process", 
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )
            elapsed = time.time() - start_time
            self.logger.debug(f"⏱️ 서버 처리 요청 시간: {elapsed:.2f}초")
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
            self.logger.error(f"❌ 서버 연결 실패: {self.server_url}")
            return None
        except requests.exceptions.RequestException as e:
            elapsed = time.time() - start_time
            self.logger.error(f"❌ 요청 실패: {e} ({elapsed:.2f}초)")
            return None
    
    def get_job_status(self, job_id: str) -> Optional[dict]:
        """작업 상태 조회"""
        try:
            response = requests.get(f"{self.server_url}/jobs/{job_id}", timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                self.logger.warning(f"⚠️ 작업을 찾을 수 없음: {job_id}")
                return None
            else:
                self.logger.error(f"❌ 작업 상태 조회 실패: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 서버 연결 실패: {e}")
            return None
    
    def wait_for_job_completion(self, job_id: str, polling_interval: int = 10, timeout: int = 3600) -> dict:
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
                time.sleep(polling_interval)
            else:
                self.logger.warning(f"⚠️ 알 수 없는 작업 상태: {status}")
                time.sleep(polling_interval)
        
        raise TimeoutError(f"작업 완료 대기 시간 초과: {job_id}")    

    def get_db_info(self) -> Dict:
        """DB 정보 조회"""
        self.logger.info("📊 DB 정보 조회 중...")
        
        try:
            if not self.duckdb_path.exists():
                return {
                    'exists': False,
                    'message': 'DB 파일이 없습니다. build_db()로 생성하세요.'
                }
            
            # DB 기본 정보
            db_size = self.duckdb_path.stat().st_size / 1024 / 1024
            db_mtime = datetime.fromtimestamp(self.duckdb_path.stat().st_mtime)
            
            info = {
                'user_id': self.user_id,
                'exists': True,
                'path': str(self.duckdb_path),
                'size_mb': round(db_size, 1),
                'modified_time': db_mtime.strftime('%Y-%m-%d %H:%M:%S'),
                'is_outdated': self._is_db_outdated(),
            }

            with DuckDBClient(str(self.duckdb_path), read_only=True) as duck_client:
                tables = duck_client.list_tables()
                info['tables'] = tables['name'].tolist()
                
                if self.table_name in info['tables']:
                    count_result = duck_client.execute_query(f"SELECT COUNT(*) as total FROM {self.table_name}")
                    total_rows = count_result['total'].iloc[0]
                    info['total_rows'] = total_rows
                    info['partitions'] = 0
                    info['provider_stats'] = {}
                    info['dataset_stats'] = {}
                    info['task_stats'] = {}
                    info['variant_stats'] = {}
                    
                    # 파티션 정보
                    try:
                        partitions_df = duck_client.retrieve_partitions(self.table_name)
                        info['partitions'] = len(partitions_df)

                        # Provider별 통계
                        if not partitions_df.empty:
                            provider_stats = partitions_df.groupby('provider').size().to_dict()
                            dataset_stats = partitions_df.groupby('dataset').size().to_dict()
                            task_stats = partitions_df.groupby('task').size().to_dict()
                            variant_stats = partitions_df.groupby('variant').size().to_dict()
                            info['provider_stats'] = provider_stats
                            info['dataset_stats'] = dataset_stats
                            info['task_stats'] = task_stats
                            info['variant_stats'] = variant_stats
                    except Exception as e:
                        self.logger.warning(f"파티션 정보 조회 실패: {e}")
                
            return info

        except Exception as e:
            self.logger.error(f"❌DB 정보 조회 실패: {e}")
            return {
                'exists': False,
                'error': str(e)
            }
    
    def build_db(self, force_rebuild: bool = False) -> bool:
        """DB 구축 또는 재구축"""
        self.logger.info("🔨 DB 구축 시작...")
        
        try:
            if not self.catalog_path.exists():
                raise FileNotFoundError(f"Catalog 디렉토리가 존재하지 않습니다: {self.catalog_path}")

            # 기존 DB 파일 처리
            if self.duckdb_path.exists():
                if force_rebuild:
                    self.logger.info("🗑️ 기존 DB 파일 삭제 중...")
                    self._cleanup_db_files()
                else:
                    self.logger.info("⚠️ 기존 DB 파일이 존재합니다. force_rebuild=True로 재구축하세요.")
                    return False

            # 디렉토리 생성
            self.duckdb_path.parent.mkdir(mode=0o777, parents=True, exist_ok=True)

            # Parquet 파일들 확인
            parquet_files = list(self.catalog_path.rglob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError("Parquet 파일을 찾을 수 없습니다.")

            self.logger.info(f"📂 발견된 Parquet 파일: {len(parquet_files)}개")

            # 새 DB 생성
            with DuckDBClient(str(self.duckdb_path), read_only=False) as duck_client:
                parquet_pattern = str(self.catalog_path / "**" / "*.parquet")
                
                self.logger.info("📊 테이블 생성 중...")
                duck_client.create_table_from_parquet(
                    self.table_name,
                    parquet_pattern,
                    hive_partitioning=True,
                    union_by_name=True
                )

                # 결과 검증
                count_result = duck_client.execute_query(f"SELECT COUNT(*) as total FROM {self.table_name}")
                total_rows = count_result['total'].iloc[0]
                
                self.logger.info(f"✅ DB 구축 완료!")
                self.logger.info(f"📊 총 {total_rows:,}개 행")
                self.logger.info(f"💾 DB 파일: {self.duckdb_path}")
                self.logger.info(f"📁 파일 크기: {self.duckdb_path.stat().st_size / 1024 / 1024:.1f}MB")

            # 권한 설정
            self.duckdb_path.chmod(0o777)
            return True

        except Exception as e:
            self.logger.error(f"❌ DB 구축 실패: {e}")
            # 실패 시 정리
            if self.duckdb_path.exists():
                try:
                    self.duckdb_path.unlink()
                except:
                    pass
            return False
    
    def get_partitions(self) -> pd.DataFrame:
        """사용 가능한 파티션 목록 조회"""
        self.logger.debug("🔍 파티션 조회 중...")
        
        try:
            if not self.duckdb_path.exists():
                raise FileNotFoundError("DB가 없습니다. build_db()로 먼저 생성하세요.")
                
            with DuckDBClient(str(self.duckdb_path), read_only=True) as duck_client:
                self._validate_db(duck_client)
                partitions_df = duck_client.retrieve_partitions(self.table_name)
                
                self.logger.debug(f"📊 총 {len(partitions_df)}개 파티션 조회됨")
                return partitions_df

        except Exception as e:
            self.logger.error(f"❌ 파티션 조회 실패: {e}")
            raise

    def search(
        self,
        providers: Optional[List[str]] = None,
        datasets: Optional[List[str]] = None,
        tasks: Optional[List[str]] = None,
        variants: Optional[List[str]] = None,
        text_search: Optional[Dict] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        DB에서 데이터 검색
        
        Args:
            providers: Provider 목록 (None이면 전체)
            datasets: Dataset 목록 (None이면 전체)
            tasks: Task 목록 (None이면 전체)
            variants: Variant 목록 (None이면 전체)
            text_search: 텍스트 검색 설정 {"column": str, "text": str, "json_path": str}
            limit: 결과 제한 수
            
        Returns:
            검색 결과 DataFrame
        """
        self.logger.info("🔍 검색 시작")
        
        try:
            if not self.duckdb_path.exists():
                raise FileNotFoundError("DB가 없습니다. build_db()로 먼저 생성하세요.")
            
            with DuckDBClient(str(self.duckdb_path), read_only=True) as duck_client:
                self._validate_db(duck_client)
                
                if text_search:
                    # 텍스트 검색
                    results = self._perform_text_search(duck_client, text_search, limit)
                else:
                    # 파티션 기반 검색
                    results = self._perform_partition_search(
                        duck_client, providers, datasets, tasks, variants, limit
                    )

                self.logger.info(f"📊 검색 결과: {len(results):,}개 항목")
                return results

        except Exception as e:
            self.logger.error(f"❌ 검색 실패: {e}")
            raise

    def to_pandas(
        self, 
        search_results: pd.DataFrame, 
        absolute_paths: bool = True,
    ) -> pd.DataFrame:
        self.logger.info("📊 Pandas DataFrame 변환 시작...")
        
        df_copy = search_results.copy()
        
        if absolute_paths and 'path' in df_copy.columns.tolist():
            df_copy['path'] = df_copy['path'].apply(
                lambda x: (self.assets_path / x).as_posix() if isinstance(x, str) and x else x
            )
            self.logger.debug("📁 경로를 절대경로로 변환")
            
        self.logger.info(f"✅ DataFrame 변환 완료: {len(df_copy):,}개 항목")
        return df_copy

    def to_dataset(
        self,
        search_results: pd.DataFrame,
        absolute_paths: bool = True,
        check_path_exists: bool = True,
        include_images: bool = False,
    ):
        self.logger.info("📥 Dataset 객체 생성 시작...")
        
        df_copy = self.to_pandas(search_results, absolute_paths)
        dataset = Dataset.from_pandas(df_copy)
        print(dataset.column_names)
        if check_path_exists and 'path' in dataset.column_names:
            print("?")
            dataset = self._check_file_exist(dataset)
        if include_images:
            dataset = self._add_images_to_dataset(dataset)

        self.logger.info(f"✅ Dataset 객체 생성 완료: {len(dataset):,}개 항목") 
        return dataset
    
    def download(
        self,
        search_results: pd.DataFrame,
        output_path: Union[str, Path],
        format: str = "auto",
        absolute_paths: bool = True,
        check_path_exists: bool = True,
        include_images: bool = False,  # dataset 전용
    ) -> Path:
        """
        검색 결과를 지정된 형식으로 저장
        
        Args:
            search_results: 검색 결과 DataFrame
            output_path: 출력 경로
            format: 출력 형식 ("parquet", "dataset", "auto")
            absolute_paths: 절대 경로 사용 여부
            include_images: 이미지 포함 여부 (dataset만 해당)
            
        Returns:
            저장된 파일/디렉토리 경로
        """
        output_path = Path(output_path)
        
        # auto인 경우 확장자로 형식 결정
        if format == "auto":
            if output_path.suffix.lower() == ".parquet":
                format = "parquet"
            else:
                format = "dataset"  # 기본값
        
        self.logger.info(f"💾 {format.upper()} 형식으로 저장 시작...")
        
        if format == "parquet":
            return self._save_as_parquet(search_results, output_path, absolute_paths)
        elif format == "dataset":
            return self._save_as_dataset(search_results, output_path, include_images, check_path_exists, absolute_paths)
        else:
            raise ValueError(f"지원하지 않는 형식입니다: {format}")
    
    def save_collection(
        self,
        search_results,
        name,
        version = None,
        description: str = "",
    ):
        dataset = self.to_dataset(search_results, absolute_paths=True, check_path_exists=True)
        return self.dataset_manager.save_collection(
            dataset=dataset,
            name=name,
            version=version,
            description=description,
            user_id=self.user_id,
        )
        
    def import_collection(
        self,
        data_file: Union[str, Path, pd.DataFrame, Dataset],
        name: str,
        version: Optional[str] = None,
        description: str = "",
    ) -> str:
        
        dataset = self._load_to_dataset(data_file)
        
        return self.collection_manager.save_collection(
            collection=dataset,
            name=name,
            version=version,
            description=description,
            user_id=self.user_id,
        )
            
    def request_asset_validation(
        self,
        search_results: pd.DataFrame,
        sample_percent: Optional[float] = None
    ) -> Optional[str]:
        try:
            required_columns = ['hash', 'path']
            self.logger.debug(f"필수 컬럼: {required_columns}")
            search_results = search_results[required_columns].dropna(subset=['hash', 'path'])
            search_results = search_results.drop_duplicates(subset=['hash', 'path'])
            if search_results.empty:
                self.logger.warning("⚠️ 검색 결과가 비어 있습니다. 유효성 검사 요청을 건너뜁니다.")
                return None
            self.logger.debug(f"유효성 검사 대상 데이터: {len(search_results):,}개")
            search_data = search_results.to_dict('records')
            self.logger.info(f"🔍 유효성 검사 요청: {len(search_data):,}개 항목")
            response = requests.post(
                f"{self.server_url}/validate-assets",  
                json={
                    "user_id": self.user_id,
                    "search_data": search_data,
                    "sample_percent": sample_percent
                },
                timeout=120,
            )
            
            if response.status_code == 200:
                result = response.json()
                job_id = result.get('job_id')
                status = result.get('status')
                
                if status == 'already_running':
                    self.logger.info("🔄 이미 유효성 검사가 진행 중입니다")
                    return job_id
                elif status == 'started':
                    self.logger.info(f"✅ 파일 존재 여부 검사 시작됨: {job_id}")
                    return job_id
                else:
                    self.logger.warning(f"⚠️ 알 수 없는 상태: {status}")
                    return job_id
            else:
                self.logger.error(f"❌ 검사 요청 실패: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 서버 연결 실패: {e}")
            return None

    def check_db_processes(self):
        """DB 사용 중인 프로세스 확인 (개선된 버전)"""
        print("\n🔍 DB 사용 중인 프로세스 확인")
        print("="*50)
        
        db_path = Path(self.duckdb_path).resolve()  # 절대경로로 변환

        try:
            using_processes = []

            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    # 1. cmdline 검사 (기존 방식)
                    cmdline_match = False
                    if proc.info['cmdline']:
                        cmdline = ' '.join(proc.info['cmdline'])
                        if str(db_path) in cmdline:
                            cmdline_match = True

                    # 2. 열린 파일 디스크립터 검사 (새로운 방식)
                    file_match = False
                    try:
                        process = psutil.Process(proc.info['pid'])
                        open_files = process.open_files()
                        for f in open_files:
                            file_path = Path(f.path).resolve()
                            # DB 파일이나 관련 파일들 확인
                            if (file_path == db_path or 
                                file_path.name == db_path.name or
                                str(file_path).endswith('.duckdb') or
                                str(file_path).endswith('.duckdb.wal') or
                                str(file_path).endswith('.duckdb.tmp')):
                                file_match = True
                                break
                    except (psutil.AccessDenied, psutil.NoSuchProcess):
                        # 권한이 없거나 프로세스가 사라진 경우
                        pass

                    # 3. 메모리 매핑 검사 (추가)
                    memory_match = False
                    try:
                        process = psutil.Process(proc.info['pid'])
                        memory_maps = process.memory_maps()
                        for m in memory_maps:
                            if str(db_path) in m.path:
                                memory_match = True
                                break
                    except (psutil.AccessDenied, psutil.NoSuchProcess, AttributeError):
                        # 일부 시스템에서는 memory_maps()가 없을 수 있음
                        pass

                    # 하나라도 매치되면 DB 사용 중인 프로세스
                    if cmdline_match or file_match or memory_match:
                        match_type = []
                        if cmdline_match: match_type.append("cmdline")
                        if file_match: match_type.append("open_files")
                        if memory_match: match_type.append("memory_map")
                        
                        using_processes.append({
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'cmdline': cmdline[:100] + '...' if len(cmdline) > 100 else cmdline,
                            'match_type': ', '.join(match_type)
                        })
                        
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            # DB 파일 상태 정보도 포함
            db_info = {
                'path': str(db_path),
                'exists': db_path.exists()
            }

            if db_path.exists():
                stat = db_path.stat()
                db_info.update({
                    'size': stat.st_size,
                    'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat()
                })

                # WAL 파일 확인
                wal_file = db_path.with_suffix('.duckdb.wal')
                db_info['has_wal'] = wal_file.exists()

            result = {
                'processes': using_processes,
                'db_info': db_info
            }

            self.logger.info(f"📊 DB 프로세스 확인 완료: {len(using_processes)}개 프로세스 발견")
            return result

        except Exception as e:
            self.logger.error(f"❌ 프로세스 확인 실패: {e}")
            return {'error': str(e)}
     
    def _check_file_exist(self, dataset):
        """Dataset의 파일 존재 여부를 병렬로 확인"""
        def check_exists(example):
            try:
                if example.get('path'):
                    file_path = Path(example['path'])
                    example['exists'] = file_path.exists()
                else:
                    example['exists'] = False
            except Exception as e:
                self.logger.warning(f"파일 존재 확인 실패: {example.get('path', 'unknown')} - {e}")
                example['exists'] = False
            return example

        self.logger.info("📁 파일 존재 여부 확인 중...")
        dataset_with_exists = dataset.map(
            check_exists,
            desc="파일 존재 확인",
            num_proc=self.num_proc
        )

        # 존재하는 파일만 필터링
        valid_dataset = dataset_with_exists.filter(
            lambda x: x,
            input_columns=['exists'],
            desc="존재하는 파일 필터링",
            num_proc=self.num_proc
        )

        # exists 컬럼 제거
        valid_dataset = valid_dataset.remove_columns(['exists'])

        total_items = len(dataset)
        valid_items = len(valid_dataset)
        self.logger.info(f"📊 파일 존재 확인 결과: {valid_items:,}/{total_items:,} 존재")

        return valid_dataset
    
    def _save_as_parquet(
        self, 
        search_results: pd.DataFrame, 
        output_path: Union[str, Path],
        absolute_paths: bool = True,
    ) -> Path:
        
        df_copy = self.to_pandas(search_results, absolute_paths)
        
        output_path = Path(output_path).with_suffix('.parquet')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_copy.to_parquet(output_path, index=False)
        
        file_size = output_path.stat().st_size / 1024 / 1024
        self.logger.info(f"✅ Parquet 저장 완료: {output_path}")
        self.logger.info(f"📊 {len(df_copy):,}개 항목, {file_size:.1f}MB")
        
        return output_path

    def _save_as_dataset(
        self,
        search_results: pd.DataFrame,
        output_path: Union[str, Path], 
        include_images: bool = False,
        check_path_exists: bool = True,
        absolute_paths: bool = True,
    ) -> Path:
    
        dataset = self.to_dataset(search_results, absolute_paths, check_path_exists, include_images)
        
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        dataset.save_to_disk(str(output_path))
        
        total_size = sum(f.stat().st_size for f in output_path.rglob('*') if f.is_file()) / 1024 / 1024
        
        self.logger.info(f"✅ Dataset 저장 완료: {output_path}")
        self.logger.info(f"📊 {len(dataset):,}개 항목, {total_size:.1f}MB")
        
        return output_path
    
    def _handle_existing_data(self, provider: str, dataset_name: str, task: str = None, 
                            variant: str = None, overwrite: bool = False, is_raw: bool = True):
        """기존 데이터 처리 로직"""
        existing_dirs = self._cleanup_existing_pending(
            provider, dataset_name, task, variant=variant, is_raw=is_raw
        )
        
        if existing_dirs and not overwrite:
            self.logger.warning(f"⚠️ 이미 pending 데이터가 있어 업로드를 건너뜁니다: {len(existing_dirs)}개")
            self.logger.info("💡 덮어쓰려면 overwrite=True를 사용하세요")
            return False
        
        # 기존 데이터 삭제
        for existing_dir in existing_dirs:
            try:
                shutil.rmtree(existing_dir)
                self.logger.info(f"🗑️ 삭제 완료: {existing_dir.name}")
            except Exception as e:
                self.logger.error(f"❌ 삭제 실패: {existing_dir.name} - {e}")
        
        return True

    def _validate_db(self, duck_client):
        """DB 유효성 검사"""
        tables = duck_client.list_tables()
        if tables.empty or self.table_name not in tables['name'].values:
            raise ValueError(f"❌ '{self.table_name}' 테이블이 없습니다. DB가 올바르게 구축되었는지 확인하세요.")

    def _is_db_outdated(self) -> bool:
        """DB가 최신 상태인지 확인"""
        if not self.duckdb_path.exists():
            return True

        db_mtime = self.duckdb_path.stat().st_mtime

        # 가장 최근 Parquet 파일 확인
        latest_parquet_mtime = 0
        for parquet_file in self.catalog_path.rglob("*.parquet"):
            file_mtime = parquet_file.stat().st_mtime
            if file_mtime > latest_parquet_mtime:
                latest_parquet_mtime = file_mtime
        
        return latest_parquet_mtime > db_mtime

    def _cleanup_db_files(self):
        """DB 관련 파일들 정리"""
        files_to_remove = [
            self.duckdb_path,
            self.duckdb_path.with_suffix('.duckdb-wal'),
            self.duckdb_path.with_suffix('.duckdb-shm'),
            self.duckdb_path.with_suffix('.duckdb.wal'),
            self.duckdb_path.with_suffix('.duckdb.tmp'),
            self.duckdb_path.with_suffix('.duckdb.lock')
        ]

        for file_path in files_to_remove:
            if file_path.exists():
                try:
                    file_path.unlink()
                    self.logger.debug(f"🗑️ 삭제: {file_path}")
                except Exception as e:
                    self.logger.warning(f"⚠️ 삭제 실패: {file_path} - {e}")

    def _perform_partition_search(
        self, 
        duck_client, 
        providers, 
        datasets, 
        tasks, 
        variants, 
        limit
    ):
        """파티션 기반 검색 실행"""
        return duck_client.retrieve_with_existing_cols(
            providers=providers,
            datasets=datasets, 
            tasks=tasks,
            variants=variants,
            table=self.table_name,
            limit=limit
        )

    def _perform_text_search(
        self,
        duck_client,
        text_search,
        limit,
    ):
        """텍스트 기반 검색 실행"""
        column = text_search.get("column")
        text = text_search.get("text")
        json_path = text_search.get("json_path")

        if json_path:
            # JSON 검색
            sql = duck_client.json_queries.search_text_in_column(
                table=self.table_name,
                column=column,
                search_text=text,
                search_type="json",
                json_loc=json_path,
                engine="duckdb"
            )
        else:
            # 단순 텍스트 검색
            sql = duck_client.json_queries.search_text_in_column(
                table=self.table_name,
                column=column,
                search_text=text,
                search_type="simple",
                engine="duckdb"
            )

        if limit is not None:
            sql += f" LIMIT {limit}"
            
        return duck_client.execute_query(sql)

    def _add_images_to_dataset(self, dataset):
        def load_image(example):
            try:
                if example.get('path'):
                    image_path = Path(example['path'])
                    pil_image = Image.open(image_path)
                    example['image'] = pil_image
                    example['has_valid_image'] = True
                    return example
            except Exception as e:
                self.logger.warning(f"이미지 로드 실패: {example.get('path', 'unknown')} - {e}")

            example['image'] = None
            example['has_valid_image'] = False
            return example

        self.logger.info("🖼️ 이미지 로딩 중...")
        dataset_with_images = dataset.map(
            load_image,
            desc="이미지 로딩",
            num_proc=self.num_proc
        )
        if len(dataset_with_images) == 0:
            self.logger.warning("⚠️ 이미지가 포함된 데이터가 없습니다.")
            return dataset_with_images
        
        valid_dataset = dataset_with_images.filter(
            lambda x: x,
            desc="유효 이미지 필터링",
            input_columns=['has_valid_image'],
            num_proc=self.num_proc
        )

        # 임시 컬럼 제거
        valid_dataset = valid_dataset.remove_columns(['has_valid_image'])

        total_items = len(dataset)
        valid_items = len(valid_dataset)
        self.logger.info(f"📊 이미지 로딩 결과: {valid_items:,}/{total_items:,} 성공")

        return valid_dataset

    def _check_raw_data_exists(self, provider: str, dataset: str) -> bool:
        try:
            results = self.search(
                providers=[provider],
                datasets=[dataset], 
                tasks=["raw"]
            )
            return not results.empty
        except:
            return False    
    
    def _create_metadata(
        self,
        provider: str,
        dataset: str,
        task: str,
        variant: str,
        total_rows: int,
        data_type: str,
        has_images: bool = False,
        has_files: bool = False,
        dataset_description: str = "",
        original_source: str = "",
        meta: Optional[Dict] = None,
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
            'has_images': has_images,
            'has_files': has_files,
            'total_rows': total_rows,
            'uploaded_by': self.user_id,
            'uploaded_at': datetime.now().isoformat(),
            'file_id': str(uuid.uuid4())[:8],
            
        }
        if meta:
            metadata.update(meta)
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
        return existing_dirs
    
    def _get_file_type(self, data_file) -> str:
        if isinstance(data_file, Dataset):
            return "dataset"
        elif isinstance(data_file, pd.DataFrame):
            return "dataframe"
        elif isinstance(data_file, (str, Path)):
            data_path = Path(data_file).resolve()
            if not data_path.exists():
                raise FileNotFoundError(f"❌ 데이터 파일이 존재하지 않습니다: {data_path}")
            
            if data_path.is_dir():
                return "datasets_folder"
            elif data_path.suffix == '.parquet':
                return "parquet"
            else:
                raise ValueError(f"❌ 지원하지 않는 파일 형식: {data_path.suffix}")
        else:
            raise TypeError(f"❌ 지원하지 않는 데이터 타입: {type(data_file)}. "
                            "Dataset, pandas DataFrame, str 또는 Path 객체만 지원합니다.")
            
    def _load_to_dataset(self, data_file) -> Dataset:
        data_type = self._get_file_type(data_file)
        if data_type == "dataset":
            self.logger.info(f"🤗 Hugging Face Dataset 로드 중: {len(data_file)} 행")
            try:
                dataset_obj = data_file  # 이미 Dataset 객체이므로 그대로 사용
                self.logger.info(f"✅ Hugging Face Dataset 로드 완료: {len(dataset_obj)} 행")
                
                # Dataset 정보 로깅
                if hasattr(dataset_obj, 'features'):
                    features = list(dataset_obj.features.keys())
                    self.logger.info(f"📋 Dataset features: {features}")
                    
            except Exception as e:
                raise ValueError(f"❌ Hugging Face Dataset 처리 실패: {e}")
        
        elif data_type == "dataframe":
            self.logger.info(f"📊 pandas DataFrame 로드 중: {len(data_file)} 행")
            try:
                dataset_obj = Dataset.from_pandas(data_file, preserve_index=False)
                self.logger.info(f"✅ pandas DataFrame 로드 완료: {len(data_file)} 행")
            except Exception as e:
                raise ValueError(f"❌ pandas DataFrame 변환 실패: {e}")
                
        elif data_type == "datasets_folder":
            data_path = Path(data_file).resolve()
            self.logger.info(f"📂 datasets 폴더 로드 중: {data_path}")
            try:
                dataset_obj = load_from_disk(str(data_path))
                self.logger.info(f"✅ datasets 폴더 로드 완료: {len(dataset_obj)} 행")
            except Exception as e:
                raise ValueError(f"❌ datasets 폴더 로드 실패: {e}")
                
        elif data_type == "parquet_file":
            data_path = Path(data_file).resolve()
            self.logger.info(f"📂 Parquet 파일 로드 중: {data_path}")
            try:
                df = pd.read_parquet(data_path)
                dataset_obj = Dataset.from_pandas(df)
                self.logger.info(f"✅ Parquet 파일 로드 완료: {len(df)} 행")
            except Exception as e:
                raise ValueError(f"❌ Parquet 파일 로드 실패: {e}")
        
        else:            
            raise ValueError(f"❌ 지원하지 않는 데이터 타입: {data_type}. "
                "Dataset, pandas DataFrame, datasets 폴더 또는 Parquet 파일만 지원합니다.")
            
        return dataset_obj
        
    def _load_data(self, data_file) -> tuple[Dataset, dict]:
        
        dataset_obj = self._load_to_dataset(data_file)
        
        self.logger.info(f"✅ 데이터 파일 로드 완료: {data_file}")
        column_names = dataset_obj.column_names
        self.logger.info(f"데이터셋 컬럼: {column_names}")

        metadata_columns_to_remove = [
            'provider', 'dataset', 'task', 'variant', 
            'data_type', 'uploaded_by', 'uploaded_at', 'file_id'
        ]

        columns_to_remove = [col for col in metadata_columns_to_remove 
                            if col in dataset_obj.column_names]

        if columns_to_remove:
            dataset_obj = dataset_obj.remove_columns(columns_to_remove)
            self.logger.info(f"🗑️ 기존 메타데이터 컬럼 제거: {columns_to_remove}")
            
       # 통합된 컬럼 타입 변환 처리 (JSON dumps + 이미지)
        
        file_info = self._detect_file_columns_and_type(dataset_obj)
        self.logger.debug(f"📂 파일 정보: {file_info}")
        if file_info['process_assets']:
            dataset_obj = self._normalize_column_names(dataset_obj, file_info)

            self.logger.info(f"📄 파일 분석 결과: variant={file_info['type']}, "
                           f"이미지컬럼={file_info['image_columns']}, "
                           f"파일컬럼={file_info['file_columns']}, "
                           f"확장자={file_info['extensions']}")
        else:
            self.logger.debug("📄 Assets 컬럼 처리 생략")
            
        dataset_obj = self._process_cast_columns(dataset_obj)
        return dataset_obj, file_info

    def _detect_file_columns_and_type(
        self,
        dataset_obj: Dataset,
    ) -> Dict:
        """파일 컬럼들을 찾고 확장자 기반으로 type 결정"""
        result = {
            'image_columns': [],
            'file_columns': [],
            'extensions': set(),
            'type': 'text'
        }
        for key in dataset_obj.column_names:
            sample_value = dataset_obj[0][key]
            
            # PIL Image나 bytes 데이터인 경우
            if key in self.image_data_candidates:
                result['image_columns'].append(key)
            # 경로 기반 파일인 경우
            elif key in self.file_path_candidates:
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
        result['has_image_data'] = bool(result['image_columns'])
        result['has_file_paths'] = bool(result['file_columns'])
        
        has_image_data = result['has_image_data']
        has_file_paths = result['has_file_paths']
        extensions = result['extensions']
        
        result['process_assets'] = has_image_data or has_file_paths
        
        if has_image_data and has_file_paths:
            result['type'] = "mixed"
        elif has_image_data:
            result['type'] = "image"
        elif has_file_paths:
            # 확장자 기반으로 구체적 분류
            if any(ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'] for ext in extensions):
                result['type'] = "image"
            else:
                result['type'] = "files"
                
        
        return result

    def _normalize_column_names(
        self,
        dataset_obj: Dataset,
        file_info: Dict,
    ) -> Dataset:
        """컬럼명을 표준화 (image_columns → image, file_columns → file_path)"""
        
        # 이미지 컬럼 표준화
        if len(file_info['image_columns']):
            image_col = file_info['image_columns'][0]
            self.logger.info(f"🔄 이미지 컬럼 표준화: {image_col} → {self.image_data_key}")
            
            if image_col != self.image_data_key:
                dataset_obj = dataset_obj.rename_column(image_col, self.image_data_key)
                
            dataset_obj = dataset_obj.cast_column(self.image_data_key, DatasetImage())
        
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

    def _save_to_staging(
        self,
        dataset_obj: Dataset,
        metadata: dict,
    ) -> str:
        """데이터셋을 staging 폴더에 저장하고 메타데이터 파일 생성"""
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
            has_file = metadata.get('has_files', False)
            if has_file:
                staging_assets_dir = staging_dir / "assets"
                staging_assets_dir.mkdir(mode=0o775, parents=True, exist_ok=True)
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
            sample_path = Path(sample_value).resolve()
            staging_device = os.stat(staging_assets_dir.parent).st_dev
            source_device = os.stat(sample_path.parent).st_dev
            same_device = (source_device == staging_device)
            
            copy_method = "OS 레벨 cp" if same_device else "Python copy2"
            self.logger.debug(f"📤 파일 복사 모드: {copy_method} (device: {source_device}→{staging_device})")
            def copy_file(example, idx):
                original_path = Path(example[self.file_path_key]).resolve()
                if original_path.exists():
                    ext = original_path.suffix or ""
                    prefix = "file"
                    
                    folder_num = idx // 1000
                    folder_name = f"batch_{folder_num:04d}"
                    new_filename = f"{prefix}_{idx:06d}{ext}"
                    target_dir = staging_assets_dir / folder_name
                    target_path = target_dir / new_filename
                    target_path.parent.mkdir(mode=0o775,parents=True, exist_ok=True)
                    
                    if same_device:
                        result = subprocess.run(
                            ["cp", str(original_path), str(target_path)], 
                            capture_output=True, text=True
                        )
                        if result.returncode != 0:
                            raise RuntimeError(f"cp 명령 실패: {result.stderr}")
                    else:
                        shutil.copy2(original_path, target_path)
                    relative_path = target_path.relative_to(self.staging_pending_path)
                    example[self.file_path_key] = str(relative_path)
                    
                return example
            
            dataset_obj = dataset_obj.map(
                copy_file, 
                with_indices=True,
                num_proc=self.num_proc,
                desc="파일 경로 복사 중",)
            return dataset_obj
        else:
            self.logger.warning(f"⚠️ 파일 경로 컬럼 '{self.file_path_key}'가 유효하지 않거나 존재하지 않습니다: {sample_value}")
            raise ValueError(f"파일 경로 컬럼 '{self.file_path_key}'가 유효하지 않거나 존재하지 않습니다.")
        
    def _add_metadata_columns(self, dataset_obj: Dataset, metadata: Dict):
        """Task 데이터에 메타데이터 컬럼 추가"""
        self.logger.info("📝 Task 메타데이터 컬럼 추가 중")
        
        required_fields = self.schema_manager.get_required_fields(metadata['task'])
        self.logger.debug(f"필수 필드: {required_fields}")
        self.logger.debug(f"현재 columns: {dataset_obj.column_names}")
        
        # 데이터셋 길이
        num_rows = len(dataset_obj)
        # 필수 필드들만 컬럼으로 추가
        added_columns = []
        
        for field in required_fields:
            value = metadata.get(field)
            if value is None:
                self.logger.warning(f"⚠️ 필수 필드 '{field}'가 메타데이터에 없습니다. 추가하지 않습니다.")
                raise ValueError(f"필수 필드 '{field}'를 추가해주세요.")
            column_data = [value] * num_rows
            dataset_obj = dataset_obj.add_column(field, column_data)
            added_columns.append(f"{field}={value}")
            self.logger.debug(f"📝 컬럼 추가: {field} = {value}")
        if added_columns:
            self.logger.info(f"✅ 필수 필드 컬럼 추가 완료: {', '.join(added_columns)}")
        else:
            self.logger.info("📝 추가할 필수 필드 컬럼 없음")
            
        return dataset_obj
    
            
if __name__ == "__main__":
    from utils.config import Config
    config = Config()
    manager = DatalakeClient(
        user_id="user",
        log_level="DEBUG",
    )
    
    manager.show_server_dashboard()
    
    manager.schema_manager.show_schema_info()
    manager.schema_manager.add_provider("example_provider")
    manager.schema_manager.remove_provider("example_provider")
    manager.schema_manager.add_provider("example_provider")
    manager.schema_manager.add_task("example_task", required_fields=["field1", "field2"], allowed_values={"field1": ["value1", "value2"]})
    staging_dir, job_id = manager.upload_raw(
        data_file="test.parquet",
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
