import argparse
import json
import shutil
import pandas as pd
import psutil

from datasets import Dataset
from PIL import Image
from pathlib import Path
from datetime import datetime

from managers.datalake_client import DatalakeClient  
from client.src.core.duckdb_client import DuckDBClient

class CatalogError(Exception):
    """Catalog 관련 오류"""
    pass

class CatalogNotFoundError(CatalogError):
    """Catalog DB 파일이 없음"""
    pass

class CatalogEmptyError(CatalogError):
    """Catalog에 데이터가 없음"""
    pass

class CatalogLockError(CatalogError):
    """Catalog DB가 잠금 상태"""
    pass


class DataManagerCLI:
    """Data Manager CLI 인터페이스"""
    
    def __init__(
        self, 
        base_path: str = "/mnt/AI_NAS/datalake",
        nas_api_url: str = "http://192.168.20.62:8091",
        log_level: str = "INFO",
        num_proc: int = 16,
    ):
        self.data_manager = DatalakeClient(
            base_path=base_path,
            nas_api_url=nas_api_url,
            log_level=log_level,
            num_proc=num_proc
        )
        self.schema_manager = self.data_manager.schema_manager
        self.duckdb_path = self.data_manager.base_path / "db" / "catalog.duckdb"
    
    def show_catalog_db_info(self):
        """Catalog DB 정보 표시"""
        print("\n📊 Catalog DB 정보")
        print("="*50)
        
        try:
            db_path = self.duckdb_path
            
            if not db_path.exists():
                print("❌ Catalog DB 파일이 없습니다.")
                print(f"💡 'python cli.py catalog rebuild' 명령으로 생성할 수 있습니다.")
                return False
            
            # DB 기본 정보
            db_size = db_path.stat().st_size / 1024 / 1024
            from datetime import datetime
            db_mtime = datetime.fromtimestamp(db_path.stat().st_mtime)
            
            print(f"📁 DB 파일: {db_path}")
            print(f"💾 파일 크기: {db_size:.1f}MB")
            print(f"🕒 수정 시간: {db_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
            
            with DuckDBClient(str(db_path)) as duck_client:
                # 테이블 정보
                tables = duck_client.list_tables()
                print(f"\n📋 테이블: {len(tables)}개")
                for _, table in tables.iterrows():
                    print(f"  • {table['name']}")
                
                if 'catalog' in tables['name'].values:
                    # Catalog 테이블 상세 정보
                    count_result = duck_client.execute_query("SELECT COUNT(*) as total FROM catalog")
                    total_rows = count_result['total'].iloc[0]
                    
                    partitions_df = duck_client.retrieve_partitions("catalog")
                    
                    print(f"\n📊 Catalog 테이블:")
                    print(f"  📈 총 행 수: {total_rows:,}개")
                    print(f"  🏷️ 파티션: {len(partitions_df)}개")
                    
                    # 상위 Provider별 통계
                    if not partitions_df.empty:
                        provider_stats = partitions_df.groupby('provider').size().sort_values(ascending=False)
                        print(f"\n🏢 Provider별 파티션 수:")
                        for provider, count in provider_stats.head(5).items():
                            print(f"  • {provider}: {count}개")
                        
                        if len(provider_stats) > 5:
                            print(f"  ... 외 {len(provider_stats) - 5}개")
                
                return True
                
        except Exception as e:
            print(f"❌ DB 정보 조회 실패: {e}")
            return False
        
    def create_provider_interactive(self):
        """대화형 Provider 생성"""
        print("\n" + "="*50)
        print("🏢 새 Provider 생성")
        print("="*50)
        
        try:
            # Provider 이름 입력
            provider_name = input("🏢 Provider 이름을 입력하세요: ").strip()
            if not provider_name:
                print("❌ Provider 이름이 필요합니다.")
                return False
            
            # 기존 Provider 확인
            if provider_name in self.schema_manager.get_all_providers():
                print(f"⚠️ Provider '{provider_name}'가 이미 존재합니다.")
                return False
            
            # 확인 및 생성
            confirm = input(f"\nProvider '{provider_name}'를 생성하시겠습니까? (y/N): ").strip().lower()
            if confirm in ['y', 'yes']:
                result = self.schema_manager.add_provider(provider_name)
                if result:
                    print(f"✅ Provider '{provider_name}' 생성 완료!")
                    return True
                else:
                    print(f"❌ Provider 생성 실패")
                    return False
            else:
                print("❌ 생성이 취소되었습니다.")
                return False
                
        except KeyboardInterrupt:
            print("\n❌ 생성이 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Provider 생성 중 오류: {e}")
            return False
    
    def create_task_interactive(self):
        """대화형 Task 생성"""
        print("\n" + "="*50)
        print("🔧 새 Task 생성")
        print("="*50)
        
        try:
            # Task 이름 입력
            task_name = input("📝 Task 이름을 입력하세요: ").strip()
            if not task_name:
                print("❌ Task 이름이 필요합니다.")
                return False
            
            # 기존 Task 확인
            existing_tasks = self.schema_manager.get_all_tasks()
            if task_name in existing_tasks:
                print(f"⚠️ Task '{task_name}'가 이미 존재합니다.")
                update = input("업데이트하시겠습니까? (y/N): ").strip().lower()
                if update not in ['y', 'yes']:
                    return False
            
            # 필수 필드 입력
            print("\n📝 필수 필드 설정 (Enter로 완료)")
            required_fields = []
            while True:
                field = input(f"필수 필드 #{len(required_fields)+1}: ").strip()
                if not field:
                    break
                required_fields.append(field)
                print(f"  ✅ 추가됨: {field}")
            
            # 허용 값 설정
            print("\n🔧 허용 값 설정")
            allowed_values = {}
            for field in required_fields:
                values_input = input(f"{field}의 허용 값 (쉼표로 구분, 생략 가능): ").strip()
                if values_input:
                    values = [v.strip() for v in values_input.split(',') if v.strip()]
                    if values:
                        allowed_values[field] = values
                        print(f"  ✅ {field}: {values}")
            
            # 확인 및 생성
            print(f"\n📋 Task 설정 확인:")
            print(f"  이름: {task_name}")
            print(f"  필수 필드: {required_fields}")
            print(f"  허용 값: {allowed_values}")
            
            confirm = input("\n생성하시겠습니까? (y/N): ").strip().lower()
            if confirm in ['y', 'yes']:
                if task_name in existing_tasks:
                    result = self.schema_manager.update_task(task_name, required_fields, allowed_values)
                else:
                    result = self.schema_manager.add_task(task_name, required_fields, allowed_values)
                
                if result:
                    print(f"✅ Task '{task_name}' 생성/업데이트 완료!")
                    return True
                else:
                    print(f"❌ Task 생성/업데이트 실패")
                    return False
            else:
                print("❌ 생성이 취소되었습니다.")
                return False
                
        except KeyboardInterrupt:
            print("\n❌ 생성이 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Task 생성 중 오류: {e}")
            return False

    def remove_provider_interactive(self):
        """대화형 Provider 제거"""
        providers = self.schema_manager.get_all_providers()
        if not providers:
            print("❌ 제거할 Provider가 없습니다.")
            return False
        
        print("\n🏢 등록된 Provider:")
        for i, provider in enumerate(providers, 1):
            print(f"  {i}. {provider}")
        
        try:
            choice = input("\n제거할 Provider 번호 또는 이름: ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(providers):
                    provider = providers[idx]
                else:
                    print("❌ 잘못된 번호입니다.")
                    return False
            else:
                provider = choice
                if provider not in providers:
                    print(f"❌ Provider '{provider}'가 존재하지 않습니다.")
                    return False
            
            confirm = input(f"\nProvider '{provider}'를 제거하시겠습니까? (y/N): ").strip().lower()
            if confirm in ['y', 'yes']:
                result = self.schema_manager.remove_provider(provider)
                if result:
                    print(f"✅ Provider '{provider}' 제거 완료!")
                    return True
                else:
                    print(f"❌ Provider 제거 실패")
                    return False
            else:
                print("❌ 제거가 취소되었습니다.")
                return False
                
        except KeyboardInterrupt:
            print("\n❌ 제거가 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Provider 제거 중 오류: {e}")
            return False

    def remove_task_interactive(self):
        """대화형 Task 제거"""
        tasks = self.schema_manager.get_all_tasks()
        if not tasks:
            print("❌ 제거할 Task가 없습니다.")
            return False
        
        print("\n📝 등록된 Task:")
        task_names = list(tasks.keys())
        for i, task_name in enumerate(task_names, 1):
            print(f"  {i}. {task_name}")
        
        try:
            choice = input("\n제거할 Task 번호 또는 이름: ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(task_names):
                    task = task_names[idx]
                else:
                    print("❌ 잘못된 번호입니다.")
                    return False
            else:
                task = choice
                if task not in tasks:
                    print(f"❌ Task '{task}'가 존재하지 않습니다.")
                    return False
            
            confirm = input(f"\nTask '{task}'를 제거하시겠습니까? (y/N): ").strip().lower()
            if confirm in ['y', 'yes']:
                result = self.schema_manager.remove_task(task)
                if result:
                    print(f"✅ Task '{task}' 제거 완료!")
                    return True
                else:
                    print(f"❌ Task 제거 실패")
                    return False
            else:
                print("❌ 제거가 취소되었습니다.")
                return False
                
        except KeyboardInterrupt:
            print("\n❌ 제거가 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Task 제거 중 오류: {e}")
            return False
    
    def upload_data_interactive(self):
        """대화형 데이터 업로드"""
        print("\n" + "="*50)
        print("📥 데이터 업로드")
        print("="*50)
        
        try:
            # 1. 데이터 파일 경로 입력
            data_file = input("📁 데이터 파일 경로: ").strip()
            if not data_file or not Path(data_file).exists():
                print("❌ 유효한 파일 경로를 입력해주세요.")
                return False
            
            # 2. 데이터 타입 선택 (가장 중요한 분기점)
            data_type = input("\n📝 데이터 타입 (raw/task) [raw]: ").strip().lower() or "raw"
            if data_type not in ["raw", "task"]:
                print("❌ 잘못된 데이터 타입입니다. (raw 또는 task)")
                return False
            
            # 3. Provider 선택
            providers = self.schema_manager.get_all_providers()
            if not providers:
                print("❌ 등록된 Provider가 없습니다. 먼저 Provider를 생성해주세요.")
                return False
                
            print(f"\n🏢 사용 가능한 Provider:")
            for i, provider in enumerate(providers, 1):
                print(f"  {i}. {provider}")
            
            provider_choice = input("Provider 번호 또는 이름 입력: ").strip()
            if provider_choice.isdigit():
                idx = int(provider_choice) - 1
                if 0 <= idx < len(providers):
                    provider = providers[idx]
                else:
                    print("❌ 잘못된 번호입니다.")
                    return False
            else:
                provider = provider_choice
                if provider not in providers:
                    print(f"❌ Provider '{provider}'가 존재하지 않습니다.")
                    return False
            
            # 4. 데이터 타입별 플로우
            if data_type == "raw":
                # Raw 데이터: 새 Dataset 생성
                dataset = input("\n📦 새 Dataset 이름: ").strip()
                if not dataset:
                    print("❌ Dataset 이름이 필요합니다.")
                    return False
                
                description = input("📄 데이터셋 설명 (선택사항): ").strip()
                source = input("🔗 원본 소스 URL (선택사항): ").strip()
                
                print(f"\n📋 업로드 정보:")
                print(f"  📁 파일: {data_file}")
                print(f"  📝 타입: Raw 데이터")
                print(f"  🏢 Provider: {provider}")
                print(f"  📦 Dataset: {dataset} (새로 생성)")
                if description:
                    print(f"  📄 설명: {description}")
                if source:
                    print(f"  🔗 소스: {source}")
                
                confirm = input("\n업로드하시겠습니까? (y/N): ").strip().lower()
                if confirm in ['y', 'yes']:
                    staging_dir, job_id = self.data_manager.upload_raw_data(
                        data_file=data_file,
                        provider=provider,
                        dataset=dataset,
                        dataset_description=description,
                        original_source=source
                    )
                    print(f"✅ 업로드 완료: {staging_dir}")
                    print("💡 'python cli.py process start' 명령으로 처리를 시작할 수 있습니다.")
                    return True
                    
            elif data_type == "task":
                # Task 데이터: 기존 Dataset에서 선택
                print(f"\n📦 기존 Dataset 선택:")
                print("💡 Task 데이터는 기존에 업로드된 raw 데이터에서 추출됩니다.")
                
                # 해당 Provider의 기존 dataset 목록 조회
                catalog_path = self.data_manager.catalog_path / f"provider={provider}"
                existing_datasets = []
                
                if catalog_path.exists():
                    for dataset_dir in catalog_path.iterdir():
                        if dataset_dir.is_dir() and dataset_dir.name.startswith("dataset="):
                            dataset_name = dataset_dir.name.replace("dataset=", "")
                            existing_datasets.append(dataset_name)
                
                if not existing_datasets:
                    print(f"❌ Provider '{provider}'에 업로드된 데이터가 없습니다.")
                    print("💡 먼저 raw 데이터를 업로드해주세요.")
                    return False
                
                print(f"\n📦 사용 가능한 Dataset ({len(existing_datasets)}개):")
                for i, dataset_name in enumerate(existing_datasets, 1):
                    print(f"  {i}. {dataset_name}")
                
                dataset_choice = input("Dataset 번호 또는 이름 입력: ").strip()
                if dataset_choice.isdigit():
                    idx = int(dataset_choice) - 1
                    if 0 <= idx < len(existing_datasets):
                        dataset = existing_datasets[idx]
                    else:
                        print("❌ 잘못된 번호입니다.")
                        return False
                else:
                    dataset = dataset_choice
                    if dataset not in existing_datasets:
                        print(f"❌ Dataset '{dataset}'가 존재하지 않습니다.")
                        return False
                
                # Task 선택
                tasks = self.schema_manager.get_all_tasks()
                if not tasks:
                    print("❌ 등록된 Task가 없습니다. 먼저 Task를 생성해주세요.")
                    return False
                    
                print(f"\n📝 사용 가능한 Task:")
                task_names = list(tasks.keys())
                for i, task_name in enumerate(task_names, 1):
                    print(f"  {i}. {task_name}")
                
                task_choice = input("Task 번호 또는 이름 입력: ").strip()
                if task_choice.isdigit():
                    idx = int(task_choice) - 1
                    if 0 <= idx < len(task_names):
                        task = task_names[idx]
                    else:
                        print("❌ 잘못된 번호입니다.")
                        return False
                else:
                    task = task_choice
                    if task not in tasks:
                        print(f"❌ Task '{task}'가 존재하지 않습니다.")
                        return False
                
                # Variant 입력
                variant = input("\n🏷️ Variant 이름: ").strip()
                if not variant:
                    print("❌ Variant 이름이 필요합니다.")
                    return False
                
                # 필수 필드 입력
                all_tasks = self.data_manager.schema_manager.get_all_tasks()
                task_info = all_tasks.get(task, {})
                required_fields = task_info.get('required_fields', [])
                allowed_values = task_info.get('allowed_values', {})
                
                metadata = {}
                if required_fields:
                    print(f"\n📝 필수 필드 입력:")
                    for field in required_fields:
                        if field in allowed_values:
                            print(f"  {field} 허용값: {allowed_values[field]}")
                        value = input(f"  {field}: ").strip()
                        if not value:
                            print(f"❌ 필수 필드 '{field}'가 누락되었습니다.")
                            return False
                        metadata[field] = value
                
                # 검증
                is_valid, error_msg = self.data_manager.schema_manager.validate_task_metadata(task, metadata)
                if not is_valid:
                    print(f"❌ 검증 실패: {error_msg}")
                    return False
                
                print(f"\n📋 업로드 정보:")
                print(f"  📁 파일: {data_file}")
                print(f"  📝 타입: Task 데이터")
                print(f"  🏢 Provider: {provider}")
                print(f"  📦 Dataset: {dataset} (기존)")
                print(f"  📝 Task: {task}")
                print(f"  🏷️ Variant: {variant}")
                print(f"  📋 메타데이터: {metadata}")
                
                confirm = input("\n업로드하시겠습니까? (y/N): ").strip().lower()
                if confirm in ['y', 'yes']:
                    staging_dir, job_id = self.data_manager.upload_task_data(
                        data_file=data_file,
                        provider=provider,
                        dataset=dataset,
                        task=task,
                        variant=variant,
                        **metadata
                    )
                    print(f"✅ 업로드 완료: {staging_dir}")
                    print("💡 'python cli.py process start' 명령으로 처리를 시작할 수 있습니다.")
                    return True
                
        except KeyboardInterrupt:
            print("\n❌ 업로드가 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ 업로드 중 오류: {e}")
            return False
    
    def download_data_interactive(self):
        """대화형 데이터 다운로드"""
        print("\n" + "="*50)
        print("📥 데이터 다운로드")
        print("="*50)
        
        try:
            db_path = self.duckdb_path
            
            # DB 파일 존재 확인
            if not db_path.exists():
                raise CatalogNotFoundError(
                    "Catalog DB 파일이 없습니다. "
                    "'python cli.py catalog update' 명령으로 먼저 DB를 생성해주세요."
                )
            
            # Read-only 모드로 DuckDB 연결
            try:
                with DuckDBClient(str(db_path), read_only=True) as duck_client:
                    print("🔄 Catalog 데이터 로딩 중...")
                    catalog_path = self.data_manager.catalog_path

                    # 업데이트 필요 여부만 확인하고 강제 업데이트하지 않음
                    db_is_current = self._check_and_update_catalog_db(duck_client, catalog_path)
                    if not db_is_current:
                        raise CatalogError(
                            "Catalog DB가 최신 상태가 아닙니다. "
                            "'python cli.py catalog update' 명령으로 DB를 업데이트해주세요."
                        )
                    
                    # 사용 가능한 파티션 확인
                    partitions_df = duck_client.retrieve_partitions("catalog")
                    if partitions_df.empty:
                        raise CatalogEmptyError(
                            "Catalog에 데이터가 없습니다. "
                            "'python cli.py catalog update' 명령으로 DB를 다시 구축해보세요."
                        )
                        
                    print(f"📊 {len(partitions_df)} 개 파티션 사용 가능")
            
                    search_results = self._perform_search(duck_client, partitions_df)
                    
                    if search_results is None or search_results.empty:
                        raise CatalogError("검색 결과가 없습니다.")
                    
                    print(f"\n📊 검색 결과: {len(search_results):,}개 항목")
                    
                    # 결과 미리보기
                    print("\n📋 결과 미리보기:")
                    print(search_results.head(10))
                    if len(search_results) > 3:
                        print(f"... (총 {len(search_results):,}개 항목)")
                    
                    # 다운로드 옵션 선택
                    return self._download_options(search_results)

                    
            except Exception as db_error:
                if "lock" in str(db_error).lower() or "locked" in str(db_error).lower():
                    raise CatalogLockError(
                        "DB가 다른 프로세스에서 사용 중입니다. "
                        "다른 CLI 세션이나 Jupyter 노트북에서 DB를 사용 중인지 확인해주세요."
                    ) from db_error
                else:
                    raise CatalogError(f"DB 연결 오류: {db_error}") from db_error
                    
        except KeyboardInterrupt:
            raise CatalogError("다운로드가 사용자에 의해 취소되었습니다.") from None
        except CatalogError:
            # 이미 우리가 정의한 예외는 그대로 전파
            raise
        except Exception as e:
            raise CatalogError(f"다운로드 중 예상치 못한 오류: {e}") from e

    def trigger_processing(self):
        """NAS 처리 수동 시작"""
        print("\n" + "="*50)
        print("🔄 NAS 데이터 처리 시작")
        print("="*50)
        
        try:
            # 현재 상태 확인
            status = self.data_manager.get_nas_status()
            if status:
                pending_count = status.get('pending', 0)
                processing_count = status.get('processing', 0)
                
                print(f"📦 Pending: {pending_count}개")
                print(f"🔄 Processing: {processing_count}개")
                
                if pending_count == 0:
                    print("💡 처리할 pending 데이터가 없습니다.")
                    return True
                
                if processing_count > 0:
                    print("⚠️ 이미 처리 중인 작업이 있습니다.")
                    continue_anyway = input("그래도 새 처리를 시작하시겠습니까? (y/N): ").strip().lower()
                    if continue_anyway not in ['y', 'yes']:
                        print("❌ 처리가 취소되었습니다.")
                        return False
            else:
                print("⚠️ NAS 서버 상태를 확인할 수 없습니다.")
                continue_anyway = input("그래도 처리를 시작하시겠습니까? (y/N): ").strip().lower()
                if continue_anyway not in ['y', 'yes']:
                    print("❌ 처리가 취소되었습니다.")
                    return False
            
            # 처리 시작
            job_id = self.data_manager.trigger_nas_processing()
            if job_id:
                print(f"✅ 처리 시작됨: {job_id}")
                
                # 대기 여부 확인
                wait_completion = input("처리 완료까지 대기하시겠습니까? (y/N): ").strip().lower()
                if wait_completion in ['y', 'yes']:
                    try:
                        print("⏳ 처리 완료 대기 중... (Ctrl+C로 중단)")
                        result = self.data_manager.wait_for_job_completion(job_id, timeout=3600)
                        print(f"📊 처리 완료: {result}")
                        return True
                    except KeyboardInterrupt:
                        print("\n⏸️ 대기 중단됨. 백그라운드에서 처리는 계속됩니다.")
                        print(f"💡 'python cli.py process status {job_id}' 명령으로 상태를 확인할 수 있습니다.")
                        return True
                    except Exception as e:
                        print(f"❌ 처리 대기 중 오류: {e}")
                        return False
                else:
                    print(f"🔄 백그라운드에서 처리 중입니다. Job ID: {job_id}")
                    print(f"💡 'python cli.py process status {job_id}' 명령으로 상태를 확인할 수 있습니다.")
                    return True
            else:
                print("❌ 처리 시작에 실패했습니다.")
                return False
                
        except KeyboardInterrupt:
            print("\n❌ 처리가 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ 처리 중 오류: {e}")
            return False

    def check_job_status(self, job_id: str):
        """특정 작업 상태 확인"""
        print(f"\n🔍 작업 상태 확인: {job_id}")
        print("="*50)
        
        try:
            job_status = self.data_manager.get_job_status(job_id)
            if job_status:
                status = job_status.get('status', 'unknown')
                started_at = job_status.get('started_at', 'N/A')
                finished_at = job_status.get('finished_at', 'N/A')
                
                status_emoji = {"running": "🔄", "completed": "✅", "failed": "❌"}.get(status, "❓")
                print(f"{status_emoji} 상태: {status}")
                print(f"⏰ 시작: {started_at}")
                
                if status == 'completed':
                    print(f"🏁 완료: {finished_at}")
                    result = job_status.get('result', {})
                    print(f"📊 성공: {result.get('success', 0)}개")
                    print(f"❌ 실패: {result.get('failed', 0)}개")
                elif status == 'failed':
                    print(f"💥 실패: {finished_at}")
                    error = job_status.get('error', 'Unknown error')
                    print(f"🔍 오류: {error}")
                elif status == 'running':
                    print("🔄 진행 중...")
                    
                return True
            else:
                print(f"❌ 작업을 찾을 수 없습니다: {job_id}")
                return False
                
        except Exception as e:
            print(f"❌ 상태 확인 중 오류: {e}")
            return False

    def list_all_data(self):
        """내 데이터 전체 현황 조회"""
        print("\n📋 내 데이터 현황")
        print("="*50)
        
        total_items = 0
        
        try:
            # 1. 📥 업로드됨 (Pending)
            pending_path = self.data_manager.staging_pending_path
            pending_items = []
            
            if pending_path.exists():
                pending_dirs = [d for d in pending_path.iterdir() if d.is_dir()]
                for pending_dir in sorted(pending_dirs, key=lambda x: x.stat().st_mtime, reverse=True):
                    try:
                        metadata_file = pending_dir / "upload_metadata.json"
                        if metadata_file.exists():
                            with open(metadata_file, 'r', encoding='utf-8') as f:
                                metadata = json.load(f)
                            
                            provider = metadata.get('provider', 'Unknown')
                            dataset = metadata.get('dataset', 'Unknown')
                            task = metadata.get('task', 'Unknown')
                            uploaded_at = metadata.get('uploaded_at', 'Unknown')
                            total_rows = metadata.get('total_rows', 0)
                            
                            try:
                                from datetime import datetime
                                upload_time = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
                                time_str = upload_time.strftime('%m-%d %H:%M')
                            except:
                                time_str = uploaded_at[:10]
                            
                            pending_items.append({
                                'name': f"{provider}/{dataset}/{task}",
                                'time': time_str,
                                'rows': total_rows
                            })
                    except:
                        continue
            
            # 2. 🔄 처리 중/완료 (Jobs)
            jobs = self.data_manager.list_nas_jobs() or []
            recent_jobs = jobs[-5:] if jobs else []  # 최근 5개
            
            # 출력
            if pending_items:
                print(f"\n📥 업로드됨 ({len(pending_items)}개)")
                for item in pending_items:
                    rows_str = f"{item['rows']:,}" if item['rows'] > 0 else "?"
                    print(f"  📦 {item['name']} ({rows_str} rows) - {item['time']}")
                total_items += len(pending_items)
            
            if recent_jobs:
                print(f"\n🔄 처리 작업 ({len(recent_jobs)}개)")
                for job in reversed(recent_jobs):
                    status_emoji = {"running": "🔄", "completed": "✅", "failed": "❌"}.get(job['status'], "❓")
                    job_id_short = job['job_id'][:8] + "..." if len(job['job_id']) > 8 else job['job_id']
                    started_at = job.get('started_at', 'Unknown')
                    try:
                        time_str = started_at.split('T')[1][:5] if 'T' in started_at else started_at[:5]
                    except:
                        time_str = started_at
                    print(f"  {status_emoji} {job_id_short} ({job['status']}) - {time_str}")
                total_items += len(recent_jobs)
            
            # 요약 및 안내
            if total_items == 0:
                print("\n📭 데이터가 없습니다.")
                print("💡 'python cli.py upload' 명령으로 데이터를 업로드해보세요.")
            else:
                if pending_items:
                    print(f"\n💡 'python cli.py process' 명령으로 업로드된 데이터를 처리할 수 있습니다.")
                
            return True
            
        except Exception as e:
            print(f"❌ 데이터 현황 조회 중 오류: {e}")
            return False
   
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
                        if str(db_path) in cmdline or 'catalog.duckdb' in cmdline:
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
            
            if using_processes:
                print(f"⚠️ {len(using_processes)}개 프로세스가 DB를 사용 중:")
                for proc in using_processes:
                    print(f"  PID {proc['pid']}: {proc['name']} (감지: {proc['match_type']})")
                    print(f"    명령어: {proc['cmdline']}")
                
                print(f"\n💡 종료 방법:")
                print(f"  - Jupyter 노트북: 커널 재시작")
                print(f"  - Python 스크립트: Ctrl+C로 종료")
                print(f"  - 강제 종료: kill -9 <PID>")
                
                # 4. lsof로도 한번 더 확인 (Linux/Mac)
                print(f"\n🔍 lsof로 추가 확인:")
                try:
                    import subprocess
                    result = subprocess.run(['lsof', str(db_path)], 
                                        capture_output=True, text=True, timeout=5)
                    if result.stdout:
                        print(result.stdout)
                    else:
                        print("  lsof에서 추가 프로세스 발견되지 않음")
                except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
                    print("  lsof 명령어 사용 불가")
                    
            else:
                print("✅ DB를 사용 중인 프로세스가 없습니다.")
                
                # 그래도 잠금 상태라면 파일 시스템 이슈일 수 있음
                print(f"\n🔍 DB 파일 상태 확인:")
                print(f"  경로: {db_path}")
                print(f"  존재: {db_path.exists()}")
                if db_path.exists():
                    stat = db_path.stat()
                    print(f"  크기: {stat.st_size:,} bytes")
                    print(f"  수정시간: {datetime.fromtimestamp(stat.st_mtime)}")
                    
                    # WAL 파일도 확인
                    wal_file = db_path.with_suffix('.duckdb.wal')
                    if wal_file.exists():
                        print(f"  ⚠️ WAL 파일 존재: {wal_file} (비정상 종료 가능성)")
                        
        except ImportError:
            print("❌ psutil 라이브러리가 필요합니다: pip install psutil")
        except Exception as e:
            print(f"❌ 프로세스 확인 실패: {e}")

    def safe_update_catalog_db(self):
        """잠금 상태 확인 후 안전한 DB 업데이트"""
        print("\n" + "="*50)
        print("🔄 Catalog DB 안전 업데이트")
        print("="*50)
        
        db_path = self.duckdb_path
        
        # 1. 잠금 테스트
        print("🔍 DB 잠금 상태 확인 중...")
        try:
            # 임시 연결로 잠금 테스트
            with DuckDBClient(str(db_path), read_only=False) as test_client:
                test_client.execute_query("SELECT 1")
            print("✅ DB 잠금 없음 - 업데이트 가능")
            
        except Exception as e:
            if "lock" in str(e).lower():
                print("❌ DB가 잠금 상태입니다.")
                print("\n🔍 사용 중인 프로세스 확인:")
                self.check_db_processes()
                
                force = input("\n그래도 강제 업데이트하시겠습니까? (y/N): ").strip().lower()
                if force not in ['y', 'yes']:
                    print("❌ 업데이트가 취소되었습니다.")
                    return False
            else:
                print(f"❌ DB 연결 테스트 실패: {e}")
                return False
        
        # 2. 실제 업데이트
        print("\n🔄 업데이트 시작...")
        return self._build_catalog_db()

    def quick_catalog_check(self):
        """빠른 카탈로그 상태 확인"""
        print("\n📊 Catalog 빠른 상태 확인")
        print("="*40)
        
        try:
            db_path = self.duckdb_path
            catalog_path = self.data_manager.catalog_path
            
            if not db_path.exists():
                print("❌ DB 파일 없음")
                return False
            
            if not catalog_path.exists():
                print("❌ Catalog 디렉토리 없음")
                return False
            
            # 파일 정보
            from datetime import datetime
            db_mtime = datetime.fromtimestamp(db_path.stat().st_mtime)
            db_size = db_path.stat().st_size / 1024 / 1024
            
            # 최신 Parquet 파일 확인
            latest_parquet = None
            latest_parquet_mtime = 0
            
            for parquet_file in catalog_path.rglob("*.parquet"):
                file_mtime = parquet_file.stat().st_mtime
                if file_mtime > latest_parquet_mtime:
                    latest_parquet_mtime = file_mtime
                    latest_parquet = parquet_file
            
            print(f"📁 DB: {db_size:.1f}MB ({db_mtime.strftime('%m-%d %H:%M')})")
            
            if latest_parquet:
                latest_parquet_dt = datetime.fromtimestamp(latest_parquet_mtime)
                print(f"📄 최신 Parquet: {latest_parquet_dt.strftime('%m-%d %H:%M')}")
                
                if latest_parquet_mtime > db_path.stat().st_mtime:
                    print("⚠️ DB 업데이트 필요")
                else:
                    print("✅ DB 최신 상태")
            
            # 잠금 상태 확인
            try:
                with DuckDBClient(str(db_path), read_only=True) as duck_client:
                    tables = duck_client.list_tables()
                    if 'catalog' in tables['name'].values:
                        count = duck_client.execute_query("SELECT COUNT(*) as total FROM catalog")
                        print(f"📊 총 {count['total'].iloc[0]:,}개 행")
                    else:
                        print("❌ catalog 테이블 없음")
            except Exception as e:
                if "lock" in str(e).lower():
                    print("🔒 DB 잠금 상태 (다른 프로세스 사용 중)")
                else:
                    print(f"❌ DB 연결 실패: {e}")
            
            return True
            
        except Exception as e:
            print(f"❌ 상태 확인 실패: {e}")
            return False

    def validate_data_integrity(self, provider=None, generate_report=False):
        """데이터 무결성 검사 (Dataset library 최적화 버전)"""
        print("\n" + "="*50)
        print("🔍 데이터 무결성 검사 (병렬 처리)")
        print("="*50)
        
        issues = {
            'missing_files': [],
        }
        
        try:
            from datasets import Dataset
            
            db_path = self.duckdb_path
            if not db_path.exists():
                raise CatalogNotFoundError("Catalog DB가 없습니다.")
            
            with DuckDBClient(str(db_path), read_only=True) as duck_client:
                # 검사할 데이터 조회
                if provider:
                    print(f"🏢 Provider '{provider}' 검사 중...")
                    query = f"SELECT * FROM catalog WHERE provider = '{provider}'"
                    catalog_data = duck_client.execute_query(query)
                else:
                    print("🌍 전체 데이터 검사 중...")
                    catalog_data = duck_client.execute_query("SELECT * FROM catalog")
                
                total_items = len(catalog_data)
                print(f"📊 검사 대상: {total_items:,}개 항목")
                
                if total_items == 0:
                    print("📭 검사할 데이터가 없습니다.")
                    return True
                
                # DataFrame을 Dataset으로 변환
                dataset = Dataset.from_pandas(catalog_data)
                print(f"🔄 Dataset 생성 완료: {len(dataset):,}개 행")
                
                # 1. 파일 존재 여부 검사 (병렬 처리)
                print("\n1️⃣ 파일 존재 여부 검사 (병렬 처리 중)...")
                
                def check_file_exists(example):
                    """파일 존재 여부 확인"""
                    path_val = example.get('path')
                    if not path_val:
                        example['file_exists'] = None
                        return example
                    
                    file_path = self.data_manager.assets_path / path_val
                    exists = file_path.exists()
                    example['file_exists'] = exists
                    
                    return example
                
                # 병렬로 파일 존재 여부 확인
                dataset_with_file_check = dataset.map(
                    check_file_exists,
                    desc="파일 존재 확인",
                    num_proc=min(self.data_manager.num_proc, 16),
                    load_from_cache_file=False
                )
                
                # 누락된 파일 찾기
                missing_files_data = dataset_with_file_check.filter(
                    lambda x: not x['file_exists'],
                    desc="누락 파일 필터링"
                )
                
                issues['missing_files'] = [
                    {
                        'hash': item['hash'],
                        'path': item.get('path'),
                        'provider': item.get('provider'),
                        'dataset': item.get('dataset'),
                        'task': item.get('task'),
                        'variant': item.get('variant')
                    }
                    for item in missing_files_data
                ]
                
                print(f"    ❌ 누락된 파일: {len(issues['missing_files'])}개")
            
            # 결과 출력
            print("\n" + "="*50)
            print("📋 검사 결과 요약")
            print("="*50)
            
            total_issues = sum(len(issue_list) for issue_list in issues.values())
            
            if total_issues == 0:
                print("✅ 모든 검사 통과! 데이터가 정상입니다.")
                return True
            
            print(f"⚠️ 총 {total_issues}개 문제 발견:")
            
            for issue_type, issue_list in issues.items():
                if issue_list:
                    issue_names = {
                        'missing_files': '누락된 파일',
                    }
                    print(f"  • {issue_names[issue_type]}: {len(issue_list)}개")
            
            # 상위 문제들 샘플 출력
            self._print_issue_samples(issues)
            
            # 처리 옵션
            print(f"\n💡 처리 옵션:")
            if issues['missing_files']:
                print("  - 누락된 파일은 자동으로 복구할 수 없습니다. 수동으로 업로드가 필요합니다.")
            
            if generate_report:
                self._generate_validation_report(issues)
            
            return len(issues['missing_files']) == 0  # 중요한 문제만 False 반환
            
        except ImportError:
            print("❌ datasets 라이브러리가 필요합니다: pip install datasets")
            return False
            
        except Exception as e:
            print(f"❌ 검사 중 오류: {e}")
            return False
    def _print_issue_samples(self, issues):
        """문제 샘플 출력"""
        print(f"\n🔍 주요 문제 샘플:")
        
        # 누락된 파일 샘플
        if issues['missing_files']:
            print(f"\n  📁 누락된 파일 (상위 3개):")
            for item in issues['missing_files'][:3]:
                print(f"    • {item['hash'][:16]}... ({item['provider']}/{item['dataset']})")


    def _generate_validation_report(self, issues):
        """검사 보고서 생성"""
        from datetime import datetime
        
        report_path = Path(f"./validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        try:
            report_data = {
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_issues': sum(len(issue_list) for issue_list in issues.values()),
                    'by_type': {k: len(v) for k, v in issues.items()}
                },
                'details': issues
            }
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            
            print(f"📄 상세 보고서 생성: {report_path}")
            
        except Exception as e:
            print(f"❌ 보고서 생성 실패: {e}")
        
    def _check_and_update_catalog_db(self, duck_client, catalog_path):
        """Catalog DB 상태 확인 및 업데이트 필요 여부 판단 (잠금 오류 방지)"""
        try:
            # 테이블 존재 여부 확인
            tables = duck_client.list_tables()
            if tables.empty or 'catalog' not in tables['name'].values:
                print("📝 새로운 Catalog DB 생성 필요")
                raise Exception("Catalog 테이블이 존재하지 않습니다.")
            
            # DB 파일과 Parquet 파일들의 수정 시간 비교
            db_path = self.duckdb_path
            db_mtime = db_path.stat().st_mtime if db_path.exists() else 0
            
            # 가장 최근 Parquet 파일의 수정 시간 확인
            latest_parquet_mtime = 0
            for parquet_file in catalog_path.rglob("*.parquet"):
                file_mtime = parquet_file.stat().st_mtime
                if file_mtime > latest_parquet_mtime:
                    latest_parquet_mtime = file_mtime
            
            if latest_parquet_mtime > db_mtime:
                print("🔄 Parquet 파일이 DB보다 최신입니다.")
                print("⚠️ 최신 데이터를 보려면 DB 업데이트가 필요합니다.")
                
                # 사용자에게 선택권 제공
                choice = input("\n현재 DB로 계속 진행하시겠습니까? (y/N): ").strip().lower()
                if choice not in ['y', 'yes']:
                    print("❌ 작업이 취소되었습니다.")
                    raise Exception("DB 업데이트가 필요합니다.")
                
                print("✅ 기존 DB로 계속 진행합니다.")
                return True
            else:
                print("✅ DB가 최신 상태입니다.")
                return True
                
        except Exception as e:
            print(f"⚠️ DB 상태 확인 실패: {e}")
            print("💡 'python cli.py catalog update' 명령으로 DB를 업데이트 할 수 있습니다.")
            return False

    def _build_catalog_db(self):
        """Catalog DB 강제 재구축"""
        try:
            db_path = self.duckdb_path
            catalog_path = self.data_manager.catalog_path
            if not catalog_path.exists():
                print("❌ Catalog 디렉토리가 존재하지 않습니다.")
                return False
            
            # # 기존 DB 파일 백업
            # if db_path.exists():
            #     backup_path = db_path.with_suffix('.duckdb.backup')
            #     shutil.copy(db_path, backup_path)
            #     print(f"💾 기존 DB 백업: {backup_path}")
            
            with DuckDBClient(str(db_path), read_only=False) as duck_client:
                # 기존 테이블 삭제 (있다면)
                try:
                    duck_client.execute_query("DROP TABLE IF EXISTS catalog")
                except:
                    pass
                
                # 새로 생성
                duck_client.create_table_from_parquet(
                    "catalog",
                    str(catalog_path / "**" / "*.parquet"),
                    hive_partitioning=True,
                    union_by_name=True
                )
                
                # 결과 확인
                count_result = duck_client.execute_query("SELECT COUNT(*) as total FROM catalog")
                total_rows = count_result['total'].iloc[0]
                
                partitions_df = duck_client.retrieve_partitions("catalog")
                total_partitions = len(partitions_df)
                
                print(f"✅ Catalog DB 재구축 완료!")
                print(f"📊 총 {total_rows:,}개 행, {total_partitions}개 파티션")
                print(f"💾 DB 파일: {db_path}")
                print(f"📁 파일 크기: {db_path.stat().st_size / 1024 / 1024:.1f}MB")
                
                return True
                
        except Exception as e:
            print(f"❌ DB 재구축 실패: {e}")
            return False
        
    def _perform_search(self, duck_client, partitions_df):
        """검색 수행"""
        print("\n🔍 검색 방법을 선택하세요:")
        print("  1. 파티션 기반 검색 (Provider/Dataset/Task/Variant)")
        print("  2. 텍스트 검색 (JSON 라벨 내 텍스트)")

        search_choice = input("검색 방법 (1-2) [1]: ").strip() or "1"
        
        if search_choice == "1":
            search_results = self._partition_search(duck_client, partitions_df)
            print("\n📊 파티션 기반 검색 결과:")
            print(search_results.head(3).to_string(index=False, max_cols=5))
            return search_results
        elif search_choice == "2":
            return self._text_search(duck_client)
        else:
            raise CatalogError("잘못된 검색 방법을 선택했습니다.")
    
    def _partition_search(self, duck_client: DuckDBClient, partitions_df: pd.DataFrame):
        """파티션 기반 검색"""
        
        # 1. Provider 선택
        providers = self._select_items(
            items=sorted(partitions_df['provider'].unique().tolist()),
            name="Provider",
            partitions_df=partitions_df,
            column='provider',
            level="task",
        )
        if not providers:
            return None
        
        # 2. Dataset 선택 (필터링된 결과에서)
        filtered_df = partitions_df[partitions_df['provider'].isin(providers)]
        datasets = self._select_items(
            items=sorted(filtered_df['dataset'].unique().tolist()),
            name="Dataset",
            partitions_df=filtered_df,
            column='dataset',
            level="task",
        )
        if not datasets:
            return None
        
        # 3. Task 선택
        filtered_df = filtered_df[filtered_df['dataset'].isin(datasets)]
        tasks = self._select_items(
            items=sorted(filtered_df['task'].unique().tolist()),
            name="Task",
            partitions_df=filtered_df,
            column='task',
            level='variant',
        )
        if not tasks:
            return None
        
        # 4. Variant 선택
        filtered_df = filtered_df[filtered_df['task'].isin(tasks)]
        variants = self._select_items(
            items=sorted(filtered_df['variant'].unique().tolist()),
            name="Variant",
            partitions_df=filtered_df,
            column='variant',
            level="dataset"
        )
        if not variants:
            return None
        
        # 5. 검색 실행
        print(f"\n🔍 검색 실행:")
        print(f"  Provider: {providers}")
        print(f"  Dataset: {datasets}")
        print(f"  Task: {tasks}")
        print(f"  Variant: {variants}")
        
        return duck_client.retrieve_with_existing_cols(
            providers=providers,
            datasets=datasets,
            tasks=tasks,
            variants=variants,
            table="catalog"
        )

    def _select_items(self, items, name, partitions_df, column, level):
        """아이템 다중 선택"""
        if not items:
            print(f"❌ 사용 가능한 {name}가 없습니다.")
            return None
        
        self._show_matrix(partitions_df, column, level)
                
        print(f"\n{self._get_icon(name)} {name} 선택 ({len(items)}개):")
        for i, item in enumerate(items, 1):
            count = len(partitions_df[partitions_df[column] == item])
            print(f"  {i:2d}. {item} ({count}개)")
            
        print(f"\n선택: 번호(1,2,3), 범위(1-5), 이름, 전체(Enter)")
        user_input = input(f"{name}: ").strip()
        
        # 전체 선택
        if not user_input:
            print(f"✅ 전체 선택 ({len(items)}개)")
            return items
        
        # 선택 파싱
        selected = self._parse_input(user_input, items)
        
        if selected:
            print(f"✅ {len(selected)}개 선택: {selected}")
            return selected
        else:
            print(f"❌ 잘못된 입력")
            return None

    def _parse_input(self, user_input, items):
        """입력 파싱"""
        selected = set()
        parts = user_input.split(',')
        
        for part in parts:
            part = part.strip()
            
            if '-' in part and not part.startswith('-'):
                # 범위: 1-5
                try:
                    start, end = part.split('-', 1)
                    start_idx = int(start) - 1
                    end_idx = int(end) - 1
                    
                    if 0 <= start_idx < len(items) and 0 <= end_idx < len(items):
                        for i in range(min(start_idx, end_idx), max(start_idx, end_idx) + 1):
                            selected.add(items[i])
                except ValueError:
                    print(f"⚠️ 잘못된 범위: {part}")
                    
            elif part.isdigit():
                # 번호: 1, 2, 3
                idx = int(part) - 1
                if 0 <= idx < len(items):
                    selected.add(items[idx])
                else:
                    print(f"⚠️ 잘못된 번호: {part}")
                    
            else:
                # 이름: imagenet, coco
                if part in items:
                    selected.add(part)
                else:
                    print(f"⚠️ 찾을 수 없음: {part}")
        
        return list(selected) if selected else None

    def _get_icon(self, name):
        """아이콘 반환"""
        icons = {
            "Provider": "🏢",
            "Dataset": "📦", 
            "Task": "📝",
            "Variant": "🏷️"
        }
        return icons.get(name, "📋")

    def _show_matrix(self, partitions_df, level1, level2):
        print(f"\n📊 {level1.title()}-{level2.title()} 조합 매트릭스:")
        
        items1 = sorted(partitions_df[level1].unique())
        items2 = sorted(partitions_df[level2].unique())
        
        # 헤더 (첫 번째 컬럼 너비 조정)
        col_width = max(len(level1.title()), 15)
        print(level1.title().ljust(col_width), end=" | ")
        for item2 in items2:
            print(item2[:8].ljust(8), end=" | ")
        print()
        
        # 구분선
        print("-" * (col_width + len(items2) * 11))
        
        # 데이터 행
        for idx, item1 in enumerate(items1):
            data1 = partitions_df[partitions_df[level1] == item1]
            #print(item1[:col_width-1].ljust(col_width), end=" | ")
            item_name = f"{idx+1:>2}. {item1}"
            print(f"{item_name[:col_width-1].ljust(col_width)}", end=" | ")
            
            for item2 in items2:
                data12 = data1[data1[level2] == item2]
                count = len(data12) if not data12.empty else 0
                
                if count > 0:
                    print(f"{count:>3}".ljust(8), end=" | ")
                else:
                    print(" - ".ljust(8), end=" | ")
            print()
        
        print(f"💡 숫자: 파티션 수, '-': 조합 없음")


    def _text_search(self, duck_client: DuckDBClient):
        """텍스트 기반 검색"""
        print("\n🔤 텍스트 검색:")
        
        search_text = input("검색할 텍스트: ").strip()
        if not search_text:
            print("❌ 검색 텍스트가 필요합니다.")
            return None
        
        columns_df = duck_client.get_table_info("catalog")
        columns = columns_df['column_name'].tolist()
        # 컬럼 선택
        print(f"\n📝 컬럼 선택:")
        for i, col in enumerate(columns, 1):
            print(f"  {i}. {col}")
        
        col_choice = input(f"컬럼 선택 (1-{len(columns)}) [1]: ").strip() or "1"
        if col_choice.isdigit():
            idx = int(col_choice) - 1
            if 0 <= idx < len(columns):
                selected_column = columns[idx]
            else:
                print("❌ 잘못된 번호입니다.")
                return None
        else:
            print("❌ 잘못된 입력입니다.")
            return None
            
        # 🆕 검색 방법 선택
        print(f"\n🔍 '{selected_column}' 컬럼에서 검색 방법:")
        print("  1. 단순 텍스트 검색 (LIKE)")
        print("  2. JSON 파싱 후 검색")
        
        method_choice = input("검색 방법 (1-2) [1]: ").strip() or "1"
        
        if method_choice == "1":
            # 단순 LIKE 검색
            print(f"\n🔍 단순 텍스트 검색 실행:")
            print(f"  텍스트: '{search_text}'")
            print(f"  컬럼: {selected_column}")
            
            sql = duck_client.json_queries.search_text_in_column(
                table="catalog",
                column=selected_column,
                search_text=search_text,
                search_type="simple",
                engine="duckdb"
            )
            return duck_client.execute_query(sql)
            
        elif method_choice == "2":
            # JSON 파싱 검색
            json_path = input("JSON 경로 (예: $.image.text.content): ").strip()
            if not json_path:
                print("❌ JSON 경로가 필요합니다.")
                return None
            
            # Variant 선택 (JSON 검색시에만)
            partitions_df = duck_client.retrieve_partitions("catalog")
            variants = sorted(partitions_df['variant'].unique().tolist())
            
            print(f"\n🏷️ 사용 가능한 Variant ({len(variants)}개):")
            for i, variant in enumerate(variants, 1):
                count = len(partitions_df[partitions_df['variant'] == variant])
                print(f"  {i}. {variant} ({count}개 파티션)")
            
            variant_choice = input(f"Variant 선택 (1-{len(variants)}) [1]: ").strip() or "1"
            if variant_choice.isdigit():
                idx = int(variant_choice) - 1
                if 0 <= idx < len(variants):
                    selected_variant = variants[idx]
                else:
                    print("❌ 잘못된 번호입니다.")
                    return None
            else:
                if variant_choice in variants:
                    selected_variant = variant_choice
                else:
                    print(f"❌ Variant '{variant_choice}'가 존재하지 않습니다.")
                    return None
            
            print(f"\n🔍 JSON 파싱 검색 실행:")
            print(f"  텍스트: '{search_text}'")
            print(f"  컬럼: {selected_column}")
            print(f"  JSON 경로: {json_path}")
            print(f"  Variant: {selected_variant}")
            
            # Variant 조건 추가
            partition_conditions = {"variant": selected_variant}
            
            sql = duck_client.json_queries.search_text_in_column(
                table="catalog",
                column=selected_column,
                search_text=search_text,
                search_type="json",
                json_loc=json_path,
                partition_conditions=partition_conditions,
                engine="duckdb"
            )
            return duck_client.execute_query(sql)
        
        else:
            print("❌ 잘못된 선택입니다.")
            return None

    def _download_options(self, search_results):
        """다운로드 옵션 선택 및 실행"""
        print("\n💾 다운로드 옵션:")
        print("  1. 메타데이터만 (Parquet)")
        print("  2. 메타데이터만 (Arrow Dataset)")
        print("  3. 메타데이터 + 이미지 (Dataset format)")
        
        download_choice = input("다운로드 옵션 (1-3) [1]: ").strip() or "1"
        
        # 저장 경로 입력
        default_path = f"./downloads/export_{len(search_results)}_items"
        save_path = input(f"저장 경로 [{default_path}]: ").strip() or default_path
        save_path = Path(save_path)
        
        try:
            if download_choice == "1":
                # Parquet 저장
                parquet_path = save_path.with_suffix('.parquet')
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                search_results.to_parquet(parquet_path, index=False)
                print(f"✅ Parquet 저장 완료: {parquet_path}")
                print(f"📊 {len(search_results):,}개 항목, {parquet_path.stat().st_size / 1024 / 1024:.1f}MB")
                
            elif download_choice == "2":
                # Arrow Dataset 저장
                return self._save_as_dataset(search_results, save_path, include_images=False)
                
            elif download_choice == "3":
                # Dataset + 이미지 저장
                return self._save_as_dataset(search_results, save_path, include_images=True)
                
            else:
                print("❌ 잘못된 선택입니다.")
                return False
                
            return True
            
        except Exception as e:
            print(f"❌ 저장 중 오류: {e}")
            return False
        
    def _save_as_dataset(self, search_results, save_path, include_images=False):
        """datasets 라이브러리를 사용하여 Dataset 형태로 저장"""
        try:
            save_path = Path(save_path)
            save_path.mkdir(parents=True, exist_ok=True)
            
            if include_images:
                print(f"\n📥 이미지 포함 Dataset 생성 중...")
                path_column = 'path'

                if path_column not in search_results.columns:
                    print("❌ 이미지 경로 컬럼을 찾을 수 없습니다.")
                    return False
                            
                def load_and_validate_image(example):
                    """이미지 로드 및 유효성 검사"""
                    try:
                        if example[path_column] and pd.notna(example[path_column]):
                            image_path = self.data_manager.assets_path / example[path_column]
                            if image_path.exists():
                                pil_image = Image.open(image_path)
                                pil_image.verify()  # 손상된 이미지 체크
                                pil_image = Image.open(image_path) 
                                
                                example['image'] = pil_image
                                example['has_valid_image'] = True
                                return example
                            else:
                                print(f"⚠️ 파일 없음: {example[path_column]}")
                        else:
                            print(f"⚠️ 경로 없음: {example.get('hash', 'unknown')}")
                    except Exception as e:
                        print(f"⚠️ 이미지 로드 실패: {example.get(path_column, 'unknown')} - {e}")
                    
                    # 실패한 경우
                    example['image'] = None
                    example['has_valid_image'] = False
                    return example

                # DataFrame을 Dataset으로 변환
                dataset = Dataset.from_pandas(search_results)

                # 이미지 로드 (배치 단위로 처리)
                print("🖼️ 이미지 검증 및 로딩 중...")
                dataset_with_validation = dataset.map(
                    load_and_validate_image,
                    desc="이미지 검증",
                    num_proc=self.data_manager.num_proc,
                )

                # 유효한 이미지만 필터링
                print("🔍 유효한 이미지만 필터링 중...")
                valid_dataset = dataset_with_validation.filter(
                    lambda x: x,
                    desc="유효 이미지 필터링",
                    input_columns=['has_valid_image'],
                    num_proc=self.data_manager.num_proc
                )

                # 불필요한 컬럼 제거
                valid_dataset = valid_dataset.remove_columns(['has_valid_image'])

                # 결과 출력
                total_items = len(dataset)
                valid_images = len(valid_dataset)
                filtered_out = total_items - valid_images

                print(f"📊 이미지 로딩 결과:")
                print(f"  🔢 총 항목: {total_items:,}개")
                print(f"  ✅ 유효 이미지: {valid_images:,}개")
                print(f"  ❌ 제외된 항목: {filtered_out:,}개")

                if filtered_out > 0:
                    success_rate = (valid_images / total_items) * 100
                    print(f"  📈 성공률: {success_rate:.1f}%")
                    
                    # 사용자에게 확인
                    if filtered_out > total_items * 0.1:  # 10% 이상 실패
                        print(f"⚠️ 주의: {filtered_out}개 항목이 제외되었습니다.")
                        continue_choice = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
                        if continue_choice not in ['y', 'yes']:
                            return False

                print(f"\n💾 Dataset 저장 중...")
                valid_dataset.save_to_disk(str(save_path))

                print(f"✅ Dataset 저장 완료: {save_path}")
                print(f"📊 최종 {valid_images:,}개 항목 (이미지 포함)")
                print(f"💾 총 크기: {sum(f.stat().st_size for f in save_path.rglob('*') if f.is_file()) / 1024 / 1024:.1f}MB")

                # 사용법 안내
                print(f"\n💡 사용법:")
                print(f"```python")
                print(f"from datasets import load_from_disk")
                print(f"dataset = load_from_disk('{save_path}')")
                print(f"# 모든 항목에 유효한 이미지가 있습니다")
                print(f"# dataset[0]['image'].show()")
                print(f"```")
                
            else:
                # 메타데이터만 Dataset으로 저장
                print(f"\n📄 메타데이터 Dataset 생성 중...")
                
                # DataFrame을 Dataset으로 변환
                dataset = Dataset.from_pandas(search_results)
                
                # Dataset 저장
                dataset.save_to_disk(str(save_path))
                
                print(f"✅ Dataset 저장 완료: {save_path}")
                print(f"📊 {len(dataset):,}개 항목")
                print(f"💾 크기: {sum(f.stat().st_size for f in save_path.rglob('*') if f.is_file()) / 1024 / 1024:.1f}MB")
                
                # 사용법 안내
                print(f"\n💡 사용법:")
                print(f"```python")
                print(f"from datasets import load_from_disk")
                print(f"dataset = load_from_disk('{save_path}')")
                print(f"df = dataset.to_pandas()  # pandas로 변환")
                print(f"```")
            
            return True
            
        except ImportError:
            print("❌ datasets 라이브러리가 설치되지 않았습니다.")
            print("💡 설치 명령: pip install datasets")
            return False
        except Exception as e:
            print(f"❌ Dataset 저장 실패: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(
        description="📊 Data Manager CLI - 데이터 업로드/처리/다운로드 관리",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
📋 사용 가능한 명령어:

🔧 설정 관리:
  python cli.py config                         # 설정 도움말
  python cli.py config list                    # 전체 설정 확인
  python cli.py config provider               # Provider 관리 도움말
  python cli.py config provider create        # Provider 생성
  python cli.py config provider list          # Provider 목록
  python cli.py config provider remove        # Provider 제거
  python cli.py config task                   # Task 관리 도움말
  python cli.py config task create            # Task 생성
  python cli.py config task list              # Task 목록
  python cli.py config task remove            # Task 제거

📥 데이터 관리:
  python cli.py upload                         # 데이터 업로드
  python cli.py download                       # 데이터 다운로드

  다운로드 포맷:
    1. Parquet (메타데이터만)
    2. Arrow Dataset (메타데이터만) 
    3. Dataset + 이미지 (HuggingFace datasets 형태)
    
🔄 처리 관리:
  python cli.py process                        # 처리 시작 
  python cli.py process start                  # 새 처리 시작
  python cli.py process status JOB_ID          # 작업 상태 확인
  python cli.py process list                   # 내 데이터 현황

📊 Catalog DB 관리:
  python cli.py catalog info                   # Catalog DB 정보 확인
  python cli.py catalog rebuild                # Catalog DB 강제 재구축
  python cli.py catalog update                 # Catalog DB 업데이트 
  
🔍 데이터 무결성 검사:
  python cli.py validate                       # 전체 데이터 무결성 검사
  python cli.py validate --provider=NAME       # 특정 Provider만 검사
  python cli.py validate --fix                 # 문제 자동 수정
  python cli.py validate --report              # 상세 보고서 생성

💡 팁: Dataset 형태로 저장하면 datasets 라이브러리로 쉽게 로드할 수 있습니다.
     from datasets import load_from_disk
     dataset = load_from_disk('./downloads/my_dataset')
        """
    )
    parser.add_argument("--base-path", default="/mnt/AI_NAS/datalake",
                       help="데이터 저장 기본 경로")
    parser.add_argument("--nas-url", default="http://192.168.20.62:8091", 
                       help="NAS API 서버 URL")
    parser.add_argument("--log-level", default="INFO",
                       help="로깅 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    parser.add_argument("--num-proc", type=int, default=8,
                       help="병렬 처리 프로세스 수")
    
    subparsers = parser.add_subparsers(dest='command', help='명령어')
    
    
    # Config 관리 (Provider + Task)
    config_parser = subparsers.add_parser('config', help='설정 관리 (Provider, Task)')
    config_subparsers = config_parser.add_subparsers(dest='config_type')
    
    # Provider 관리
    provider_parser = config_subparsers.add_parser('provider', help='Provider 관리')
    provider_subparsers = provider_parser.add_subparsers(dest='provider_action')
    provider_subparsers.add_parser('create', help='새 Provider 생성')
    provider_subparsers.add_parser('remove', help='Provider 제거')
    provider_subparsers.add_parser('list', help='Provider 목록')
    
    # Task 관리
    task_parser = config_subparsers.add_parser('task', help='Task 관리')
    task_subparsers = task_parser.add_subparsers(dest='task_action')
    task_subparsers.add_parser('create', help='새 Task 생성')
    task_subparsers.add_parser('remove', help='Task 제거')
    task_subparsers.add_parser('list', help='Task 목록')
    
    # Config 전체 목록
    config_subparsers.add_parser('list', help='전체 설정 목록')
    
    # 데이터 업로드
    subparsers.add_parser('upload', help='데이터 업로드')
    # 데이터 다운로드
    subparsers.add_parser('download', help='데이터 다운로드')
    
    
    # 처리 관리
    process_parser = subparsers.add_parser('process', help='데이터 처리 관리')
    process_subparsers = process_parser.add_subparsers(dest='process_action')
    process_subparsers.add_parser('start', help='새 처리 시작')
    process_subparsers.add_parser('list', help='내 데이터 전체 현황 확인')
    job_status_parser = process_subparsers.add_parser('status', help='특정 작업 상태 확인')
    job_status_parser.add_argument('job_id', help='작업 ID')
    
    # Catalog DB 관리
    catalog_parser = subparsers.add_parser('catalog', help='Catalog DB 관리')
    catalog_subparsers = catalog_parser.add_subparsers(dest='catalog_action')
    catalog_subparsers.add_parser('info', help='Catalog DB 정보 확인')
    catalog_subparsers.add_parser('rebuild', help='Catalog DB 강제 재구축')
    catalog_subparsers.add_parser('check', help='Catalog 빠른 상태 확인')
    catalog_subparsers.add_parser('update', help='Catalog DB 안전 업데이트')
    catalog_subparsers.add_parser('processes', help='DB 사용 프로세스 확인')
    
    # 데이터 무결성 검사
    validate_parser = subparsers.add_parser('validate', help='Catalog DB 상태 검사 및 문제 해결')
    validate_parser.add_argument('--provider', type=str, help='특정 Provider만 검사')
    validate_parser.add_argument('--fix', action='store_true', help='문제 자동 수정')
    validate_parser.add_argument('--report', action='store_true', help='검사 보고서 생성')
    validate_parser.add_argument('--generate-report', action='store_true', help='검사 후 보고서 생성')
    # 상태 확인
    
    args = parser.parse_args()
    if not args.command:
        print("\n🚀 Data Manager CLI에 오신 것을 환영합니다!")
        print("="*60)
        print("\n사용 가능한 주요 명령어:")
        print("  🔧 python cli.py config     - 설정 관리 (Provider, Task)")
        print("  📥 python cli.py upload     - 데이터 업로드")
        print("  📤 python cli.py download   - 데이터 다운로드")
        print("  🔄 python cli.py process    - 데이터 처리")
        print("  📊 python cli.py catalog    - Catalog DB 관리")
        print("  🔍 python cli.py validate   - 데이터 무결성 검사")
        
        print("\n🌟 처음 사용하시나요? 다음 순서로 시작해보세요:")
        print(" 1️⃣  python cli.py config provider create  # 데이터 제공자 생성")
        print(" 2️⃣  python cli.py config task create      # 작업 유형 정의")
        print(" 3️⃣  python cli.py upload                  # 데이터 업로드")
        print(" 4️⃣  python cli.py process                 # 데이터 처리 시작")
        
        print("\n 💡 데이터 다운로드는 'python cli.py download' 명령으로 가능합니다.")
        print(" 1️⃣  python cli.py catalog update         # Catalog DB 구축")
        print(" 2️⃣  python cli.py download                # 데이터 다운로드")
        print("      → 옵션 1: Parquet (메타데이터만)")
        print("      → 옵션 2: Arrow Dataset (메타데이터만)")  
        print("      → 옵션 3: Dataset + 이미지 (HuggingFace 형태)")

        print("\n🔍 데이터 관리 및 문제 해결:")
        print("  📊 python cli.py catalog check            # 빠른 상태 확인")
        print("  🔍 python cli.py validate                 # 데이터 무결성 검사")
        

        print("\n💡 각 명령어 뒤에 -h 또는 --help를 붙이면 상세 도움말을 볼 수 있습니다.")
        print("   예: python cli.py config -h")
        print("\n🔥 Dataset 형태로 저장하면 ML 작업에 바로 사용할 수 있어요!")
        print("   from datasets import load_from_disk")
        print("   dataset = load_from_disk('./downloads/my_dataset')")
        print("\n" + "="*60)
        return

    
    # CLI 인스턴스 생성
    cli = DataManagerCLI(
        base_path=args.base_path,
        nas_api_url=args.nas_url,
        log_level=args.log_level,
        num_proc=args.num_proc
    )
    
    try:
        if args.command == 'config':
            if not args.config_type:
                print("\n❓ config 하위 명령어를 선택해주세요:")
                print("  📋 python cli.py config list      - 전체 설정 확인")
                print("  🏢 python cli.py config provider  - Provider 관리")
                print("  📝 python cli.py config task      - Task 관리")
                print("\n💡 처음 사용하시나요? 다음 순서로 시작해보세요:")
                print(" 1️⃣  python cli.py config provider create  # Provider 생성")
                print(" 2️⃣  python cli.py config task create      # Task 생성")
                print(" 3️⃣  python cli.py upload                  # 데이터 업로드")
                print(" 4️⃣  python cli.py process                 # 처리 시작")
                return
                
            if args.config_type == 'provider':
                if not args.provider_action:
                    print("\n❓ provider 하위 명령어를 선택해주세요:")
                    print("  📋 python cli.py config provider list    - Provider 목록")
                    print("  ➕ python cli.py config provider create  - Provider 생성")
                    print("  🗑️  python cli.py config provider remove  - Provider 제거")
                    return
                    
                if args.provider_action == 'create':
                    cli.create_provider_interactive()
                elif args.provider_action == 'remove':
                    cli.remove_provider_interactive()
                elif args.provider_action == 'list':
                    providers = cli.schema_manager.get_all_providers()
                    print(f"\n🏢 등록된 Provider ({len(providers)}개):")
                    if providers:
                        for provider in providers:
                            print(f"  • {provider}")
                    else:
                        print("  📭 등록된 Provider가 없습니다.")
                        print("  💡 'python cli.py config provider create' 명령으로 Provider를 생성해주세요.")
            
            elif args.config_type == 'task':
                if not args.task_action:
                    print("\n❓ task 하위 명령어를 선택해주세요:")
                    print("  📋 python cli.py config task list    - Task 목록")
                    print("  ➕ python cli.py config task create  - Task 생성")
                    print("  🗑️  python cli.py config task remove  - Task 제거")
                    return
                    
                if args.task_action == 'create':
                    cli.create_task_interactive()
                elif args.task_action == 'remove':
                    cli.remove_task_interactive()
                elif args.task_action == 'list':
                    tasks = cli.schema_manager.get_all_tasks()
                    print(f"\n📝 등록된 Task ({len(tasks)}개):")
                    if tasks:
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
                    else:
                        print("  📭 등록된 Task가 없습니다.")
                        print("  💡 'python cli.py config task create' 명령으로 Task를 생성해주세요.")
            
            elif args.config_type == 'list':
                cli.schema_manager.show_schema_info()
        
        elif args.command == 'upload':
            cli.upload_data_interactive()
        elif args.command == 'download':
            cli.download_data_interactive()
        elif args.command == 'process':
            if not args.process_action:
                print("\n❓ process 하위 명령어를 선택해주세요:")
                print("  🚀 python cli.py process start           - 새 처리 시작")
                print("  🔍 python cli.py process status JOB_ID   - 작업 상태 확인")
                print("  📋 python cli.py process list            - 내 데이터 현황")
                return
                
            if args.process_action == 'start':
                cli.trigger_processing()
            elif args.process_action == 'status':
                cli.check_job_status(args.job_id)
            elif args.process_action == 'list':
                cli.list_all_data()
        
        elif args.command == 'catalog':
            if not args.catalog_action:
                print("\n❓ catalog 하위 명령어를 선택해주세요:")
                print("  📊 python cli.py catalog info     - Catalog DB 상세 정보")
                print("  🔍 python cli.py catalog check    - Catalog 빠른 상태 확인")
                print("  🔄 python cli.py catalog update   - Catalog DB 안전 업데이트")
                print("  🔍 python cli.py catalog processes - DB 사용 프로세스 확인")
                return
                
            if args.catalog_action == 'info':
                cli.show_catalog_db_info()
            elif args.catalog_action == 'check':  # 새로 추가
                cli.quick_catalog_check()
            elif args.catalog_action == 'update':  # 새로 추가
                cli.safe_update_catalog_db()
            elif args.catalog_action == 'processes':  # 새로 추가
                cli.check_db_processes() 
        elif args.command == 'validate':
            # 매개변수 확인 및 정리
            provider = getattr(args, 'provider', None)
            fix_issues = getattr(args, 'fix', False)
            generate_report = getattr(args, 'generate_report', False)
            
            if provider:
                print(f"🏢 검사 대상: Provider '{provider}'")
            if fix_issues:
                # 구현안됌
                print("🔧 문제 자동 수정 모드 활성화")
                raise NotImplementedError("문제 자동 수정 기능은 아직 구현되지 않았습니다.")
            if generate_report:
                print("📄 보고서 생성 모드 활성화")
            
            # 검사 실행
            success = cli.validate_data_integrity(
                provider=provider,
                generate_report=generate_report
            )
            
            if not success:
                print("\n❌ 검사 중 중요한 문제가 발견되었습니다.")
                print("💡 'python cli.py validate --fix' 명령으로 자동 수정을 시도할 수 있습니다.")
                return 1
            else:
                print("\n✅ 데이터 무결성 검사 완료!")
                if fix_issues:
                    print("🔧 발견된 문제들이 자동으로 수정되었습니다.")
                if generate_report:
                    print("📄 상세 보고서가 생성되었습니다.")
                return 0

            
    except KeyboardInterrupt:
        print("\n👋 작업이 중단되었습니다.")
        print("💡 언제든지 다시 시도할 수 있습니다.")
    except FileNotFoundError as e:
        print(f"❌ 파일을 찾을 수 없습니다: {e}")
        print("💡 파일 경로를 확인해주세요.")
    except ValueError as e:
        print(f"❌ 입력 값 오류: {e}")
        print("💡 입력 값을 다시 확인해주세요.")
    except ConnectionError as e:
        print(f"❌ 연결 오류: {e}")
        print("💡 NAS 서버 연결을 확인해주세요.")
    except CatalogNotFoundError as e:
        print(f"❌ {e}")
    except CatalogEmptyError as e:
        print(f"❌ {e}")
    except CatalogLockError as e:
        print(f"❌ {e}")
        print("💡 잠시 후 다시 시도해주세요.")
    except CatalogError as e:
        print(f"❌ {e}")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")


if __name__ == "__main__":
    main()