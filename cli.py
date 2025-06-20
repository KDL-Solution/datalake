import argparse
import json
import shutil
import pandas as pd
import psutil
import random 

from datasets import Dataset
from PIL import Image
from pathlib import Path
from datetime import datetime

from managers.datalake_client import DatalakeClient  

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
        num_proc: int = 8,
    ):
        self.data_manager = DatalakeClient(
            base_path=base_path,
            nas_api_url=nas_api_url,
            log_level=log_level,
            num_proc=num_proc
        )
        self.schema_manager = self.data_manager.schema_manager
    
    def show_catalog_db_info(self):
        """Catalog DB 정보 표시"""
        print("\n📊 Catalog DB 정보")
        print("="*50)
        
        try:
            catalog_info = self.data_manager.get_catalog_info()
            
            if not catalog_info['exists']:
                print("❌ Catalog DB 파일이 없습니다.")
                print("💡 'python cli.py catalog update' 명령으로 생성할 수 있습니다.")
                return False
            
            # 기본 정보 출력
            print(f"📁 DB 파일: {catalog_info['path']}")
            print(f"💾 파일 크기: {catalog_info['size_mb']}MB")
            print(f"🕒 수정 시간: {catalog_info['modified_time']}")
            
            if catalog_info.get('is_outdated'):
                print("⚠️ DB가 최신 상태가 아닙니다.")
            
            # 테이블 정보
            if 'tables' in catalog_info:
                print(f"\n📋 테이블: {len(catalog_info['tables'])}개")
                for table in catalog_info['tables']:
                    print(f"  • {table}")
            
            # Catalog 상세 정보
            if 'total_rows' in catalog_info:
                print(f"\n📊 Catalog 테이블:")
                print(f"  📈 총 행 수: {catalog_info['total_rows']:,}개")
                print(f"  🏷️ 파티션: {catalog_info.get('partitions', 0)}개")
                
                # Provider별 통계
                if 'provider_stats' in catalog_info:
                    print(f"\n🏢 Provider별 파티션 수:")
                    for provider, count in list(catalog_info['provider_stats'].items())[:5]:
                        print(f"  • {provider}: {count}개")
                    
                    if len(catalog_info['provider_stats']) > 5:
                        print(f"  ... 외 {len(catalog_info['provider_stats']) - 5}개")
            
            return True
            
        except Exception as e:
            print(f"❌ DB 정보 조회 실패: {e}")
            return False
        
    def build_catalog_db_interactive(self):
        """대화형 Catalog DB 구축"""
        print("\n" + "="*50)
        print("🔨 Catalog DB 구축")
        print("="*50)
        
        try:
    
            catalog_info = self.data_manager.get_catalog_info()
            force_rebuild = False
            
            if catalog_info['exists']:
                print("⚠️ 기존 Catalog DB가 있습니다.")
                print(f"  📁 파일: {catalog_info['path']}")
                print(f"  💾 크기: {catalog_info['size_mb']}MB")
                print(f"  📊 행 수: {catalog_info.get('total_rows', 'N/A'):,}개")

                choice = self._ask_yes_no(
                    question="\n기존 DB를 삭제하고 재구축하시겠습니까?",
                    default=False,
                )
                if choice:
                    force_rebuild = True
                else:  
                    print("❌ 구축이 취소되었습니다.")
                    return False
            
            # DB 구축 실행
            print("\n🔄 Catalog DB 구축 중...")
            success = self.data_manager.build_catalog_db(force_rebuild=force_rebuild)
            
            if success:
                print("✅ Catalog DB 구축 완료!")
                # 결과 확인
                new_info = self.data_manager.get_catalog_info()
                if new_info['exists']:
                    print(f"📊 총 {new_info.get('total_rows', 0):,}개 행 생성됨")
                return True
            else:
                print("❌ Catalog DB 구축 실패")
                return False
                
        except Exception as e:
            print(f"❌ 구축 중 오류: {e}")
            return False
        
    def quick_catalog_check(self):
        """빠른 카탈로그 상태 확인"""
        print("\n📊 Catalog 빠른 상태 확인")
        print("="*40)
        
        try:
            catalog_info = self.data_manager.get_catalog_info()
            
            if not catalog_info['exists']:
                print("❌ DB 파일 없음")
                return False
            
            print(f"📁 DB: {catalog_info['size_mb']}MB ({catalog_info['modified_time']})")
            
            if catalog_info.get('is_outdated'):
                print("⚠️ DB 업데이트 필요")
            else:
                print("✅ DB 최신 상태")
            
            if 'total_rows' in catalog_info:
                print(f"📊 총 {catalog_info['total_rows']:,}개 행")
            
            return True
            
        except Exception as e:
            print(f"❌ 상태 확인 실패: {e}")
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
                
                meta = {}
                if required_fields:
                    print(f"\n📝 필수 필드 입력:")
                    for field in required_fields:
                        if field in allowed_values:
                            print(f"  {field} 허용값: {allowed_values[field]}")
                        value = input(f"  {field}: ").strip()
                        if not value:
                            print(f"❌ 필수 필드 '{field}'가 누락되었습니다.")
                            return False
                        meta[field] = value
                
                # 검증
                is_valid, error_msg = self.data_manager.schema_manager.validate_task_metadata(task, meta)
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
                print(f"  📋 메타데이터: {meta}")
                
                confirm = input("\n업로드하시겠습니까? (y/N): ").strip().lower()
                if confirm in ['y', 'yes']:
                    staging_dir, job_id = self.data_manager.upload_task_data(
                        data_file=data_file,
                        provider=provider,
                        dataset=dataset,
                        task=task,
                        variant=variant,
                        meta=meta
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
        """대화형 데이터 다운로드 (간소화 버전)"""
        print("\n" + "="*50)
        print("📥 데이터 다운로드")
        print("="*50)
        
        try:
            # 1. 파티션 정보 조회
            print("🔄 사용 가능한 데이터 조회 중...")
            partitions_df = self.data_manager.get_catalog_partitions()
            
            if partitions_df.empty:
                print("❌ 사용 가능한 데이터가 없습니다.")
                print("💡 'python cli.py catalog update' 명령으로 Catalog를 먼저 구축해주세요.")
                return False
                
            print(f"📊 {len(partitions_df)}개 파티션 사용 가능")
            
            # 2. 검색 수행
            search_results = self._search_interactive(partitions_df)
            
            if search_results is None or search_results.empty:
                print("❌ 검색 결과가 없습니다.")
                return False
                
            print(f"\n📊 검색 결과: {len(search_results):,}개 항목")
            print("\n📋 결과 미리보기:")
            print(search_results.head(10))
            
            # 3. 다운로드 실행
            return self._download_selected_data(search_results)
            
        except FileNotFoundError as e:
            print(f"❌ {e}")
            print("💡 'python cli.py catalog update' 명령으로 Catalog DB를 생성해주세요.")
            return False
        except Exception as e:
            print(f"❌ 다운로드 중 오류: {e}")
            return False

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

    def check_db_processes(self):
        """DB 사용 중인 프로세스 확인 (UI)"""
        print("\n🔍 DB 사용 중인 프로세스 확인")
        print("="*50)
        
        try:
            result = self.data_manager.check_db_processes()
            
            if 'error' in result:
                print(f"❌ {result['error']}")
                return
            
            processes = result.get('processes', [])
            db_info = result.get('db_info', {})
            
            if processes:
                print(f"⚠️ {len(processes)}개 프로세스가 DB를 사용 중:")
                for proc in processes:
                    print(f"  PID {proc['pid']}: {proc['name']} (감지: {proc['match_type']})")
                    print(f"    명령어: {proc['cmdline']}")
                
                print(f"\n💡 종료 방법:")
                print(f"  - Jupyter 노트북: 커널 재시작")
                print(f"  - Python 스크립트: Ctrl+C로 종료")
                print(f"  - 강제 종료: kill -9 <PID>")
                
                # lsof 추가 확인
                print(f"\n🔍 lsof로 추가 확인:")
                try:
                    import subprocess
                    result = subprocess.run(['lsof', db_info['path']], 
                                        capture_output=True, text=True, timeout=5)
                    if result.stdout:
                        print(result.stdout)
                    else:
                        print("  lsof에서 추가 프로세스 발견되지 않음")
                except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
                    print("  lsof 명령어 사용 불가")
                    
            else:
                print("✅ DB를 사용 중인 프로세스가 없습니다.")
                
                # DB 파일 상태 출력
                print(f"\n🔍 DB 파일 상태:")
                print(f"  경로: {db_info['path']}")
                print(f"  존재: {db_info['exists']}")
                
                if db_info['exists']:
                    print(f"  크기: {db_info['size']:,} bytes")
                    print(f"  수정시간: {db_info['modified_time']}")
                    
                    if db_info.get('has_wal'):
                        print(f"  ⚠️ WAL 파일 존재 (비정상 종료 가능성)")
                        
        except Exception as e:
            print(f"❌ 프로세스 확인 실패: {e}")
            
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

    def validate_data_integrity_interactive(self, report=False):
        """대화형 데이터 무결성 검사"""
        print("\n" + "="*50)
        print("🔍 데이터 무결성 검사")
        print("="*50)
        
        try:
            # 검사 범위 선택
            print("🔍 검사 범위 선택:")
            print("  1. 조건별 검사 (Provider/Dataset/Task 선택)")
            print("  2. 전체 데이터 검사")
            
            while True:
                scope_choice = input("검사 범위 (1-2) [1]: ").strip() or "1"
                
                if scope_choice in ["1", "2"]:
                    break
                else:
                    print("❌ 잘못된 선택입니다. 1 또는 2를 입력해주세요.")
            
            search_results = None
            try:
                if scope_choice == "1":
                    
                    print("\n🔄 사용 가능한 데이터 조회 중...")
                    partitions_df = self.data_manager.get_catalog_partitions()
                    
                    if partitions_df.empty:
                        print("❌ 사용 가능한 데이터가 없습니다.")
                        return False
                        
                    print(f"📊 {len(partitions_df)}개 파티션 사용 가능")
                    
                    # 검색 수행 (텍스트 검색 제외, 파티션 기반만)
                    search_results = self._partition_search_interactive(partitions_df)
                    
                    if search_results is None or search_results.empty:
                        print("❌ 검색 결과가 없습니다.")
                        return False  # 🔥 여기서 바로 종료
                        
                    print(f"\n📊 검사 대상: {len(search_results):,}개 항목")
                    
                elif scope_choice == "2":
                    print("\n🔄 전체 데이터 조회 중...")
                    search_results = self.data_manager.search_catalog()  # 전체 검색
                    
                    if search_results is None or search_results.empty:
                        print("❌ 검사할 데이터가 없습니다.")
                        return False
                        
                    print(f"\n📊 전체 데이터: {len(search_results):,}개 항목")
                    
            except Exception as e:
                print(f"❌ 검색 실패: {e}")
                return False  # 🔥 예외 발생 시에도 바로 종
            
            sample_check = self._ask_yes_no(
                question="샘플 데이터만 검사하시겠습니까?",
                default=False,
            )
            sample_percent = None
            if sample_check:
                while True:
                    sample_input = input("샘플 비율 입력 (0.1 = 10%) [0.1]: ").strip() or "0.1"
                    try:
                        sample_percent = float(sample_input)
                        if 0 < sample_percent <= 1:
                            break
                        else:
                            print("❌ 비율은 0과 1 사이의 값이어야 합니다. 다시 입력해주세요.")
                    except ValueError:
                        print("❌ 숫자를 입력해주세요. (예: 0.1, 0.05)")
        
            # 검사 실행
            print("\n🔄 무결성 검사 시작...")
            result = self.data_manager.validate_data_integrity(
                search_results=search_results, 
                sample_percent=sample_percent
            )
            
            # 결과 출력
            print("\n" + "="*50)
            print("📋 검사 결과 요약")
            print("="*50)
            
            if 'errors' in result and result['errors']:
                print("❌ 검사 중 오류 발생:")
                for error in result['errors']:
                    print(f"  • {error}")
                return False
            
            total_items = result.get('total_items', 0)
            checked_items = result.get('checked_items', 0)
            missing_count = result.get('missing_count', 0)
            integrity_rate = result.get('integrity_rate', 0)
            
            if total_items == 0:
                print("❌ 검사할 데이터가 없습니다.")
                return False
            
            print(f"📊 검사 통계:")
            print(f"  • 총 항목: {total_items:,}개")
            print(f"  • 검사 항목: {checked_items:,}개")
            print(f"  • 누락 파일: {missing_count:,}개")
            print(f"  • 무결성 비율: {integrity_rate:.1f}%")
            
            if missing_count == 0:
                print("\n✅ 모든 검사 통과! 데이터가 정상입니다.")
                return True
            else:
                print(f"\n⚠️ {missing_count}개 파일이 누락되었습니다.")
                
                # 샘플 표시
                missing_files = result.get('missing_files', [])
                if missing_files:
                    print(f"\n📁 누락된 파일 (상위 3개):")
                    for item in missing_files[:3]:
                        print(f"  • {item.get('hash', 'unknown')[:16]}... ({item.get('provider', 'unknown')}/{item.get('dataset', 'unknown')})")
                
                # 보고서 생성
                if report:
                    report_path = self._generate_validation_report(result)
                    print(f"📄 상세 보고서: {report_path}")
                
                return missing_count == 0
                
        except Exception as e:
            print(f"❌ 검사 중 오류: {e}")
            return False

    def _generate_validation_report(self, result):
        """검사 보고서 생성"""
        report_path = Path(f"./validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        try:
            report_data = {
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_items': result.get('total_items', 0),
                    'checked_items': result.get('checked_items', 0),
                    'missing_count': result.get('missing_count', 0),
                    'integrity_rate': result.get('integrity_rate', 0)
                },
                'missing_files': result.get('missing_files', [])
            }
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            
            return report_path
            
        except Exception as e:
            print(f"❌ 보고서 생성 실패: {e}")
            return None
        
    def _ask_yes_no(self, question, default=False):
        """y/N 질문 함수"""
        full_question = f"{question} (y/N): " if not default else f"{question} (Y/n): "
        while True:
            answer = input(full_question).strip().lower()
            
            if not answer:  # Enter만 누른 경우
                return default.lower() in ['y', 'yes']
            
            if answer in ['y', 'yes']:
                return True
            elif answer in ['n', 'no']:
                return False
            else:
                print("❌ y/yes 또는 n/no를 입력해주세요.")     
        
    def _search_interactive(self, partitions_df):
        """대화형 검색 수행"""
        print("\n🔍 검색 방법 선택:")
        print("  1. 파티션 기반 검색 (Provider/Dataset/Task/Variant)")
        print("  2. 텍스트 검색 (JSON 내용 검색)")
        
        while True:
            choice = input("검색 방법 (1-2) [1]: ").strip() or "1"
            
            if choice == "1":
                return self._partition_search_interactive(partitions_df)
            elif choice == "2":
                return self._text_search_interactive()
            else:
                print("❌ 잘못된 선택입니다. 1 또는 2를 입력해주세요.")
    
    def _partition_search_interactive(self, partitions_df):
        """파티션 기반 대화형 검색"""
        # Provider 선택
        providers = self._select_items_interactive(
            partitions_df['provider'].unique().tolist(),
            "Provider"
        )
        if not providers:
            return None
        
        # Dataset 선택
        filtered_df = partitions_df[partitions_df['provider'].isin(providers)]
        datasets = self._select_items_interactive(
            filtered_df['dataset'].unique().tolist(),
            "Dataset"
        )
        if not datasets:
            return None
        
        # Task 선택
        filtered_df = filtered_df[filtered_df['dataset'].isin(datasets)]
        tasks = self._select_items_interactive(
            filtered_df['task'].unique().tolist(),
            "Task"
        )
        if not tasks:
            return None
        
        # Variant 선택
        filtered_df = filtered_df[filtered_df['task'].isin(tasks)]
        variants = self._select_items_interactive(
            filtered_df['variant'].unique().tolist(),
            "Variant"
        )
        if not variants:
            return None
        
        # 검색 실행
        print(f"\n🔍 검색 실행 중...")
        return self.data_manager.search_catalog(
            providers=providers,
            datasets=datasets,
            tasks=tasks,
            variants=variants
        )

    def _select_items_interactive(self, items, name):
        """아이템 대화형 선택"""
        if not items:
            print(f"❌ 사용 가능한 {name}가 없습니다.")
            return None
        
        items = sorted(items)
        print(f"\n{name} 선택 ({len(items)}개):")
        for i, item in enumerate(items, 1):
            print(f"  {i:2d}. {item}")
        
        print("\n선택: 번호(1,2,3), 범위(1-5), 전체(Enter)")
        while True:  # 🔥 올바른 입력까지 반복
            
            user_input = input(f"{name}: ").strip()
            
            if not user_input: 
                return items
            
            selected = self._parse_input(user_input, items)
            if selected is not None:  # 🔥 올바른 선택이면 반환
                return selected
            
            # 🔥 잘못된 입력이면 다시 시도
            print("❌ 잘못된 입력입니다. 다시 선택해주세요.")

    def _parse_input(self, user_input, items):
        """입력 파싱"""
        selected = set()
        parts = user_input.split(',')
        has_error = False  # 🔥 오류 플래그 추가
        
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
                    else:
                        print(f"⚠️ 잘못된 범위: {part}")
                        has_error = True
                except ValueError:
                    print(f"⚠️ 잘못된 범위: {part}")
                    has_error = True
                    
            elif part.isdigit():
                # 번호: 1, 2, 3
                idx = int(part) - 1
                if 0 <= idx < len(items):
                    selected.add(items[idx])
                else:
                    print(f"⚠️ 잘못된 번호: {part}")
                    has_error = True
                    
            else:
                # 이름: imagenet, coco
                if part in items:
                    selected.add(part)
                else:
                    print(f"⚠️ 찾을 수 없음: {part}")
                    has_error = True
        
        if has_error or not selected:  # 🔥 오류가 있거나 선택된 게 없으면 None 반환
            return None
        
        return list(selected)

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

    def _text_search_interactive(self):
        """텍스트 기반 대화형 검색"""
        search_text = input("\n검색할 텍스트: ").strip()
        if not search_text:
            print("❌ 검색 텍스트가 필요합니다.")
            return None
        
        # 컬럼 선택 (간단하게)
        print("\n주요 검색 컬럼:")
        print("  1. labels (라벨 정보)")
        print("  2. metadata (메타데이터)")
        
        # col_choice = input("컬럼 선택 (1-2) [1]: ").strip() or "1"
        # column = "labels" if col_choice == "1" else "metadata"
        while True:
            col_choice = input("컬럼 선택 (1-2) [1]: ").strip() or "1"
            if col_choice in ["1", "2"]:
                column = "labels" if col_choice == "1" else "metadata"
                break
            else:
                print("❌ 잘못된 선택입니다. 1 또는 2를 입력해주세요.")
        
        # JSON 경로 입력
        json_path = input("JSON 경로 (예: $.image.text.content, 생략 가능): ").strip()
        
        text_search_config = {
            "column": column,
            "text": search_text
        }
        
        if json_path:
            text_search_config["json_path"] = json_path
        
        # 검색 실행
        return self.data_manager.search_catalog(text_search=text_search_config)

    def _download_selected_data(self, search_results):
        """대화형 다운로드 수행"""
        print("\n💾 다운로드 옵션:")
        print("  1. Parquet 파일 (pandas 호환)")
        print("  2. Dataset 폴더 (HuggingFace 호환)")
        print("  3. Dataset + 이미지 로딩 (즉시 사용 가능)")
        
        while True:
            choice = input("다운로드 옵션 (1-3) [1]: ").strip() or "1"
            
            if choice in ["1", "2", "3"]:
                break
            else:
                print("❌ 잘못된 선택입니다. 1, 2, 또는 3을 입력해주세요.")
        
        default_path = f"./downloads/export_{len(search_results)}_items"
        save_path = input(f"저장 경로 [{default_path}]: ").strip() or default_path
        
        try:
            if choice == "1":
                output_path = self.data_manager.download_as_parquet(search_results, save_path, absolute_paths=True)
                print(f"✅ Parquet 저장 완료: {output_path}")
                
            elif choice == "2":
                output_path = self.data_manager.download_as_dataset(
                    search_results, save_path, include_images=False, absolute_paths=True,
                )
                print(f"✅ Dataset 저장 완료: {output_path}")
                self._show_usage_example(output_path)
                
            elif choice == "3":
                output_path = self.data_manager.download_as_dataset(
                    search_results, save_path, include_images=True, absolute_paths=True,
                )
                print(f"✅ Dataset + 이미지 저장 완료: {output_path}")
                self._show_usage_example(output_path, with_images=True)
            
            return True
            
        except Exception as e:
            print(f"❌ 저장 실패: {e}")
            return False
        
    def _show_usage_example(self, output_path, with_images=False):
        """사용법 예제 표시"""
        print(f"\n💡 사용법:")
        print(f"```python")
        print(f"from datasets import load_from_disk")
        print(f"dataset = load_from_disk('{output_path}')")
        if with_images:
            print(f"# 이미지 확인")
            print(f"dataset[0]['image'].show()")
        else:
            print(f"# pandas로 변환")
            print(f"df = dataset.to_pandas()")
        print(f"```")
        
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
python cli.py config task                   # Task 관리 도움말

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
python cli.py catalog update                 # Catalog DB 업데이트 
python cli.py catalog check                  # Catalog 빠른 상태 확인
python cli.py catalog processes              # DB 사용 프로세스 확인

🔍 데이터 무결성 검사:
python cli.py validate                       # 데이터 무결성 검사
python cli.py validate --report              # 검사 보고서 생성

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
    catalog_subparsers.add_parser('check', help='Catalog 빠른 상태 확인')
    catalog_subparsers.add_parser('update', help='Catalog DB 안전 업데이트')
    catalog_subparsers.add_parser('processes', help='DB 사용 프로세스 확인')
    
    # 데이터 무결성 검사
    validate_parser = subparsers.add_parser('validate', help='Catalog DB 상태 검사 및 문제 해결')
    validate_parser.add_argument('--report', action='store_true', help='검사 보고서 생성')
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
                cli.build_catalog_db_interactive()
            elif args.catalog_action == 'processes':  # 새로 추가
                cli.check_db_processes() 
        elif args.command == 'validate':
            # 매개변수 확인 및 정리
            report = getattr(args, 'report', False)
            if report:
                print("📄 보고서 생성 모드 활성화")
            
            # 검사 실행
            success = cli.validate_data_integrity_interactive(report=report)
            
            if not success:
                return 1
            else:
                print("\n✅ 데이터 무결성 검사 완료!")
                if report:
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