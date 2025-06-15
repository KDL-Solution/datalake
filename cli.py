import argparse
import sys
import json
from pathlib import Path
from typing import List, Dict, Optional
sys.path.append(str(Path(__file__).resolve().parent.parent))  # 상위 디렉토리 추가
from managers.datalake_client import DatalakeClient

class DataManagerCLI:
    """Data Manager CLI 인터페이스"""
    
    def __init__(
        self, 
        base_path: str = "/mnt/AI_NAS/datalake",
        nas_api_url: str = "http://localhost:8000",
        log_level: str = "INFO"
    ):
        self.data_manager = DatalakeClient(
            base_path=base_path,
            nas_api_url=nas_api_url,
            auto_process=False,  # CLI에서는 수동 제어
            log_level=log_level,
        )
        self.schema_manager = self.data_manager.schema_manager
        
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
    
    def show_status(self):
        """상태 정보 출력"""
        print("\n" + "="*60)
        print("📊 Data Manager Status")
        print("="*60)
        
        # Schema 정보
        self.schema_manager.show_schema_info()
        
        # NAS 상태
        self.data_manager.show_nas_dashboard()


def main():
    parser = argparse.ArgumentParser(
        description="📊 Data Manager CLI - 데이터 업로드 및 처리 관리",
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
  python cli.py upload                         # 데이터 업로드 (대화형)

🔄 처리 관리:
  python cli.py process                        # 처리 시작 (대화형)
  python cli.py process start                  # 새 처리 시작
  python cli.py process status JOB_ID          # 작업 상태 확인
  python cli.py process list                   # 내 데이터 현황

📊 상태 확인:
  python cli.py status                         # 전체 상태 대시보드

💡 팁: 각 명령어는 부분 입력 시 사용 가능한 하위 옵션을 안내합니다.
        """
    )
    parser.add_argument("--base-path", default="/mnt/AI_NAS/datalake/migrate_test",
                       help="데이터 저장 기본 경로")
    parser.add_argument("--nas-url", default="http://localhost:8000", 
                       help="NAS API 서버 URL")
    parser.add_argument("--log-level", default="INFO",
                       help="로깅 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    
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
    
    # 처리 관리
    process_parser = subparsers.add_parser('process', help='데이터 처리 관리')
    process_subparsers = process_parser.add_subparsers(dest='process_action')
    process_subparsers.add_parser('start', help='새 처리 시작')
    process_subparsers.add_parser('list', help='내 데이터 전체 현황 확인')
    job_status_parser = process_subparsers.add_parser('status', help='특정 작업 상태 확인')
    job_status_parser.add_argument('job_id', help='작업 ID')
    
    
    
    # 상태 확인
    subparsers.add_parser('status', help='전체 상태 확인')
    
    args = parser.parse_args()
    if not args.command:
        print("\n🚀 Data Manager CLI에 오신 것을 환영합니다!")
        print("="*60)
        print("\n사용 가능한 주요 명령어:")
        print("  🔧 python cli.py config     - 설정 관리 (Provider, Task)")
        print("  📥 python cli.py upload     - 데이터 업로드")
        print("  🔄 python cli.py process    - 데이터 처리")
        print("  📊 python cli.py status     - 상태 확인")
        
        print("\n🌟 처음 사용하시나요? 다음 순서로 시작해보세요:")
        print("  1️⃣ python cli.py config provider create  # 데이터 제공자 생성")
        print("  2️⃣ python cli.py config task create      # 작업 유형 정의")
        print("  3️⃣ python cli.py upload                  # 데이터 업로드")
        print("  4️⃣ python cli.py process                 # 데이터 처리 시작")
        
        print("\n💡 각 명령어 뒤에 -h 또는 --help를 붙이면 상세 도움말을 볼 수 있습니다.")
        print("   예: python cli.py config -h")
        print("\n" + "="*60)
        return
    
    # CLI 인스턴스 생성
    cli = DataManagerCLI(
        base_path=args.base_path,
        nas_api_url=args.nas_url,
        log_level=args.log_level,
    )
    
    try:
        if args.command == 'config':
            if not args.config_type:
                print("\n❓ config 하위 명령어를 선택해주세요:")
                print("  📋 python cli.py config list      - 전체 설정 확인")
                print("  🏢 python cli.py config provider  - Provider 관리")
                print("  📝 python cli.py config task      - Task 관리")
                print("\n💡 처음 사용하시나요? 다음 순서로 시작해보세요:")
                print("  1️⃣ python cli.py config provider create  # Provider 생성")
                print("  2️⃣ python cli.py config task create      # Task 생성")
                print("  3️⃣ python cli.py upload                  # 데이터 업로드")
                print("  4️⃣ python cli.py process                 # 처리 시작")
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
        
        elif args.command == 'status':
            cli.show_status()
            
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
    except Exception as e:
        print(f"❌ 예상하지 못한 오류가 발생했습니다: {e}")
        print("💡 문제가 지속되면 관리자에게 문의해주세요.")


if __name__ == "__main__":
    main()