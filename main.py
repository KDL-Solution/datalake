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

from core.datalake import DatalakeClient  

class DataManagerCLI:
    """Data Manager CLI 인터페이스"""
    
    def __init__(
        self, 
        user_id: str = "user",
        base_path: str = "/mnt/AI_NAS/datalake",
        server_url: str = "http://192.168.20.62:8091",
        log_level: str = "INFO",
        num_proc: int = 8,
    ):
        self.data_manager = DatalakeClient(
            user_id=user_id,
            base_path=base_path,
            server_url=server_url,
            log_level=log_level,
            num_proc=num_proc
        )
        self.schema_manager = self.data_manager.schema_manager
    
    def show_db_info(self):
        """DB 정보 표시 및 상태 확인"""
        print("\n📊 DB 정보")
        print("="*50)
        
        try:
            db_info = self.data_manager.get_db_info()
            
            if not db_info['exists']:
                print("❌ DB 파일이 없습니다.")
                print("💡 'python main.py db update' 명령으로 생성할 수 있습니다.")
                return False
            
            # 기본 정보
            print(f"📁 DB 파일: {db_info['path']}")
            print(f"💾 파일 크기: {db_info['size_mb']}MB")
            print(f"🕒 수정 시간: {db_info['modified_time']}")
            
            # 업데이트 상태 확인 및 제안
            if db_info.get('is_outdated'):
                print("⚠️ DB가 최신 상태가 아닙니다.")
                choice = self._ask_yes_no(
                    question="DB를 업데이트하시겠습니까?",
                    default=True,
                )
                if choice:
                    print("🔄 DB 업데이트 중...")
                    success = self.data_manager.build_db()
                    if success:
                        print("✅ DB 업데이트 완료")
                        # 업데이트 후 새 정보 가져오기
                        db_info = self.data_manager.get_db_info()
                    else:
                        print("❌ DB 업데이트 실패")
                        return False
            else:
                print("✅ DB 최신 상태")
            
            # 테이블 정보
            if 'tables' in db_info:
                print(f"\n📋 테이블: {len(db_info['tables'])}개")
                for table in db_info['tables']:
                    print(f"  • {table}")
            
            # 데이터 통계
            if 'total_rows' in db_info:
                print(f"\n📊 데이터 통계:")
                print(f"  📈 총 행 수: {db_info['total_rows']:,}개")
                print(f"  🏷️ 파티션: {db_info.get('partitions', 0)}개")
                
                # Provider별 통계 (상위 5개)
                if 'provider_stats' in db_info and db_info['provider_stats']:
                    print(f"\n🏢 Provider별 파티션:")
                    provider_items = list(db_info['provider_stats'].items())
                    for provider, count in provider_items[:5]:
                        print(f"  • {provider}: {count}개")
                    
                    if len(provider_items) > 5:
                        print(f"  ... 외 {len(provider_items) - 5}개")
                
                # Task별 통계
                if 'task_stats' in db_info and db_info['task_stats']:
                    print(f"\n📝 Task별 파티션:")
                    for task, count in db_info['task_stats'].items():
                        print(f"  • {task}: {count}개")
            
            return True
            
        except Exception as e:
            print(f"❌ DB 정보 조회 실패: {e}")
            return False
        
    def build_db_interactive(self):
        """대화형 DB 구축"""
        print("\n" + "="*50)
        print("🔨 DB 구축")
        print("="*50)
        
        try:
    
            db_info = self.data_manager.get_db_info()
            force_rebuild = False
            
            if db_info['exists']:
                print("⚠️ 기존 DB가 있습니다.")
                print(f"  📁 파일: {db_info['path']}")
                print(f"  💾 크기: {db_info['size_mb']}MB")
                print(f"  📊 행 수: {db_info.get('total_rows', 'N/A'):,}개")

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
            print("\n🔄 DB 구축 중...")
            success = self.data_manager.build_db(force_rebuild=force_rebuild)
            
            if success:
                print("✅ DB 구축 완료!")
                # 결과 확인
                new_info = self.data_manager.get_db_info()
                if new_info['exists']:
                    print(f"📊 총 {new_info.get('total_rows', 0):,}개 행 생성됨")
                return True
            else:
                print("❌ DB 구축 실패")
                return False
                
        except Exception as e:
            print(f"❌ 구축 중 오류: {e}")
            return False
    
    def create_provider(self, name: str=None):
        if name is None:
            while True:
                name = input("🏢 Provider 이름을 입력하세요: ").strip()
                if name:
                    break
                print("❌ Provider 이름은 필수입니다. 다시 입력해주세요.")
        try:
            if name in self.schema_manager.get_all_providers():
                print(f"Provider '{name}'가 이미 존재합니다.")
                return False
            
            description = input("📜 Provider 설명 (선택): ").strip()
                
            choice = self._ask_yes_no(
                question=f"Provider '{name}'를 생성하시겠습니까?",
                default=True,
            )
            if not choice:
                print("❌ 생성이 취소되었습니다.")
                return False
            
            result = self.schema_manager.add_provider(name, description)
            if result:
                print(f"✅ Provider '{name}' 생성 완료")
                return True
            else:
                print(f"❌ Provider 생성 실패")
                return False
        except KeyboardInterrupt:
            print("\n❌ 생성이 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Provider 생성 중 오류: {e}")
            return False

    def remove_provider(self, name: str):
        try:
            providers = self.schema_manager.get_all_providers()
            
            if name not in providers:
                print(f"❌ Provider '{name}'이 존재하지 않습니다.")
                if providers:
                    print(f"등록된 Provider: {', '.join(providers)}")
                return False
            choice = self._ask_yes_no(
                question=f"Provider '{name}'를 정말로 제거하시겠습니까?",
                default=False,
            )
            if not choice:
                print("❌ 제거가 취소되었습니다.")
                return False
            
            result = self.schema_manager.remove_provider(name)
            if result:
                print(f"✅ Provider '{name}' 제거 완료")
                return True
            else:
                print(f"❌ Provider '{name}' 제거 실패")
                return False
        except KeyboardInterrupt:
            print("\n❌ 제거가 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Provider 제거 중 오류: {e}")
            return False
       
    def list_providers(self):
        try:
            providers = self.schema_manager.get_all_providers()
            print(f"\n🏢 등록된 Provider ({len(providers)}개):")
            
            if providers:
                for provider in providers:
                    print(f"  • {provider}")
            else:
                print("  📭 등록된 Provider가 없습니다.")
                print("  💡 'python main.py config provider create' 명령으로 Provider를 생성해주세요.")
            
            return True
            
        except Exception as e:
            print(f"❌ Provider 목록 조회 중 오류: {e}")
            return False
         
    def create_task(self, name: str):
        try:
            if name in self.schema_manager.get_all_tasks():
                print(f"Task '{name}'가 이미 존재합니다.")
                return False
            
            description = input("📜 Task 설명 (선택): ").strip()
            
            print(f"\n🔧 Task '{name}' 설정")
            print("="*50)   
            print("\n📝 필수 필드 설정 (Enter로 완료)")
            required_fields = []
            while True:
                field = input(f"필수 필드 #{len(required_fields)+1}: ").strip()
                if not field:
                    break
                required_fields.append(field)
                print(f"  ✅ 추가됨: {field}")
            
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
            print(f"  이름: {name}")
            print(f"  설명: {description}")
            print(f"  필수 필드: {required_fields}")
            print(f"  허용 값: {allowed_values}")
            
            choice = self._ask_yes_no(
                question=f"Task '{name}'를 생성하시겠습니까?",
                default=True,
            )
            if not choice:
                print("❌ 생성이 취소되었습니다.")
                return False
            
            result = self.schema_manager.add_task(
                name=name, 
                description=description,
                required_fields=required_fields, 
                allowed_values=allowed_values
            )
            if result:
                print(f"✅ Task '{name}' 생성/업데이트 완료!")
                return True
            else:
                print(f"❌ Task 생성/업데이트 실패")
                return False
                
        except KeyboardInterrupt:
            print("\n❌ 생성이 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Task 생성 중 오류: {e}")
            return False

    def remove_task(self, name: str):
        try: 
            tasks = self.schema_manager.get_all_tasks()
            if name not in tasks:
                print(f"❌ Task '{name}'이 존재하지 않습니다.")
                if tasks:
                    print(f"등록된 Task: {', '.join(tasks.keys())}")
                return False

            choice = self._ask_yes_no(
                question=f"Task '{name}'를 정말로 제거하시겠습니까?",
                default=False,
            )
            if not choice:
                print("❌ 제거가 취소되었습니다.")
                return False
            
            if self.schema_manager.remove_task(name):
                print(f"✅ Task '{name}' 제거 완료")
                return True
            else:
                print(f"❌ Task '{name}' 제거 실패")
                return False
                
        except KeyboardInterrupt:
            print("\n❌ 제거가 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ Task 제거 중 오류: {e}")
            return False
        
    def list_tasks(self):
        try:
            tasks = self.schema_manager.get_all_tasks()
            print(f"\n📝 등록된 Task ({len(tasks)}개):")
            
            if tasks:
                for task in tasks:
                    print(f"  • {task}")
                    task_config = self.schema_manager.get_task_info(task)
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
                print("  💡 'python main.py config task create' 명령으로 Task를 생성해주세요.")
            
            return True
            
        except Exception as e:
            print(f"❌ Task 목록 조회 중 오류: {e}")
            return False
    
    def upload_interactive(self):
        try:
            data_file = self._get_file_path()
            if not data_file:
                return False
        
            data_type = self._select_data_type()
            
            # 4. 데이터 타입별 플로우
            if data_type == "raw":
                self._upload_raw_data(data_file)
                    
            elif data_type == "task":
                self._upload_task_data(data_file)
                
        except KeyboardInterrupt:
            print("\n❌ 업로드가 취소되었습니다.")
            return False
        except Exception as e:
            print(f"❌ 업로드 중 오류: {e}")
            return False
    
    def export_interactive(self):
        try:
            print("🔄 DB 데이터 조회 중...")
            partitions_df = self.data_manager.get_partitions()
            if partitions_df.empty:
                print("❌ 사용 가능한 데이터가 없습니다.")
                print("💡 'python main.py db update' 명령으로 DB를 먼저 구축해주세요.")
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
            return self._export_selected_data(search_results)
            
        except FileNotFoundError as e:
            print(f"❌ {e}")
            print("💡 'python main.py db update' 명령으로 DB를 생성해주세요.")
            return False
        except Exception as e:
            print(f"❌ 다운로드 중 오류: {e}")
            return False

    def trigger_processing(self):
        try:
            # 현재 상태 확인
            status = self.data_manager.get_server_status()
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
                print("⚠️ 서버 상태를 확인할 수 없습니다.")
                continue_anyway = input("그래도 처리를 시작하시겠습니까? (y/N): ").strip().lower()
                if continue_anyway not in ['y', 'yes']:
                    print("❌ 처리가 취소되었습니다.")
                    return False
            
            # 처리 시작
            job_id = self.data_manager.trigger_processing()
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
                        print(f"💡 'python main.py process status {job_id}' 명령으로 상태를 확인할 수 있습니다.")
                        return True
                    except Exception as e:
                        print(f"❌ 처리 대기 중 오류: {e}")
                        return False
                else:
                    print(f"🔄 백그라운드에서 처리 중입니다. Job ID: {job_id}")
                    print(f"💡 'python main.py process status {job_id}' 명령으로 상태를 확인할 수 있습니다.")
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
            jobs = self.data_manager.list_server_jobs() or []
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
                print("💡 'python main.py upload' 명령으로 데이터를 업로드해보세요.")
            else:
                if pending_items:
                    print(f"\n💡 'python main.py process' 명령으로 업로드된 데이터를 처리할 수 있습니다.")
                
            return True
            
        except Exception as e:
            print(f"❌ 데이터 현황 조회 중 오류: {e}")
            return False

    def validate_db_integrity_interactive(self, report=False):
        """대화형 데이터 무결성 검사"""
        print("\n" + "="*50)
        print("🔍 데이터 무결성 검사")
        print("="*50)
        if not report:
            print("💡 'report=True' 옵션을 사용하면 상세 보고서를 생성합니다.")
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
                    partitions_df = self.data_manager.get_partitions()                   
                                            
                    print(f"📊 {len(partitions_df)}개 파티션 사용 가능")
                    
                    # 검색 수행 (텍스트 검색 제외, 파티션 기반만)
                    search_results = self._partition_search_interactive(partitions_df)
                    
                    if search_results is None or search_results.empty:
                        raise ValueError("검색 결과가 없습니다. 조건을 다시 확인해주세요.")
                        
                    print(f"\n📊 검사 대상: {len(search_results):,}개 항목")
                    
                elif scope_choice == "2":
                    print("\n🔄 전체 데이터 조회 중...")
                    search_results = self.data_manager.search()  # 전체 검색
                    
                    if search_results is None or search_results.empty:
                        raise ValueError("전체 데이터가 비어있습니다. DB를 먼저 구축해주세요.")
                        
                    print(f"\n📊 전체 데이터: {len(search_results):,}개 항목")
                    
            except Exception as e:
                raise e
            
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
            result = self.data_manager.validate_db_integrity(
                search_results=search_results, 
                sample_percent=sample_percent
            )
            
            print("\n" + "="*50)
            print("📋 검사 결과 요약")
            print("="*50)

            total_items = result.get('total_items', 0)
            checked_items = result.get('checked_items', 0)
            missing_count = result.get('missing_count', 0)
            integrity_rate = result.get('integrity_rate', 0)
            errors = result.get('errors', [])
            
            print(f"📊 검사 통계:")
            print(f"  • 총 항목: {total_items:,}개")
            print(f"  • 검사 항목: {checked_items:,}개")
            print(f"  • 누락 파일: {missing_count:,}개")
            print(f"  • 무결성 비율: {integrity_rate:.1f}%")
            # error
            print(f"  • 오류 발생: {len(errors)}개")
            
            
            if missing_count == 0:
                print("\n✅ 모든 검사 통과! 데이터가 정상입니다.")
            else:
                print(f"\n⚠️ {missing_count}개 파일이 누락되었습니다.")
                
                missing_files = result.get('missing_files', [])
                if missing_files:
                    print(f"\n📁 누락된 파일 (상위 3개):")
                    for item in missing_files[:3]:
                        print(f"  • {item.get('hash', 'unknown')[:16]}... ({item.get('provider', 'unknown')}/{item.get('dataset', 'unknown')})")
                
                # 보고서 생성
                if report:
                    report_path = self._generate_validation_report(result)
                    print(f"📄 상세 보고서: {report_path}")
                
                print("\n" + "="*50)
                print("💡 'python main.py db validate --report' 명령으로 상세 보고서를 생성할 수 있습니다.")
                
        except Exception as e:
            raise e

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
            
            if not answer:
                return default
            
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
            df=partitions_df,
            column="provider",
            level='task'
        )
        if not providers:
            return None
        
        # Dataset 선택
        filtered_df = partitions_df[partitions_df['provider'].isin(providers)]
        datasets = self._select_items_interactive(
            df=filtered_df,
            column="dataset",
            level='task'
        )
        if not datasets:
            return None
        
        # Task 선택
        filtered_df = filtered_df[filtered_df['dataset'].isin(datasets)]
        tasks = self._select_items_interactive(
            df=filtered_df,
            column="task",
            level='variant'
        )
        if not tasks:
            return None
        
        # Variant 선택
        filtered_df = filtered_df[filtered_df['task'].isin(tasks)]
        variants = self._select_items_interactive(
            df=filtered_df,
            column="variant",
            level="dataset",
        )
        if not variants:
            return None
        
        # 검색 실행
        print(f"\n🔍 검색 실행 중...")
        return self.data_manager.search(
            providers=providers,
            datasets=datasets,
            tasks=tasks,
            variants=variants
        )

    def _select_items_interactive(self, df=None, column=None, level=None):
        """아이템 대화형 선택"""
        items = df[column].unique().tolist()
        if not items:
            print(f"❌ 사용 가능한 {column}가 없습니다.")
            return None
        
        self._show_matrix(df, column, level)  # 🔥 파티션 매트릭스 표시
        items = sorted(items)
        print(f"\n{column} 선택 ({len(items)}개):")
        for i, item in enumerate(items, 1):
            print(f"  {i:2d}. {item}")
        
        print("\n선택: 번호(1,2,3), 범위(1-5), 전체(Enter)")
        while True:  # 🔥 올바른 입력까지 반복
            
            user_input = input(f"{column}: ").strip()
            
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
        col_width = max(len(level1.title()), 20)
        print(level1.title().ljust(col_width), end=" | ")
        for item2 in items2:
            print(item2[:15].ljust(15), end=" | ")
        print()
        
        # 구분선
        print("-" * (col_width + len(items2) * 20))
        
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
                    print(f"{count:>3}".ljust(15), end=" | ")
                else:
                    print(" - ".ljust(15), end=" | ")
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
        return self.data_manager.search(text_search=text_search_config)

    def _export_selected_data(self, search_results):
        """대화형 다운로드 수행"""
        print("\n💾 다운로드 옵션:")
        print("  1. Parquet (path)")
        print("  2. Dataset (path)")
        print("  3. Dataset (PIL)")
        
        while True:
            choice = input("다운로드 옵션 (1-3) [1]: ").strip() or "1"
            
            if choice in ["1", "2", "3"]:
                break
            else:
                print("❌ 잘못된 선택입니다. 1, 2, 또는 3을 입력해주세요.")
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        default_path = f"./exports/export_{timestamp}_{len(search_results)}_items"
        save_path = input(f"저장 경로 [{default_path}]: ").strip() or default_path
        
        try:
            if choice == "1":
                output_path = self.data_manager.export(
                    search_results=search_results, 
                    output_path=save_path, 
                    format='parquet',
                    absolute_paths=True
                )
                print(f"✅ Parquet 저장 완료: {output_path}")
                
            elif choice == "2":
                output_path = self.data_manager.export(
                    search_results=search_results, 
                    output_path=save_path, 
                    format="dataset",
                    include_images=False, 
                    absolute_paths=True,
                )
                print(f"✅ Dataset 저장 완료: {output_path}")
                self._show_usage_example(output_path)
                
            elif choice == "3":
                output_path = self.data_manager.export(
                    search_results=search_results, 
                    output_path=save_path, 
                    format="dataset",
                    include_images=True, 
                    absolute_paths=True,
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

    def _select_provider(self):
        providers = self.schema_manager.get_all_providers()
        if not providers:
            print("❌ 등록된 Provider가 없습니다. 먼저 Provider를 생성해주세요.")
            
        print(f"\n🏢 사용 가능한 Provider:")
        for i, provider in enumerate(providers, 1):
            print(f"  {i}. {provider}")
        
        while True:        
            provider_choice = input("Provider 번호 또는 이름 입력: ").strip()
            if provider_choice.isdigit():
                idx = int(provider_choice) - 1
                if 0 <= idx < len(providers):
                    return providers[idx]
                else:
                    print("❌ 잘못된 번호입니다.")
            else:
                if provider_choice in providers:
                    return provider_choice
                else:
                    print(f"❌ Provider '{provider_choice}'가 존재하지 않습니다.")
    
    def _select_task(self):
        tasks = self.schema_manager.get_all_tasks()
        if not tasks:
            print("❌ 등록된 Task가 없습니다. 먼저 Task를 생성해주세요.")

        print(f"\n🛠️ 사용 가능한 Task:")
        for i, task in enumerate(tasks, 1):
            print(f"  {i}. {task}")
        
        while True:        
            task_choice = input("Task 번호 또는 이름 입력: ").strip()
            if task_choice.isdigit():
                idx = int(task_choice) - 1
                if 0 <= idx < len(tasks):
                    return tasks[idx]
                else:
                    print("❌ 잘못된 번호입니다.")
            else:
                if task_choice in tasks:
                    return task_choice
                else:
                    print(f"❌ Task '{task_choice}'가 존재하지 않습니다.")
                    
    def _upload_raw_data(self, data_file):
        
        provider = self._select_provider()
        
        while True:
            dataset = input("\n📦 새 Dataset 이름: ").strip()
            if dataset:
                break
            print("❌ Dataset 이름이 필요합니다. 다시 입력해주세요.")
        
        description = input("📄 데이터셋 설명 (선택사항): ").strip()
        source = input("🔗 원본 소스 URL (선택사항): ").strip()
        self._show_upload_summary(
            data_file=data_file,
            data_type="raw",
            provider=provider,
            dataset=dataset,
            description=description,
            source=source
        )
        
        choice = self._ask_yes_no(
            question="업로드를 시작하시겠습니까?",
            default=True,
        )
        if not choice:
            print("❌ 업로드가 취소되었습니다.")
            return False
        try:
            staging_dir, job_id = self.data_manager.upload_raw(
                data_file=data_file,
                provider=provider,
                dataset=dataset,
                dataset_description=description,
                original_source=source
            )
            print(f"✅ 업로드 완료: {staging_dir}")
            print("💡 'python main.py process start' 명령으로 처리를 시작할 수 있습니다.")
            return True
        except Exception as e:
            print(f"❌ 업로드 실패: {e}")
            return False
        
    def _upload_task_data(self, data_file):
        partitions = self.data_manager.get_partitions()
        raw_partitions = partitions[partitions['task'] == 'raw']
        if raw_partitions.empty:
            print("❌ 업로드된 데이터가 없습니다.") 
            print("💡 먼저 raw 데이터를 업로드해주세요.")
            return False
        existing_providers = raw_partitions['provider'].unique().tolist()
        print(f"\n📂 업로드된 Provider ({len(existing_providers)}개):")
        for i, provider in enumerate(existing_providers, 1):
            print(f"  {i}. {provider}")
        while True:
            provider_choice = input("\n🏢 Provider 선택: ").strip()
            if provider_choice.isdigit():
                idx = int(provider_choice) - 1
                if 0 <= idx < len(existing_providers):
                    provider = existing_providers[idx]
                    break
                else:
                    print("❌ 잘못된 번호입니다.")
            else:
                if provider_choice in existing_providers:
                    provider = provider_choice
                    break
                else:
                    print(f"❌ Provider '{provider_choice}'가 존재하지 않습니다.")
                    
        existing_datasets = raw_partitions[raw_partitions['provider'] == provider]['dataset'].unique().tolist()
        if not existing_datasets:
            print("❌ 업로드된 데이터가 없습니다.")
            print("💡 먼저 raw 데이터를 업로드해주세요.")
            return False
        
        print(f"\n📂 업로드된 Dataset ({len(existing_datasets)}개):")
        for i, dataset_name in enumerate(existing_datasets, 1):
            print(f"  {i}. {dataset_name}")
    
        while True:
            dataset_choice = input("Dataset 번호 또는 이름 입력: ").strip()                    
            if dataset_choice.isdigit():
                idx = int(dataset_choice) - 1
                if 0 <= idx < len(existing_datasets):
                    dataset = existing_datasets[idx]
                    break
                else:
                    print("❌ 잘못된 번호입니다.")
            else:
                if dataset_choice in existing_datasets:
                    dataset = dataset_choice
                    break
                else:
                    print(f"❌ Dataset '{dataset_choice}'가 존재하지 않습니다.")
            
            print("💡 다시 선택해주세요.")
        
        task = self._select_task()
        
        while True:
            variant = input("\n🏷️ Variant 이름: ").strip()
            if variant:
                break
            print("❌ Variant 이름은 필수입니다. 다시 입력해주세요.")
            
        task_info = self.schema_manager.get_task_info(task)
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
        
        is_valid, error_msg = self.data_manager.schema_manager.validate_task_metadata(task, meta)
        if not is_valid:
            print(f"❌ 검증 실패: {error_msg}")
            return False

        self._show_upload_summary(
            data_file=data_file,
            data_type="task",
            provider=provider,
            dataset=dataset,
            task=task,
            variant=variant,
            meta=meta
        )
        choice = self._ask_yes_no(
            question="업로드를 진행하시겠습니까?",
            default=True,
        )
        if not choice:
            print("❌ 업로드가 취소되었습니다.")
            return False
        
        try:
            staging_dir, job_id = self.data_manager.upload_task(
                data_file=data_file,
                provider=provider,
                dataset=dataset,
                task=task,
                variant=variant,
                meta=meta
            )
            print(f"✅ 업로드 완료: {staging_dir}")
            print("💡 'python main.py process start' 명령으로 처리를 시작할 수 있습니다.")
            return True
        except Exception as e:
            print(f"❌ 업로드 실패: {e}")
            return False

    def _show_upload_summary(
        self, 
        data_file, 
        data_type, 
        provider, 
        dataset, 
        task=None, 
        variant=None,
        meta=None,
        description=None, 
        source=None
    ):
        """업로드 정보 요약 출력"""
        print(f"\n업로드 정보:")
        print(f"file_path: {data_file}")
        print(f"type: {data_type}")
        print(f"Provider: {provider}")
        print(f"Dataset: {dataset}")
        if task:
            print(f"Task: {task}")
        if variant:
            print(f"Variant: {variant}")
        if description:
            print(f"description: {description}")
        if source:
            print(f"source: {source}")
        if meta:
            print(f"meta: {meta}")
            
    def _select_data_type(self):
        """데이터 타입 선택 (raw/task)"""
        while True:
            data_type = input("\n📝 데이터 타입 (raw/task) [raw]: ").strip().lower() or "raw"
            if data_type in ["raw", "task"]:
                return data_type
            print("❌ 잘못된 데이터 타입입니다. (raw 또는 task)")
            
    def _get_file_path(self):
        """데이터 파일 경로 입력 및 검증"""
        data_file = input("📁 데이터 파일 경로: ").strip()
        if not data_file:
            print("❌ 데이터 파일 경로가 필요합니다.")
            return None
        
        try:
            file_type = self.data_manager._get_file_type(data_file)
            print(f"✅ 파일 유형: {file_type}")
            return data_file
        except (FileNotFoundError, ValueError, TypeError) as e:
            print(f"❌ {e}")
            return None        

def main():
    from utils.config import Config

    config = Config.load()
    parser = argparse.ArgumentParser(
        description="Data Manager CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s upload                          # 대화형 업로드
  %(prog)s process start                   # 데이터 처리 시작
  %(prog)s db update                       # DB 업데이트
  %(prog)s config provider list            # Provider 목록 보기
  
For more help on subcommands:
  %(prog)s <command> --help
        '''.strip()
    )
    parser.add_argument("--base-path", default=config.base_path,
                       help="데이터 저장 기본 경로 (default: %(default)s)")
    parser.add_argument("--server-url", default=config.server_url,
                       help="Processing 서버 URL (default: %(default)s)")
    parser.add_argument("--log-level", default=config.log_level.upper(),
                       help="로깅 레벨 (default: %(default)s)")
    parser.add_argument("--num-proc", type=int, default=config.num_proc,
                       help="병렬 처리 프로세스 수 (default: %(default)s)")
    
    subparsers = parser.add_subparsers(dest='command', title='commands', description='사용 가능한 명령어', help='명령어 설명', metavar='<command>')
        
    # Config 관리
    config_parser = subparsers.add_parser('config', help='Schema 설정', description='Schema 설정')
    config_subparsers = config_parser.add_subparsers(dest='config_type', title='Config Types', metavar='<type>')
    
    # Config 목록
    config_subparsers.add_parser('list', help='전체 설정 목록')
    
    # Provider 관리
    provider_parser = config_subparsers.add_parser('provider', help='Provider 관리', description='데이터 제공자(Provider) 설정을 관리합니다')
    provider_subparsers = provider_parser.add_subparsers(dest='provider_action', title='Provider Actions', metavar='<action>')
    provider_create_parser = provider_subparsers.add_parser('create', help='새 Provider 생성',)
    provider_create_parser.add_argument('name', nargs='?', help='새 Provider 이름 (예: "aihub", "huggingface", "opensource")')
    provider_remove_parser = provider_subparsers.add_parser('remove', help='Provider 제거')
    provider_remove_parser.add_argument('name', nargs='?', help='제거할 Provider 이름 (예: "aihub", "huggingface", "opensource")')
    provider_subparsers.add_parser('list', help='Provider 목록 확인')

    # Task 관리
    task_parser = config_subparsers.add_parser('task', help='Task 관리', description='데이터 처리 작업(Task) 설정을 관리합니다')
    task_subparsers = task_parser.add_subparsers(dest='task_action', title='Task Actions', metavar='<action>')
    task_create_parser = task_subparsers.add_parser('create', help='새 Task 생성')
    task_create_parser.add_argument('name', nargs='?', help='새 Task 이름 (예: "ocr", "document_conversion", "layout")')
    task_remove_parser = task_subparsers.add_parser('remove', help='Task 제거')
    task_remove_parser.add_argument('name', nargs='?', help='제거할 Task 이름 (예: "ocr", "document_conversion", "layout")')
    task_subparsers.add_parser('list', help='Task 목록 확인')
    
    # 데이터 업로드 및 내보내기
    subparsers.add_parser('upload', help='데이터 업로드', description='데이터를 업로드합니다.')
    subparsers.add_parser('export', help='데이터 내보내기', description='업로드된 데이터를 내보냅니다.')
    
    
    # 처리 관리
    process_parser = subparsers.add_parser('process', help='Staging 데이터 처리 관리', description='Staging 데이터 처리 작업을 관리합니다.')
    process_subparsers = process_parser.add_subparsers(dest='process_action', title='Process Actions', metavar='<action>')
    process_subparsers.add_parser('start', help='새 처리 시작')
    process_subparsers.add_parser('list', help='내 데이터 전체 현황 확인')
    job_status_parser = process_subparsers.add_parser('status', help='작업 상태 확인')
    job_status_parser.add_argument('job_id', nargs='?', help='확인할 작업 ID (예: abc123)')
    # DB 관리
    db_parser = subparsers.add_parser('db', help='DB 관리', description='DB 상태를 관리합니다.')
    db_subparsers = db_parser.add_subparsers(dest='db_action', title='DB Actions', metavar='<action>')
    db_subparsers.add_parser('info', help='DB 정보 확인')
    db_subparsers.add_parser('update', help='DB 업데이트')
    db_subparsers.add_parser('processes', help='DB 사용 프로세스 확인')
    validate_parser = db_subparsers.add_parser('validate', help='DB 상태 검사 및 문제 해결')
    validate_parser.add_argument('--report', action='store_true', help='검사 보고서 생성')
    
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    
    # CLI 인스턴스 생성
    try:
        cli = DataManagerCLI(
            base_path=args.base_path,
            server_url=args.server_url,
            log_level=args.log_level,
            num_proc=args.num_proc
        )
    except Exception as e:
        print(f"❌ CLI 초기화 실패: {e}")
        return 
    
    try:
        if args.command == 'config':
            if not args.config_type:
                config_parser.print_help()
                return
                
            if args.config_type == 'provider':
                if not args.provider_action:
                    provider_parser.print_help()
                    return
                    
                if args.provider_action == 'create':
                    if not args.name:
                        provider_create_parser.print_help()
                        return
                    cli.create_provider(args.name)    
                elif args.provider_action == 'remove':
                    if not args.name:
                        provider_remove_parser.print_help()
                        return
                    cli.remove_provider(args.name)
                elif args.provider_action == 'list':
                    cli.list_providers()
            
            elif args.config_type == 'task':
                if not args.task_action:
                    task_parser.print_help()
                    return
                    
                if args.task_action == 'create':
                    if not args.name:
                        task_create_parser.print_help()
                        return
                    cli.create_task(args.name)
                elif args.task_action == 'remove':
                    if not args.name:
                        task_remove_parser.print_help()
                        return
                    cli.remove_task(args.name)
                elif args.task_action == 'list':
                    cli.list_tasks()
            elif args.config_type == 'list':
                cli.schema_manager.show_schema_info()
        
        elif args.command == 'upload':
            cli.upload_interactive()
        elif args.command == 'export':
            cli.export_interactive()
        elif args.command == 'process':
            if not args.process_action:
                process_parser.print_help()
                return
            if args.process_action == 'start':
                cli.trigger_processing()
            elif args.process_action == 'status':
                cli.check_job_status(args.job_id)
            elif args.process_action == 'list':
                cli.list_all_data()
        
        elif args.command == 'db':
            if not args.db_action:
                db_parser.print_help()
                return
            elif args.db_action == 'info':
                cli.show_db_info()
            elif args.db_action == 'update':  # 새로 추가
                cli.build_db_interactive()
            elif args.db_action == 'processes':  # 새로 추가
                cli.check_db_processes() 
            elif args.db_action == 'validate':
                cli.validate_db_integrity_interactive(
                    report=args.report
                )

    except KeyboardInterrupt:
        print("\n👋 작업이 중단되었습니다.")
        print("💡 언제든지 다시 시도할 수 있습니다.")
    except FileNotFoundError as e:
        print(f"❌ 파일을 찾을 수 없습니다: {e}")
        print("💡 파일 경로를 확인해주세요.")
    except ValueError as e:
        print(f"❌ 입력 값 오류: {e}")
        print("💡 입력 값을 다시 확인해주세요.")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")


if __name__ == "__main__":
    main()