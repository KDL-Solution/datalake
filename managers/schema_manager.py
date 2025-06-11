import yaml
import fcntl

from pathlib import Path

class SchemaManager:
    """스키마 검증 및 설정 관리"""
    
    def __init__(
        self, 
        config_path: str = "/mnt/AI_NAS/datalake/config/schema.yaml",
        create_default: bool = False
    ):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            if create_default:
                self.create_default_schema()
            else:
                raise FileNotFoundError(f"❌ 스키마 설정 파일이 없습니다: {self.config_path}")
    
    def _read_config(self):
        """파일락으로 안전하게 설정 읽기"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)  # 공유락
            return yaml.safe_load(f)
    
    def _write_config(self, config):
        """파일락으로 안전하게 설정 쓰기"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # 배타적 락
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)
    
    def validate_provider(self, provider: str) -> bool:
        """Provider 유효성 검증"""
        config = self._read_config()
        return provider in config.get('providers', [])
    
    def validate_task(self, task: str) -> bool:
        """Task 유효성 검증"""
        config = self._read_config()
        return task in config.get('tasks', {})
    
    def get_required_fields(self, task: str) -> list:
        """Task별 필수 필드 조회"""
        config = self._read_config()
        return config.get('tasks', {}).get(task, {}).get('required_fields', [])
    
    def get_allowed_values(self, task: str) -> dict:
        """Task별 허용 값 조회"""
        config = self._read_config()
        return config.get('tasks', {}).get(task, {}).get('allowed_values', {})
    
    def validate_task_metadata(self, task: str, metadata: dict) -> tuple[bool, str]:
        """Task 메타데이터 검증"""
        if not self.validate_task(task):
            return False, f"지원하지 않는 task입니다: {task}"
        
        required_fields = self.get_required_fields(task)
        allowed_values = self.get_allowed_values(task)
        
        # 필수 필드 확인
        for field in required_fields:
            if field not in metadata:
                return False, f"필수 필드 '{field}'가 없습니다"
            
            # 허용 값 확인
            if field in allowed_values:
                if metadata[field] not in allowed_values[field]:
                    return False, f"'{field}' 값이 잘못되었습니다. 허용값: {allowed_values[field]}"
        
        return True, "검증 성공"
    
    def add_provider(self, provider: str) -> bool:
        """새로운 Provider 추가"""
        config = self._read_config()
        
        if provider in config.get('providers', []):
            return False  # 이미 존재
        
        if 'providers' not in config:
            config['providers'] = []
        
        config['providers'].append(provider)
        config['providers'].sort()  # 정렬
        
        self._write_config(config)
        return True
    
    def add_task(self, task: str, required_fields: list = None, allowed_values: dict = None) -> bool:
        """새로운 Task 추가"""
        config = self._read_config()
        
        if task in config.get('tasks', {}):
            return False  # 이미 존재
        
        if 'tasks' not in config:
            config['tasks'] = {}
        
        config['tasks'][task] = {
            'required_fields': required_fields or [],
            'allowed_values': allowed_values or {}
        }
        
        self._write_config(config)
        return True
    
    def update_task(self, task: str, required_fields: list = None, allowed_values: dict = None) -> bool:
        """기존 Task 업데이트"""
        config = self._read_config()
        
        if task not in config.get('tasks', {}):
            return False  # 존재하지 않음
        
        if required_fields is not None:
            config['tasks'][task]['required_fields'] = required_fields
        
        if allowed_values is not None:
            config['tasks'][task]['allowed_values'] = allowed_values
        
        self._write_config(config)
        return True
    
    def remove_provider(self, provider: str) -> bool:
        """Provider 제거"""
        config = self._read_config()
        
        if provider not in config.get('providers', []):
            return False  # 존재하지 않음
        
        config['providers'].remove(provider)
        self._write_config(config)
        return True
    
    def remove_task(self, task: str) -> bool:
        """Task 제거"""
        config = self._read_config()
        
        if task not in config.get('tasks', {}):
            return False  # 존재하지 않음
        
        del config['tasks'][task]
        self._write_config(config)
        return True
    
    def get_all_providers(self) -> list:
        """모든 Provider 목록 조회"""
        config = self._read_config()
        return config.get('providers', [])
    
    def get_all_tasks(self) -> dict:
        """모든 Task 설정 조회"""
        config = self._read_config()
        return config.get('tasks', {})
    
    
    def _get_default_schema(self) -> dict:
        """기본 스키마 구조 반환"""
        return {
            'providers': [
                'aihub',
                'huggingface', 
                'opensource',
                'inhouse'
            ],
            'tasks': {
                'raw': {
                    'required_fields': [],
                    'allowed_values': {}
                },
                'ocr': {
                    'required_fields': ['lang', 'src'],
                    'allowed_values': {
                        'lang': ['ko', 'en', 'ja', 'multi'],
                        'src': ['real', 'synthetic']
                    }
                },
                'kie': {
                    'required_fields': ['lang', 'src'],
                    'allowed_values': {
                        'lang': ['ko', 'en', 'ja', 'multi'],
                        'src': ['real', 'synthetic']
                    }
                },
                'vqa': {
                    'required_fields': ['lang', 'src'],
                    'allowed_values': {
                        'lang': ['ko', 'en', 'ja', 'multi'],
                        'src': ['real', 'synthetic']
                    }
                },
                'layout': {
                    'required_fields': ['lang', 'src'],
                    'allowed_values': {
                        'lang': ['ko', 'en', 'ja', 'multi'],
                        'src': ['real', 'synthetic']
                    }
                },
                'document_conversion': {
                    'required_fields': ['lang', 'src', 'mod'],
                    'allowed_values': {
                        'lang': ['ko', 'en', 'ja', 'multi'],
                        'src': ['real', 'synthetic'],
                        'mod': ['page', 'table', 'chart']
                    }
                }
            }
        }
    
    def create_default_schema(self) -> bool:
        """기본 스키마 파일 생성"""
        if self.config_path.exists():
            print(f"⚠️ 스키마 파일이 이미 존재합니다: {self.config_path}")
            return False
        
        # 디렉토리 생성
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 기본 스키마 생성
        default_schema = self._get_default_schema()
        
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(default_schema, f, default_flow_style=False, allow_unicode=True, indent=2)
            
            print(f"✅ 기본 스키마 파일 생성 완료: {self.config_path}")
            return True
            
        except Exception as e:
            print(f"❌ 스키마 파일 생성 실패: {e}")
            return False
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Schema Manager")
    parser.add_argument('--init', action='store_true', help='기본 스키마 파일 생성')
    parser.add_argument('--config', default='/mnt/AI_NAS/datalake/migrate_test/config/schema.yaml', 
                       help='스키마 파일 경로')
    args = parser.parse_args()
    schema_manager = SchemaManager(args.config, create_default=args.init)
    print(f"Providers: {schema_manager.get_all_providers()}")
    print(f"Tasks: {list(schema_manager.get_all_tasks().keys())}")
    
    # provider: example_provider 생성
    schema_manager.add_provider('example_provider')
    print(f"Providers after adding example_provider: {schema_manager.get_all_providers()}")