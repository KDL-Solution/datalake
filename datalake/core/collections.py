
import json
import shutil
import hashlib
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Union
import pandas as pd
from datasets import Dataset, load_from_disk

class CollectionManager:
    """학습용 데이터셋 관리"""
    
    def __init__(self, collections_path: str = "/mnt/AI_NAS/datalake/collections"):
        self.collections_path = Path(collections_path)

    def load_collection(self, name: str, version: str = "latest") -> Dataset:
        """컬렉션 로드"""
        collection_dir = self._get_collection_path(name, version)
        if not collection_dir.exists():
            raise FileNotFoundError(f"컬렉션을 찾을 수 없습니다: {name}@{version}")
            
        return load_from_disk(str(collection_dir))
    
    def save_collection(
        self,
        collection: Dataset,
        name: str,
        version: str = None,
        description: str = "",
        user_id: str = "user",
        auto_version: bool = True
    ) -> str:
        """데이터셋 저장"""
        
        # 자동 버전 관리
        if version is None or auto_version:
            version = self._get_next_version(name, version)
            
        collection_dir = self.collections_path / name / version
        collection_dir.mkdir(parents=True, exist_ok=True)
        
        # 데이터셋 저장
        collection.save_to_disk(str(collection_dir))
        
        # 메타데이터 생성
        metadata = self._create_metadata(
            name=name,
            version=version,
            description=description,
            created_by=user_id,
            num_samples=len(collection),
        )
        
        # 메타데이터 저장
        with open(collection_dir / "metadata.json", 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
            
        # latest 심볼릭 링크 업데이트
        self._update_latest_link(name, version)
        
        collection_dir.chmod(0o777)
        for path in collection_dir.rglob('*'):
            path.chmod(0o777)
        return version
  
    def export_collection(
        self, 
        name: str, 
        version: str, 
        output_path: Union[str, Path],
        format: str = "datasets"
    ) -> Path:
        collection = self.load_collection(name, version)
        
        output_path = Path(output_path)
        
        if format == "auto":
            if output_path.suffix.lower() == ".parquet":
                format = "parquet"
            else:
                format = "dataset"  # 기본값
                
        if format == "datasets":
            # datasets 형태로 저장
            output_path.mkdir(parents=True, exist_ok=True)
            collection.save_to_disk(str(output_path))
                
        elif format == "parquet":
            # parquet 파일로 저장
            output_path = output_path.with_suffix('.parquet')
            output_path.parent.mkdir(parents=True, exist_ok=True)
            collection.to_parquet(str(output_path))
            
        else:
            raise ValueError(f"지원하지 않는 형식입니다: {format}")
            
        return output_path
    
    def delete_collection(self, name: str, version: str = None) -> bool:
        try:
            collection_base = self.collections_path / name
            if version is None:
                # 전체 컬렉션 삭제
                if collection_base.exists():
                    shutil.rmtree(collection_base)
                    return True
                return False
            else:
                # 특정 버전만 삭제
                collection_dir = collection_base / version
                if not collection_dir.exists():
                    return False
                    
                shutil.rmtree(collection_dir)
                
                remaining_versions = self.list_versions(name)
                if remaining_versions:
                    self._update_latest_link(name, remaining_versions[0])
                else:
                    if collection_base.exists():
                        shutil.rmtree(collection_base)
                        
                return True
                
        except Exception as e:
            print(f"Delete error: {e}")
            return False
    
    def list_collections(self) -> List[str]:
        """컬렉션 목록 조회"""
        collections = []
        
        if not self.collections_path.exists():
            return collections
            
        for collection_dir in self.collections_path.iterdir():
            if not collection_dir.is_dir():
                continue
                
            name = collection_dir.name
            versions = self.list_versions(name)
            
            if not versions:
                continue
                
            # latest 버전의 메타데이터 가져오기
            latest_version = self._get_latest_version(name) if versions else "unknown"
            try:
                collection_info = self.get_metadata(name, latest_version)
            except Exception:
                collection_info = self._create_metadata(
                    name=name,
                    version=latest_version,
                    description="",
                    created_by="unknown",
                    num_samples=0,
                )
            
            collection_info['num_versions'] = len(versions)
            collections.append(collection_info)
            
        return sorted(collections, key=lambda x: x["name"])
    
    def get_collection_info(self, name: str, version: str = "latest") -> Dict:
        """컬렉션 상세 정보 조회 (로직만)"""
        metadata = self.get_metadata(name, version)
        versions = self.list_versions(name)
        
        return {
            **metadata,
            "all_versions": versions,
            "num_versions": len(versions)
        }
    
        
    def get_metadata(self, name: str, version: str) -> Dict:
        collection_base = self.collections_path / name / version
        metadata_file = collection_base / "metadata.json"
        
        if not metadata_file.exists():
            raise FileNotFoundError(f"메타데이터 파일을 찾을 수 없습니다: {metadata_file}")
            
        with open(metadata_file, 'r', encoding='utf-8') as f:
            return json.load(f)
        
    def list_versions(self, name: str) -> List[str]:
        collection_base = self.collections_path / name
        if not collection_base.exists():
            return []
            
        versions = []
        for version_dir in collection_base.iterdir():
            if version_dir.is_dir() and version_dir.name != "latest":
                versions.append(version_dir.name)
                
        return sorted(versions, key=self._version_sort_key, reverse=True)

    def _get_latest_version(self, name: str) -> str:
        """latest 버전 조회"""
        latest_link = self.collections_path / name / "latest"
        
        if not latest_link.exists():
            return "unknown"
            
        target_version = latest_link.resolve().name
        return target_version if target_version else "unknown"
    
    def _version_sort_key(self, version: str):
        match = re.search(r'(\D*)(\d+)', version)
        if match:
            prefix, num = match.groups()
            return (prefix, int(num))
        return (version, 0)  # 숫자가 없으면 문자열 그대로

    def _create_metadata(
        self,
        name: str,
        version: str,
        description: str = "",
        created_by: str = "user",
        num_samples: int = 0,
    ) -> Dict:
        """메타데이터 생성"""
        return {
            "name": name,
            "version": version,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "created_by": created_by,
            "num_samples": num_samples
        }
        
    def _update_latest_link(self, name: str, version: str):
        """latest 심볼릭 링크 업데이트"""
        latest_link = self.collections_path / name / "latest"
        target_path = Path(version)
        
        if latest_link.exists():
            latest_link.unlink()
            
        latest_link.symlink_to(target_path, target_is_directory=True)
        
    def _get_next_version(
        self, 
        name: str, 
        suggested_version: str = None, 
        default_version: str = "v1",
    ) -> str:
        """다음 버전 결정"""
        existing_versions = self.list_versions(name)
        
        if suggested_version:
            if suggested_version not in existing_versions:
                return suggested_version
            else:
                raise ValueError(f"버전 {suggested_version}은 이미 존재합니다.")
            
        if not existing_versions:
            return default_version
        
        match = re.search(r'(\D*)(\d+)', default_version)
        if match:
            prefix, start_num = match.groups()
            max_num = int(start_num)
            
            for version in existing_versions:
                version_match = re.search(rf'^{re.escape(prefix)}(\d+)$', version)
                if version_match:
                    max_num = max(max_num, int(version_match.group(1)))
            
            return f"{prefix}{max_num + 1}"
        else:
            return f"{default_version}1"
        
    def _get_collection_path(self, name: str, version: str) -> Path:

        if version == "latest":
            latest_link = self.collections_path / name / "latest"
            if latest_link.exists() and latest_link.is_symlink():
                return latest_link.resolve()
            else:
                # latest 링크가 없으면 가장 최신 버전 사용
                versions = self.list_versions(name)
                if versions:
                    return self.collections_path / name / versions[0]
                else:
                    raise FileNotFoundError(f"컬렉션이 없습니다: {name}")
        else:
            return self.collections_path / name / version