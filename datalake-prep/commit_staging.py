#!/usr/bin/env python3
"""
tl-commit – 자동 커밋 파이프라인 (_meta.json 기반)

- staging/{provider}/{dataset}/images/            ← 모든 variant 공통
- staging/{provider}/{dataset}/{task}/{variant}/{partitions}/{uuid}/
      ├── data.parquet
      └── _meta.json

- catalog/{provider}/{dataset}/images/            ← 집합적으로 병합됨(중복 없음)
- catalog/{provider}/{dataset}/{task}/{variant}/{partitions}/data.parquet

동작
● staging 하위 모든 data.parquet 순회
● 각 data.parquet 옆 _meta.json에서 provider, dataset, task, variant, partitions 등 읽어 catalog 경로 결정
● parquet 내 image_path를 catalog 절대경로로 변환, date 컬럼 추가
● 이미지 검증 및 images/ 병합(중복 무시)
● _meta.json 복사
"""
import json
import shutil
import datetime
import subprocess
from pathlib import Path
from alive_progress import alive_bar
import pandas as pd
import os
from config import validate_parts, parse_to_parts, build_images_root, build_dst_root

NAS_ROOT = Path("/mnt/AI_NAS/datalake")
STAGING = NAS_ROOT / "_staging"
CATALOG = NAS_ROOT / "catalog"
RSYNC_OPTS = ["-a", "--ignore-existing", "-z"]

def sanity_img_check(parquet_fp: Path, staging_images: Path, catalog_images: Path):
    tbl = pd.read_parquet(parquet_fp)
    if "image_path" not in tbl.columns:
        raise ValueError(f"parquet file {parquet_fp} does not contain 'image_path' column.")
    
    paths = tbl["image_path"].dropna().unique().tolist()
    filtered_paths = []
    if not paths:
        return
    with alive_bar(len(paths), title="image_path sanity check") as bar:
        for rel_path in paths:
            rel_img_path = Path(rel_path)
            # images/aaa.jpg 또는 images/bucket/hash.jpg 형태 지원
            # 상대경로가 images/부터 시작하는지 체크 필요
            if str(rel_img_path).startswith("images/"):
                rel_img_path = rel_img_path.relative_to("images")
            staging_fp = staging_images / rel_img_path
            catalog_fp = catalog_images / rel_img_path
            if not staging_fp.exists() and not catalog_fp.exists():
                raise FileNotFoundError(
                    f"image_path missing: {rel_path} (checked {staging_fp} and {catalog_fp})"
                )
            if staging_fp.exists():
                filtered_paths.append(staging_fp)
            bar()
    return filtered_paths

def same_filesystem(src: Path, dst: Path) -> bool:
    """src, dst가 동일 파일시스템에 있으면 True"""
    src_stat = os.statvfs(str(src))
    dst_stat = os.statvfs(str(dst))
    return src_stat.f_fsid == dst_stat.f_fsid

def merge_images(filtered_paths: list[Path], src_images: Path, dst_images: Path):
    """
    staging/images → catalog/images 병합.
    동일 파일시스템이면 move, 아니면 rsync 기반 복사+삭제
    """
    if not src_images.is_dir():
        print(f"[ERROR] Source {src_images} is not a directory.")
        return

    dst_images.mkdir(parents=True, exist_ok=True)
    is_same_system = same_filesystem(src_images, dst_images)
    
    with alive_bar(len(filtered_paths), title="Merging images") as bar:
        for rel_img_path in filtered_paths:
            src_fp = src_images / rel_img_path.name
            dst_fp = dst_images / rel_img_path.name
            
            dst_fp.parent.mkdir(parents=True, exist_ok=True)  # 대상 디렉토리 생성
            
            if is_same_system:
                # 같은 파일시스템: os.rename 사용 (최고속)
                try:
                    os.rename(src_fp, dst_fp)
                    print(f"[MOVE] {src_fp} → {dst_fp}")
                except Exception as e:
                    print(f"[ERROR][MOVE] {src_fp} → {dst_fp} : {e}")
            else:
                # 다른 파일시스템: shutil copy 후 원본 삭제
                try:
                    import shutil
                    shutil.copy2(src_fp, dst_fp)
                    os.remove(src_fp)
                    print(f"[COPY+DELETE] {src_fp} → {dst_fp}")
                except Exception as e:
                    print(f"[ERROR][COPY+DELETE] {src_fp} → {dst_fp} : {e}")
            bar()

def update_parquet_paths(parquet_fp: Path, dst_parquet_fp: Path, dst_images: Path):
    date_str = datetime.date.today().strftime("%Y-%m-%d")
    df = pd.read_parquet(parquet_fp)
    dst_images_prefix = str(dst_images)
    df["image_path"] = df["image_path"].apply(lambda p: f"{dst_images_prefix}/{p}")
    df["date"] = date_str
    return df.to_parquet(dst_parquet_fp, index=False)

def remove_hidden_files_and_empty_dirs(root: Path):
    for path in sorted(root.rglob("*"), key=lambda p: -len(str(p))):
        # 숨김 파일/디렉토리 삭제
        if path.name in (".DS_Store", "@eaDir") or path.name.startswith("._"):
            try:
                if path.is_file():
                    
                    path.unlink()
                elif path.is_dir():
                    shutil.rmtree(path)
            except Exception as e:
                print(f"[ERROR] {path}: {e}")
                pass
        # 빈 폴더 삭제
        elif path.is_dir():
            try:
                path.rmdir()
            except OSError:
                pass
            
def commit_all():
    parquet_files = list(STAGING.rglob("data.parquet"))
    print(f"[INFO] {len(parquet_files)} parquet file(s) found for commit.")
    for parquet_fp in parquet_files:
        meta_fp = parquet_fp.with_name("_meta.json")
        if not meta_fp.is_file():
            print(f"[WARN] _meta.json missing for {parquet_fp}; skipping.")
            continue
        meta = json.load(meta_fp.open())
        provider = meta["provider"]
        dataset = meta["dataset"]
        task = meta["task"]
        variant = meta["variant"]
        partitions = meta["partitions"]
        parts = parse_to_parts(partitions)
        validate_parts(task, parts)
        dst_root = build_dst_root(
            base_root=CATALOG,
            provider=provider,
            dataset=dataset,
            task=task,
            variant=variant,
            parts=parts
        )
        dst_root.mkdir(parents=True, exist_ok=True)

        # images/ 경로
        staging_images = build_images_root(
            base_root=STAGING,
            provider=provider,
            dataset=dataset
        )
        catalog_images = build_images_root(
            base_root=CATALOG,
            provider=provider,
            dataset=dataset
        )

        filtered_paths = sanity_img_check(parquet_fp, staging_images, catalog_images)
        merge_images(filtered_paths, staging_images, catalog_images)
        
        dst_parquet_fp = dst_root / "data.parquet"
        update_parquet_paths(parquet_fp, dst_parquet_fp, catalog_images)
        if dst_parquet_fp.exists():
            # backup with timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            backup_fp = dst_parquet_fp.with_suffix(f".{timestamp}.bak")
            print(f"[WARN] {dst_parquet_fp} already exists; backing up to {backup_fp}")
            shutil.copy2(dst_parquet_fp, backup_fp)
        print(f"writing parquet → {dst_parquet_fp}")
        shutil.copy2(meta_fp, dst_root / "_meta.json")
        print(f"✅ committed: {dst_parquet_fp}")
        
        parquet_fp.unlink()
        meta_fp.unlink()
        if staging_images.is_dir() and not any(staging_images.iterdir()):
            print(f"Removing empty directory: {staging_images}")
            staging_images.rmdir()
        
    remove_hidden_files_and_empty_dirs(STAGING)
    print(f"✅ {len(parquet_files)} parquet file(s) committed.")
    print(f"✅ {STAGING} cleaned up.")
        
        
        

if __name__ == "__main__":
    commit_all()
