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
from config import (
    validate_parts,
    parse_to_parts,
    build_images_root,
    build_dst_root,
)

NAS_ROOT = Path("/mnt/AI_NAS/datalake")
STAGING = NAS_ROOT / "_staging"
CATALOG = NAS_ROOT / "catalog"
RSYNC_OPTS = ["-a", "--ignore-existing", "-z"]


def sanity_img_check(
    parquet_fp: Path,
    staging_images: Path,
    catalog_images: Path,
):
    tbl = pd.read_parquet(parquet_fp)
    if "image_path" not in tbl.columns:
        return []
    
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
                filtered_paths.append(rel_img_path)
            bar()
    return filtered_paths


def same_filesystem(
    src: Path,
    dst: Path,
) -> bool:
    """src, dst가 동일 파일시스템에 있으면 True"""
    src_stat = os.statvfs(str(src))
    dst_stat = os.statvfs(str(dst))
    return src_stat.f_fsid == dst_stat.f_fsid


def merge_images(
    filtered_paths: list[Path],
    src_images: Path,
    dst_images: Path,
):
    """
    staging/images → catalog/images 병합.
    동일 파일시스템이면 move, 아니면 rsync 기반 복사+삭제
    """
    if not filtered_paths:
        print("[INFO] No images to merge")
        return

    src_images.mkdir(parents=True, exist_ok=True)
    dst_images.mkdir(parents=True, exist_ok=True)
    is_same_system = same_filesystem(
        src_images,
        dst_images,
    )

    with alive_bar(len(filtered_paths), title="Merging images") as bar:
        for rel_img_path in filtered_paths:
            src_fp = src_images / rel_img_path
            dst_fp = dst_images / rel_img_path

            dst_fp.parent.mkdir(parents=True, exist_ok=True)  # 대상 디렉토리 생성

            if is_same_system:
                # 같은 파일시스템: os.rename 사용 (최고속)
                try:
                    os.rename(src_fp, dst_fp)
                    print(f"[MOVE] {src_fp} → {dst_fp}")
                    pass
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


def update_parquet_paths(
    parquet_fp: Path,
    dst_parquet_fp: Path,
    dst_images: Path,
):
    date_str = datetime.date.today().strftime("%Y-%m-%d")
    df = pd.read_parquet(parquet_fp)
    dst_images_prefix = str(dst_images)
    if "image_path" in df.columns:
        df["image_path"] = df["image_path"].apply(lambda p: f"{dst_images_prefix}/{p}")
    df["date"] = date_str
    return df.to_parquet(dst_parquet_fp, index=False)


def remove_hidden_files_and_empty_dirs(
    root: Path,
):
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


def group_by_dataset():
    """
    staging의 data.parquet 파일들을 데이터셋별로 그룹화하고, 
    각 그룹에서 가장 최신 버전만 선택하여 반환.
    
    Returns:
        dict: {dataset_key: latest_parquet_path}
    """
    dataset_groups = {}
    all_parquets = list(STAGING.rglob("data.parquet"))

    for parquet_fp in all_parquets:
        meta_fp = parquet_fp.with_name("_meta.json")
        if not meta_fp.is_file():
            print(f"[WARN] _meta.json missing for {parquet_fp}; skipping.")
            continue

        try:
            meta = json.load(meta_fp.open())
            provider = meta["provider"]
            dataset = meta["dataset"]
            task = meta["task"]
            variant = meta["variant"]
            partitions = meta["partitions"]
            build_time = meta.get("build_time", "")

            # 데이터셋 고유 키 생성
            dataset_key = f"{provider}|{dataset}|{task}|{variant}|{partitions}"

            # 이미 존재하는 경우, 더 최신 버전인지 확인
            if dataset_key in dataset_groups:
                existing_meta_fp = dataset_groups[dataset_key][1]
                existing_meta = json.load(existing_meta_fp.open())
                existing_build_time = existing_meta.get("build_time", "")
                
                # 기존 파일이 더 최신이면 유지, 아니면 교체
                if build_time > existing_build_time:
                    print(f"[INFO] Found newer version of {dataset_key}: {parquet_fp}")
                    dataset_groups[dataset_key] = (parquet_fp, meta_fp, meta)
            else:
                dataset_groups[dataset_key] = (parquet_fp, meta_fp, meta)
                
        except Exception as e:
            print(f"[ERROR] Processing {parquet_fp}: {e}")
            continue

    # 결과 로깅
    total = len(all_parquets)
    unique = len(dataset_groups)
    duplicates = total - unique

    print(f"[INFO] Found {total} total parquet files")
    print(f"[INFO] {unique} unique datasets")
    if duplicates > 0:
        print(f"[INFO] {duplicates} duplicate datasets will be skipped (only latest version will be committed)")
    return dataset_groups


def clean_staging():
    """
    staging 영역에 있는 모든 파일과 디렉토리를 삭제합니다.
    provider=xxx 디렉토리 구조는 유지하되, 모든 내용물은 삭제합니다.
    """
    print("[INFO] 모든 staging 파일과 디렉토리를 정리합니다...")

    # 단계 1: data.parquet 파일 체크
    remaining_parquets = list(STAGING.rglob("data.parquet"))
    if remaining_parquets:
        print(f"[WARN] {len(remaining_parquets)}개의 parquet 파일이 아직 남아있습니다. 모두 삭제합니다.")
        for p in remaining_parquets:
            try:
                p.unlink()
                print(f"[DELETE] Parquet: {p}")
            except Exception as e:
                print(f"[ERROR] Failed to delete {p}: {e}")

    # 단계 2: _meta.json 파일 체크
    remaining_metas = list(STAGING.rglob("_meta.json"))
    if remaining_metas:
        print(f"[WARN] {len(remaining_metas)}개의 메타데이터 파일이 아직 남아있습니다. 모두 삭제합니다.")
        for m in remaining_metas:
            try:
                m.unlink()
                print(f"[DELETE] Meta: {m}")
            except Exception as e:
                print(f"[ERROR] Failed to delete {m}: {e}")

    # 단계 3: provider 디렉토리 내부 삭제
    providers = [d for d in STAGING.iterdir() if d.is_dir() and d.name.startswith("provider=")]

    for provider_dir in providers:
        print(f"[INFO] {provider_dir} 내의 모든 파일 및 디렉토리 삭제 중...")

        # provider 디렉토리 내의 모든 내용을 삭제
        for item in provider_dir.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                    print(f"[DELETE] File: {item}")
                elif item.is_dir():
                    shutil.rmtree(item)
                    print(f"[DELETE] Directory: {item}")
            except Exception as e:
                print(f"[ERROR] Failed to delete {item}: {e}")

    # 단계 4: 남은 모든 디렉토리 삭제 (UUID 디렉토리 등)
    for path in sorted(STAGING.rglob("*"), key=lambda p: -len(str(p))):
        if path.is_dir() and path not in providers:
            try:
                shutil.rmtree(path)
                print(f"[DELETE] Directory: {path}")
            except Exception as e:
                try:
                    # rmtree 실패하면 빈 디렉토리 삭제 시도
                    path.rmdir()
                    print(f"[DELETE] Empty directory: {path}")
                except Exception as e2:
                    print(f"[ERROR] Failed to delete directory {path}: {e2}")

    # 단계 5: 숨김 파일 및 빈 디렉토리 정리
    remove_hidden_files_and_empty_dirs(STAGING)

    print("[INFO] 모든 staging 파일과 디렉토리가 정리되었습니다.")

    # 삭제 확인 - provider 디렉토리만 남아있어야 함
    remaining = []
    for p in providers:
        remaining.extend(list(p.rglob("*")))

    if remaining:
        print(f"[WARN] {len(remaining)}개 파일/디렉토리가 여전히 staging 영역에 남아있습니다.")
        for r in remaining[:10]:  # 처음 10개만 표시
            print(f"  - {r}")
        if len(remaining) > 10:
            print(f"  - ... 그 외 {len(remaining)-10}개 항목")
    else:
        print("[INFO] provider 디렉토리만 남아있고 모든 내용물이 정리되었습니다.")


def commit_all():
    dataset_groups = group_by_dataset()
    print(f"[INFO] Committing {len(dataset_groups)} unique datasets.")

    for dataset_key, (parquet_fp, meta_fp, meta) in dataset_groups.items():
        provider = meta["provider"]
        dataset = meta["dataset"]
        task = meta["task"]
        variant = meta["variant"]
        partitions = meta["partitions"]
        parts = parse_to_parts(partitions)
        validate_parts(
            task=task,
            parts=parts,
        )

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

        filtered_paths = sanity_img_check(
            parquet_fp,
            staging_images,
            catalog_images,
        )
        merge_images(
            filtered_paths=filtered_paths,
            src_images=staging_images,
            dst_images=catalog_images,
        )

        dst_parquet_fp = dst_root / "data.parquet"
        if dst_parquet_fp.exists():
            # backup with timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            backup_fp = dst_parquet_fp.with_suffix(f".{timestamp}.bak")
            print(f"[WARN] {dst_parquet_fp} already exists; backing up to {backup_fp}")
            shutil.copy2(dst_parquet_fp, backup_fp)
        update_parquet_paths(parquet_fp, dst_parquet_fp, catalog_images)
        print(f"{dataset_key} committed to {dst_parquet_fp}")
        shutil.copy2(meta_fp, dst_root / "_meta.json")
        print(f"✅ committed: {dst_parquet_fp}")

        # 처리된 파일 및 관련 스테이징 파일 정리
        parquet_fp.unlink()
        meta_fp.unlink()
        if staging_images.is_dir() and not any(staging_images.iterdir()):
            print(f"Removing empty directory: {staging_images}")
            staging_images.rmdir()

    # commit 후 staging 정리
    clean_staging()
    print(f"✅ {len(dataset_groups)} unique dataset(s) committed.")
    print(f"✅ {STAGING} cleaned up.")


if __name__ == "__main__":
    commit_all()
