#!/usr/bin/env python3
"""
tl-publish
==========

  _staging/{task}/{provider}/{dataset}/
      images/…                       ← 모든 variant 공통
      {variant}/{k1=v1}/{k2=v2}/<uuid>/
          ├── data.parquet
          └── _meta.json

필수 입력
---------
  --parquet : data.parquet 경로
  --provider / --dataset / --task / --variant / --partitions

옵션 입력
---------
  --images  : 실제 이미지 폴더 (없으면 이미지 업로드 생략)

data.parquet 규칙
-----------------
  • `image_path` 컬럼 필수
  • `images` 루트 바로 아래 상대 경로만 기록
      예) 'ea/0001.jpg' (OK)   'images/ea/0001.jpg' (X)
"""
import datetime
import hashlib
import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from utils import DATALAKE_DIR, STAGING
from config import (
    validate_provider,
    validate_parts,
    parse_to_parts,
    build_dst_root,
    build_images_root,
)

# NAS 설정 -------------------------------------------------
RSYNC_OPTS = ["-a", "-z", "--no-perms", "--omit-dir-times"]


# 유틸 ------------------------------------------------------
def sha256_file(
    fp: Path,
) -> str:
    h = hashlib.sha256()
    with fp.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def rsync_dir(
    src: Path,
    dst: Path,
):
    subprocess.check_call(["rsync", *RSYNC_OPTS, f"{src}/", f"{dst}/"])


# 메인 ------------------------------------------------------
def check_existing_versions(
    provider: str, 
    dataset: str, 
    task: str,
    variant: str, 
    parts: dict
) -> list:
    """
    같은 데이터셋(provider, dataset, task, variant, parts)의 버전이
    이미 staging에 있는지 확인합니다.
    
    Returns:
        list: 발견된 기존 버전 경로 목록
    """
    base_path = build_dst_root(
        base_root=STAGING, 
        provider=provider, 
        dataset=dataset, 
        task=task, 
        variant=variant, 
        parts=parts
    )

    if not base_path.exists():
        return []

    existing_versions = []
    for uuid_dir in base_path.iterdir():
        if uuid_dir.is_dir() and (uuid_dir / "data.parquet").exists():
            existing_versions.append(uuid_dir)
    
    return existing_versions


def publish(
    provider: str,
    dataset: str,
    task: str,
    variant: str,
    partitions: str,
    parquet_path: Path,
    images_path: Path | None,
    force: bool = False,
) -> None:

    # ───── 검증 ─────
    if not DATALAKE_DIR.is_dir():
        sys.exit(f"NAS_ROOT not mounted: {DATALAKE_DIR}")

    if not parquet_path.is_file():
        sys.exit(f"parquet not found: {parquet_path}")

    schema_path = parquet_path.with_suffix(".json")
    if not schema_path.is_file():
        sys.exit(f"schema.json not found next to parquet: {schema_path}")
    with schema_path.open("r") as f:
        schema = json.load(f)

    parts = parse_to_parts(partitions)

    validate_provider(provider)
    validate_parts(task, parts)

    # ───── 중복 검사 ─────
    existing_versions = check_existing_versions(provider, dataset, task, variant, parts)
    if existing_versions and not force:
        print(f"⚠️  경고: 동일한 데이터셋이 이미 {len(existing_versions)}개 존재합니다:")
        for i, version_path in enumerate(existing_versions, 1):
            meta_path = version_path / "_meta.json"
            build_time = "알 수 없음"
            if meta_path.exists():
                with meta_path.open() as f:
                    try:
                        meta_data = json.load(f)
                        build_time = meta_data.get("build_time", "알 수 없음")
                    except:
                        pass
            print(f"  {i}. {version_path} (생성일: {build_time})")
        
        user_input = input("계속 진행하시겠습니까? 기존 버전은 유지되며 새 버전이 추가됩니다. (y/n): ")
        if user_input.lower() != 'y':
            print("작업을 취소합니다.")
            sys.exit(0)
        print("기존 버전을 유지하고 새 버전을 추가합니다.")

    uuid_str = uuid.uuid4().hex
    dst_images = build_images_root(
        base_root=STAGING, 
        provider=provider, 
        dataset=dataset
    )
    dst_label = build_dst_root(
        base_root=STAGING, 
        provider=provider, 
        dataset=dataset, 
        task=task, 
        variant=variant, 
        parts=parts
    ).joinpath(uuid_str)
    dst_parquet = dst_label / "data.parquet"

    dst_label.mkdir(parents=True, exist_ok=True)
    if any(dst_label.iterdir()):
        raise RuntimeError(f"staging folder not empty: {dst_label}")

    # ───── 이미지 복사(선택) ─────
    copied_images = False
    if images_path and images_path.is_dir() and any(images_path.iterdir()):
        print(f"copying images → {dst_images}")
        dst_images.mkdir(parents=True, exist_ok=True)
        rsync_dir(images_path, dst_images)
        print("images copied")
        copied_images = True
    else:
        print("no images to upload; skipping image stage")

    # ───── Parquet 복사 ─────
    print(f"copying parquet → {dst_parquet}")
    shutil.copy2(parquet_path, dst_parquet)
    print("parquet copied")

    # ───── 메타 작성 ─────
    meta = {
        "provider": provider,
        "dataset": dataset,
        "task": task,
        "variant": variant,
        "uuid": uuid_str,
        "partitions": partitions,
        "build_time": datetime.datetime.now().isoformat(),
        "schema": schema,
        "schema_version": sha256_file(schema_path),
        "parquet_sha256": sha256_file(dst_parquet),
    }
    (dst_label / "_meta.json").write_text(json.dumps(meta, indent=2))

    # ───── 출력 ─────
    print(f"✅  staging upload → {dst_label}")
    if copied_images:
        print(f"• images    : {_short(dst_images)}")
    print(f"• parquet: {_short(dst_parquet)}")
    print(f"• meta   : {_short(dst_label / '_meta.json')}")


def _short(
    p: Path,
) -> str:
    return str(p).replace(str(DATALAKE_DIR), "")


# CLI -------------------------------------------------------
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--provider",   required=True)
    ap.add_argument("--dataset",    required=True)
    ap.add_argument("--task",       required=True)
    ap.add_argument("--variant",    required=True)
    ap.add_argument("--partitions", required=True,
                    help="comma list e.g. lang=ko,src=real")
    ap.add_argument("--parquet",    required=True, type=Path,
                    help="/path/to/data.parquet")
    ap.add_argument("--images",     type=Path, default=None,
                    help="optional image folder path")
    ap.add_argument("--force",      action="store_true",
                    help="중복 발견 시 확인 없이 강제로 진행")
    args = ap.parse_args()

    publish(
        provider=args.provider,
        dataset=args.dataset,
        task=args.task,
        variant=args.variant,
        partitions=args.partitions,
        parquet_path=args.parquet,
        images_path=args.images,
        force=args.force,
    )
