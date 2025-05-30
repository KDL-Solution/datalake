import argparse
import json
import shutil
import datetime
from pathlib import Path
from config import build_images_root, build_dst_root, parse_to_parts
import pandas as pd

NAS_ROOT = Path("/mnt/AI_NAS/datalake")
STAGING = NAS_ROOT / "_staging"
CATALOG = NAS_ROOT / "catalog"
TRASH = NAS_ROOT / "_trash"


def safe_move(
    src,
    dst,
):
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))


def trash_backup(
    path,
    dry_run=False,
):
    backup_path = TRASH / f"{path.name}.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    if dry_run:
        print(f"[DRY-RUN] Would move {path} → {backup_path}")
    else:
        safe_move(path, backup_path)
        print(f"[TRASH] {path} → {backup_path}")


def check_catalog_integrity(
    catalog_parquet,
    catalog_meta,
):
    if not catalog_parquet.exists() or not catalog_meta.exists():
        print(f"[❌ERROR] Missing {catalog_parquet} or {catalog_meta}")
        return False
    try:
        df = pd.read_parquet(catalog_parquet)
        success = df is not None and not df.empty
        if "image_path" in df.columns:
            for img_path in df["image_path"]:
                img_fp = Path(img_path)
                img_fp = NAS_ROOT / img_fp
                if not img_fp.exists():
                    print(f"[WARN] Missing image: {img_fp}")
                    success = False
        return success
    except Exception as e:
        print(f"[❌ERROR] Catalog integrity check failed: {e}")
        return False


def commit_dataset(
    staging_root,
    catalog_root,
    dry_run=False,
):
    all_parquets = list(staging_root.rglob("data.parquet"))
    print(f"[INFO] Found {len(all_parquets)} parquet files in staging.")

    by_dataset = {}
    for p in all_parquets:
        meta_fp = p.with_name("_meta.json")
        if not meta_fp.exists():
            continue
        meta = json.load(meta_fp.open())
        key = f"{meta['provider']}|{meta['dataset']}|{meta['task']}|{meta['variant']}|{meta['partitions']}"
        build_time = meta.get("build_time", "")
        if key not in by_dataset or build_time > by_dataset[key][2]:
            by_dataset[key] = (p, meta_fp, build_time)

    print(f"[INFO] {len(by_dataset)} unique datasets to commit.")

    for key, (parquet_fp, meta_fp, build_time) in by_dataset.items():
        print(f"\n[COMMIT] Dataset: {key}")
        meta = json.load(meta_fp.open())
        provider, dataset = meta["provider"], meta["dataset"]
        task, variant, partitions = meta["task"], meta["variant"], meta["partitions"]
        print(f"[INFO] Provider: {provider}, Dataset: {dataset}, Task: {task}, Variant: {variant}, Partitions: {partitions}")
        parts = parse_to_parts(partitions)
        catalog_dir = build_dst_root(catalog_root, provider, dataset, task, variant, parts)
        catalog_dir.mkdir(parents=True, exist_ok=True)
        staging_images_dir = build_images_root(staging_root, provider, dataset)
        catalog_images_dir = build_images_root(catalog_root, provider, dataset)
        catalog_images_dir.mkdir(parents=True, exist_ok=True)

        catalog_parquet = catalog_dir / "data.parquet"
        catalog_meta = catalog_dir / "_meta.json"

        # 파케트 파일 읽기
        df = pd.read_parquet(parquet_fp)
        
        # 이미지 복사 및 경로 처리를 함께 진행
        if "image_path" in df.columns:
            # 이미지 경로를 처리할 새로운 리스트 준비
            new_image_paths = []
            
            # 각 이미지에 대해 복사 및 경로 변환 처리
            for img_path in df["image_path"]:
                if img_path is None or not isinstance(img_path, str):
                    new_image_paths.append(img_path)  # None이나 문자열이 아닌 경우 그대로 유지
                    continue
                    
                # 원본 이미지 경로 (절대 경로)
                src_img_path = Path(img_path)
                
                # 상대 경로 생성 (NAS_ROOT 제거)
                if str(src_img_path).startswith(str(NAS_ROOT)):
                    # NAS_ROOT 경로 제거한 상대 경로
                    rel_img_path = src_img_path.relative_to(NAS_ROOT)
                    if str(rel_img_path).startswith("images/"):
                        rel_img_path = rel_img_path.relative_to("images")
                else:
                    rel_img_path = src_img_path
                
                src_file = staging_images_dir / rel_img_path
                dst_file = catalog_images_dir / rel_img_path
                # 카탈로그에 이미지 복사
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                # print(dst_file)
                # exit()
                if not dst_file.exists():
                    if src_file.exists():
                        if dry_run:
                            print(f"[DRY-RUN] Would copy {src_file} → {dst_file}")
                        try:
                            shutil.copy2(src_file, dst_file)
                            print(f"[OK] {src_file} → {dst_file}")
                        except Exception as e:
                            print(f"[❌ERROR] {src_file} → {dst_file}: {e}")
                    else:
                        print(f"[❌ERROR] Source file does not exist: {src_file}")
                
                # 새 DataFrame에는 상대 경로 저장 (images/ 접두사 포함)
                new_image_paths.append(dst_file.relative_to(NAS_ROOT).as_posix())
            
            df["image_path"] = new_image_paths
            print(f"[INFO] Updated image_path column with relative paths")

        # parquet/meta copy
        if dry_run:
            print(f"[DRY-RUN] Would save modified parquet to {catalog_parquet}")
            print(f"[DRY-RUN] Would copy {meta_fp} → {catalog_meta}")
            print(f"[DRY-RUN] DataFrame head:\n{df.head()}")
        else:
            # 수정된 DataFrame을 catalog_parquet에 저장
            df.to_parquet(catalog_parquet, index=False)
            shutil.copy2(meta_fp, catalog_meta)
            print(f"[OK] Modified parquet and meta copied.")

        # catalog integrity check
        if not dry_run:
            if not check_catalog_integrity(catalog_parquet, catalog_meta):
                print(f"[❌ERROR] Catalog integrity check failed for {key}. Skipping staging deletion!")
                continue

        # trash backup
        if dry_run:
            print(f"[DRY-RUN] Would move {parquet_fp} and {meta_fp} to trash")
        else:
            trash_backup(parquet_fp)
            trash_backup(meta_fp)

    print("\n[INFO] Commit process finished.")

if __name__ == "__main__":
    def run_with_error_handling(dry_run=True):
        """에러 처리와 함께 커밋 작업을 수행합니다"""
        try:
            stage_text = "DRY RUN" if dry_run else "COMMIT"
            commit_dataset(STAGING, CATALOG, dry_run=dry_run)
            if not dry_run:
                print("\n✅ 커밋이 성공적으로 완료되었습니다.")
            return True
        except Exception as e:
            print(f"\n❌ [{stage_text}] 오류 발생: {e}")
            print(f"   {stage_text} 작업이 중단되었습니다.")
            return False
    
    # 1단계: Dry Run 실행
    print("\n=== 1단계: Dry Run 실행 ===")
    if not run_with_error_handling(dry_run=True):
        exit(1)
        
    # 2단계: 사용자 확인
    print("\n=== 2단계: 사용자 확인 ===")
    proceed = input("Dry Run이 성공적으로 완료되었습니다.\n실제 커밋을 진행하려면 'y'를 입력하세요 (그 외 입력 시 취소): ").strip().lower()
    
    # 3단계: 실제 커밋 실행 (사용자 확인 후)
    if proceed == "y":
        print("\n=== 3단계: 실제 커밋 실행 ===")
        if not run_with_error_handling(dry_run=False):
            exit(1)
    else:
        print("\n⚠️ 사용자에 의해 작업이 취소되었습니다.")
