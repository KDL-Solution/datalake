import argparse
import json
import shutil
import datetime
from pathlib import Path
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
        print(f"[ERROR] Missing {catalog_parquet} or {catalog_meta}")
        return False
    try:
        df = pd.read_parquet(catalog_parquet)
        if "image_path" in df.columns:
            for img_path in df["image_path"]:
                img_fp = Path(img_path)
                if not img_fp.exists():
                    print(f"[WARN] Missing image: {img_fp}")
        return True
    except Exception as e:
        print(f"[ERROR] Catalog integrity check failed: {e}")
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
        catalog_dir = catalog_root / provider / dataset / task / variant / partitions
        catalog_dir.mkdir(parents=True, exist_ok=True)
        images_dir = staging_root / provider / dataset / "images"
        catalog_images_dir = catalog_root / provider / dataset / "images"
        catalog_images_dir.mkdir(parents=True, exist_ok=True)

        catalog_parquet = catalog_dir / "data.parquet"
        catalog_meta = catalog_dir / "_meta.json"

        # parquet/meta copy
        if dry_run:
            print(f"[DRY-RUN] Would copy {parquet_fp} → {catalog_parquet}")
            print(f"[DRY-RUN] Would copy {meta_fp} → {catalog_meta}")
        else:
            shutil.copy2(parquet_fp, catalog_parquet)
            shutil.copy2(meta_fp, catalog_meta)
            print(f"[OK] Parquet/meta copied.")

        # image copy
        df = pd.read_parquet(parquet_fp)
        if "image_path" in df.columns:
            for rel_path in df["image_path"]:
                img_fp = images_dir / Path(rel_path)
                cat_img_fp = catalog_images_dir / Path(rel_path)
                cat_img_fp.parent.mkdir(parents=True, exist_ok=True)
                if not cat_img_fp.exists():
                    if dry_run:
                        print(f"[DRY-RUN] Would copy {img_fp} → {cat_img_fp}")
                    else:
                        try:
                            shutil.copy2(img_fp, cat_img_fp)
                            print(f"[OK] {img_fp} → {cat_img_fp}")
                        except Exception as e:
                            print(f"[ERROR] {img_fp} → {cat_img_fp}: {e}")

        # catalog integrity check
        if not dry_run:
            if not check_catalog_integrity(catalog_parquet, catalog_meta):
                print(f"[ERROR] Catalog integrity check failed for {key}. Skipping staging deletion!")
                continue

        # trash backup
        if dry_run:
            print(f"[DRY-RUN] Would move {parquet_fp} and {meta_fp} to trash")
        else:
            trash_backup(parquet_fp)
            trash_backup(meta_fp)

    print("\n[INFO] Commit process finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the commit process without any actual file operations.",
    )
    args = parser.parse_args()

    commit_dataset(
        STAGING,
        CATALOG,
        TRASH,
        dry_run=args.dry_run,
    )
