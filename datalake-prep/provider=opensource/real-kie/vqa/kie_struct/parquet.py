import argparse
import gzip
import json
import logging
import os
import shutil
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from PIL import Image
from jinja2 import Template
from alive_progress import alive_it
import yaml

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

IMG_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".gif"}


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


class RealKIEProcessor:
    def __init__(
        self,
        base_data_path: str,
        output_parquet_dir: str,
        output_img_root: str,
        prompt_config_path: str,
    ):
        self.base = Path(base_data_path)
        self.parquet_out = Path(output_parquet_dir)
        self.img_out_root = Path(output_img_root)
        self.template = Template(self._load_query_template(prompt_config_path))
        self.parquet_out.mkdir(parents=True, exist_ok=True)
        self.img_out_root.mkdir(parents=True, exist_ok=True)
        self.copied_per_dataset: Dict[str, Set[Path]] = {}
        logging.info(
            "Initialized.  Parquet→%s   Images→%s", self.parquet_out, self.img_out_root
        )

    def _load_query_template(self, prompt_config_path: str) -> str:
        with open(prompt_config_path, "r", encoding="utf-8") as f:
            prompt_config = yaml.safe_load(f)
        query_template_str = prompt_config.get("query_template")
        if not query_template_str:
            raise ValueError(
                f"'query_template' 키가 '{prompt_config_path}' 파일에 없습니다."
            )
        return query_template_str

    def _hash_copy_image(
        self,
        abs_src: Path,
        rel_src_in_dataset: str,
        split_dataset_name: str,
        use_two_tier: bool,
    ) -> str:
        h = sha256_hex(rel_src_in_dataset)
        ext = abs_src.suffix.lower()
        hashed_name = f"{h}{ext}"
        if use_two_tier:
            rel_dest_in_dataset = Path(split_dataset_name, "images", h[:2], hashed_name)
        else:
            rel_dest_in_dataset = Path(split_dataset_name, "images", hashed_name)
        abs_dest = self.img_out_root / rel_dest_in_dataset
        if split_dataset_name not in self.copied_per_dataset:
            self.copied_per_dataset[split_dataset_name] = set()
        if abs_dest not in self.copied_per_dataset[split_dataset_name]:
            abs_dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(abs_src, abs_dest)
            except Exception as e:
                logging.error("Failed to copy %s → %s : %s", abs_src, abs_dest, e)
            self.copied_per_dataset[split_dataset_name].add(abs_dest)
        return str(
            Path("images")
            / rel_dest_in_dataset.relative_to(split_dataset_name, "images")
        )

    def _normalize_bbox(self, bbox: List[float], w: int, h: int) -> List[float]:
        if w <= 0 or h <= 0:
            return [0, 0, 0, 0]
        x1, y1, x2, y2 = bbox
        if x2 < x1 or y2 < y1:
            return [0, 0, 0, 0]
        return [
            round(clamp01(x1 / w), 4),
            round(clamp01(y1 / h), 4),
            round(clamp01(x2 / w), 4),
            round(clamp01(y2 / h), 4),
        ]

    def _img_dims(self, p: Path) -> Tuple[int, int]:
        try:
            with Image.open(p) as img:
                return img.width, img.height
        except Exception:
            return 0, 0

    def _find_bbox(self, s: int, e: int, tokens: List[Dict[str, Any]]) -> List[float]:
        INF, ninf = float("inf"), float("-inf")
        minx, miny, maxx, maxy = INF, INF, ninf, ninf
        found = False
        for t in tokens:
            offs = t.get("doc_offset") or {}
            ts, te = offs.get("start", -1), offs.get("end", -1)
            if max(s, ts) >= min(e, te):
                continue
            pos = t.get("position") or {}
            x1, y1, x2, y2 = (
                pos.get("left", -1),
                pos.get("top", -1),
                pos.get("right", -1),
                pos.get("bottom", -1),
            )
            if -1 in (x1, y1, x2, y2) or x2 < x1 or y2 < y1:
                continue
            minx, miny, maxx, maxy = (
                min(minx, x1),
                min(miny, y1),
                max(maxx, x2),
                max(maxy, y2),
            )
            found = True
        return [minx, miny, maxx, maxy] if found else [0, 0, 0, 0]

    def _process_row(
        self, row: pd.Series, split_dataset_name: str, use_two_tier: bool
    ) -> Optional[Dict[str, Any]]:
        try:
            ocr_path = self.base / row["ocr"]
            with gzip.open(ocr_path, "rt", encoding="utf-8") as f:
                ocr = json.load(f)[0]
        except Exception as e:
            logging.warning("OCR load err %s : %s", ocr_path, e)
            return None

        tokens = ocr.get("tokens", [])
        if not tokens:
            return None

        p0 = ocr.get("pages", [{}])[0]
        W, H = p0.get("size", {}).get("width", 0), p0.get("size", {}).get("height", 0)

        img_rel_src = json.loads(row["image_files"].replace("'", '"'))[0]
        abs_img_src = self.base / img_rel_src
        if W == 0 or H == 0:
            W, H = self._img_dims(abs_img_src)
        if W == 0 or H == 0:
            return None

        img_rel_dest = self._hash_copy_image(
            abs_img_src, img_rel_src, split_dataset_name, use_two_tier
        )

        labels_raw = json.loads(row["labels"])
        processed: Dict[str, List[Dict[str, Any]]] = {}
        for lab in labels_raw:
            key, val, s, e = lab["label"], lab["text"], lab["start"], lab["end"]
            if s >= e:
                continue
            bbox_abs = self._find_bbox(s, e, tokens)
            bbox = self._normalize_bbox(bbox_abs, W, H)
            if all(c == 0 for c in bbox):
                continue
            bucket = processed.setdefault(key, [])
            if any(d["value"] == val and d["bbox"] == bbox for d in bucket):
                continue
            bucket.append({"value": val, "bbox": bbox})

        if not processed:
            return None

        query = self.template.render(keys=sorted(processed.keys())).strip()
        return {
            "image_path": img_rel_dest,
            "query": query,
            "label": processed,
            "width": W,
            "height": H,
        }

    def process(self, limit_datasets: Optional[int] = None):
        datasets = [d for d in self.base.iterdir() if d.is_dir() and d.name != "@eaDir"]
        if limit_datasets:
            datasets = datasets[:limit_datasets]

        for di, ds in enumerate(datasets, 1):
            dataset_name = ds.name

            img_count = sum(
                1 for _ in ds.rglob("*") if _.suffix.lower() in IMG_EXTS and _.is_file()
            )
            use_two_tier = img_count >= 10_000
            logging.info(
                "[%d/%d] %s  (images=%d → %s bucket)",
                di,
                len(datasets),
                dataset_name,
                img_count,
                "2-tier" if use_two_tier else "0-tier",
            )

            for csv_name in ("train.csv", "val.csv", "test.csv"):
                csv_path = ds / csv_name
                if not csv_path.exists():
                    continue
                df = pd.read_csv(csv_path)
                rows = []
                split_dataset_name = f"{dataset_name}_{csv_name[:-4]}"
                for _, row in alive_it(
                    df.iterrows(),
                    total=len(df),
                    title=f"{split_dataset_name}",
                ):
                    res = self._process_row(row, split_dataset_name, use_two_tier)
                    if res:
                        rows.append(res)

                if not rows:
                    continue

                out_parquet = self.parquet_out / split_dataset_name / "data.parquet"
                out_parquet.parent.mkdir(parents=True, exist_ok=True)
                pd.DataFrame(rows).to_parquet(out_parquet, index=False)
                logging.info("  → %s (%d rows)", out_parquet.name, len(rows))

        logging.info("DONE ✓")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="RealKIE → Parquet + SHA-256 이미지 버킷 변환",
    )
    ap.add_argument(
        "--base_data_path",
        type=str,
        default="/mnt/AI_NAS/datalake/source/provider=opensource/real-kie",
        help="RealKIE 루트 경로",
    )
    ap.add_argument(
        "--output_parquet_dir",
        type=str,
        default="./processed_realkie_data",
        help="Parquet 저장 경로",
    )
    ap.add_argument(
        "--output_img_root",
        type=str,
        default="./processed_realkie_data",
        help="복사된 이미지 루트",
    )
    ap.add_argument(
        "--num_datasets", type=int, default=None, help="처리할 하위 폴더 수 제한"
    )
    ap.add_argument(
        "--prompt_config_path",
        type=str,
        default="/home/ian/workspace/data/datalake/datalake-prep/provider=opensource/kleister-nda/vqa/kie_struct/prompt_config.yaml",
        help="프롬프트 템플릿을 포함하는 YAML 파일 경로",
    )

    args = ap.parse_args()

    try:
        processor = RealKIEProcessor(
            base_data_path=args.base_data_path,
            output_parquet_dir=args.output_parquet_dir,
            output_img_root=args.output_img_root,
            prompt_config_path=args.prompt_config_path,
        )
        processor.process(limit_datasets=args.num_datasets)
    except (FileNotFoundError, ValueError, Exception) as e:
        logging.error(f"프로그램을 시작할 수 없습니다: {e}")
        exit(1)
