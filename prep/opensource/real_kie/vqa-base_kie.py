from __future__ import annotations

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
import re
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

IMG_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".gif"}


def get_safe_image_hash_from_pil(img: Image.Image) -> str:
    arr = np.array(img.convert("RGB"))
    meta = f"{arr.shape}{arr.dtype}".encode()
    return hashlib.sha256(arr.tobytes() + meta).hexdigest()


def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


class RealKIEProcessor:
    def __init__(
        self,
        base_data_path: str,
        output_parquet_dir: str,
        output_img_dir: str,
        prompt_config_path: str,
    ):
        self.base = Path(base_data_path)
        self.parquet_out = Path(output_parquet_dir)
        self.img_out_root = Path(output_img_dir)
        self.template = Template(self._load_query_template(prompt_config_path))
        self.parquet_out.mkdir(parents=True, exist_ok=True)
        self.img_out_root.mkdir(parents=True, exist_ok=True)
        self.image_cache: Dict[str, Tuple[str, int, int]] = {}
        self.all_rows: List[Dict[str, Any]] = []
        logging.info(
            "Initialized.  Parquet→%s   Images→%s", self.parquet_out, self.img_out_root
        )

    def _load_query_template(self, prompt_config_path: str) -> str:
        with open(prompt_config_path, "r", encoding="utf-8") as f:
            prompt_config = yaml.safe_load(f)
        return prompt_config.get("query_template", "")

    def _process_image(self, abs_src: Path) -> Optional[Tuple[str, int, int]]:
        try:
            with Image.open(abs_src) as img:
                if img.mode != "RGB":
                    img = img.convert("RGB")
                if img.width > 2048 or img.height > 2048:
                    img.thumbnail((2048, 2048), Image.LANCZOS)

                hashval = get_safe_image_hash_from_pil(img)
                subdir = hashval[:2]
                ext = abs_src.suffix.lower()
                out_path = self.img_out_root / subdir / f"{hashval}{ext}"
                out_path.parent.mkdir(parents=True, exist_ok=True)

                if not out_path.exists():
                    img.save(out_path)

                return (
                    str((Path("images") / subdir / f"{hashval}{ext}")),
                    img.width,
                    img.height,
                )
        except Exception as e:
            logging.warning("이미지 처리 실패: %s (%s)", abs_src, e)
            return None

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

    def _process_row(self, row: pd.Series) -> Optional[Dict[str, Any]]:
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
        if W == 0 or H == 0:
            return None

        img_rel_src = json.loads(row["image_files"].replace("'", '"'))[0]
        abs_img_src = self.base / img_rel_src

        if str(abs_img_src) in self.image_cache:
            img_path_rel, _, _ = self.image_cache[str(abs_img_src)]
        else:
            result = self._process_image(abs_img_src)
            if result is None:
                return None
            img_path_rel, W2, H2 = result
            self.image_cache[str(abs_img_src)] = (img_path_rel, W2, H2)

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
            if any(d["<|value|>"] == val and d["<|bbox|>"] == bbox for d in bucket):
                continue
            bucket.append({"<|value|>": val, "<|bbox|>": bbox})

        if not processed:
            return None

        return {
            "image_path": img_path_rel,
            "label": json.dumps(processed, ensure_ascii=False),
            "width": W,
            "height": H,
        }

    def process(self, limit_datasets: Optional[int] = None):
        datasets = [d for d in self.base.iterdir() if d.is_dir() and d.name != "@eaDir"]
        if limit_datasets:
            datasets = datasets[:limit_datasets]

        for ds in datasets:
            for csv_name in ("train.csv", "val.csv", "test.csv"):
                csv_path = ds / csv_name
                if not csv_path.exists():
                    continue
                df = pd.read_csv(csv_path)
                rows = []
                with ThreadPoolExecutor(max_workers=50) as executor:
                    futures = [
                        executor.submit(self._process_row, row)
                        for _, row in df.iterrows()
                    ]
                    for fut in alive_it(
                        as_completed(futures),
                        total=len(futures),
                        title=ds.name + "/" + csv_name,
                    ):
                        res = fut.result()
                        if res:
                            rows.append(res)
                self.all_rows.extend(rows)

        if self.all_rows:
            out_parquet = self.parquet_out / "data.parquet"
            pd.DataFrame(self.all_rows).to_parquet(out_parquet, index=False)
            logging.info(
                "✓ 통합 parquet 저장 완료 → %s (%d rows)",
                out_parquet,
                len(self.all_rows),
            )
        else:
            logging.warning("⚠ 처리된 row가 없습니다. parquet 저장 생략")

        logging.info("DONE ✓")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base_data_path", type=str, required=True)
    ap.add_argument("--output_parquet_dir", type=str, required=True)
    ap.add_argument("--output_img_dir", type=str, required=True)
    ap.add_argument("--prompt_config_path", type=str, required=True)
    ap.add_argument("--num_datasets", type=int, default=None)
    args = ap.parse_args()

    RealKIEProcessor(
        base_data_path=args.base_data_path,
        output_parquet_dir=args.output_parquet_dir,
        output_img_dir=args.output_img_dir,
        prompt_config_path=args.prompt_config_path,
    ).process(limit_datasets=args.num_datasets)
