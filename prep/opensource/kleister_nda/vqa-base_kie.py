from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
import fitz
from PIL import Image
from jinja2 import Template
import yaml
import hashlib
import io
import sys

BASE = Path("/mnt/AI_NAS/datalake")
STAGING_ROOT = Path("staging")
PROVIDER = "provider=opensource"
DATASET = "kleister-nda"
TASK = "vqa"
VARIANT = "kie_struct"
PARTITION = "lang=en/src=real"

DATE = datetime.today()
YEAR = f"year={DATE.year}"
MONTH = f"month={DATE.month:02}"
DAY = f"day={DATE.day:02}"

STAGING_DIR = (
    STAGING_ROOT / PROVIDER / DATASET / TASK / VARIANT / PARTITION / YEAR / MONTH / DAY
)
IMAGE_ROOT = STAGING_ROOT / PROVIDER / DATASET / "images"

THIS_DIR = Path(__file__).resolve().parent
PROMPT_CONFIG_PATH = THIS_DIR / "prompt_config.yaml"

PDF_DIR = BASE / "source" / PROVIDER / DATASET / "documents"
LABEL_PATH = BASE / "source" / PROVIDER / DATASET / "train" / "expected.tsv"


def save_image_sha256(
    img: Image.Image, img_root: Path, rel_base_dir: Path, use_bucket=True
):
    if img.mode != "RGB":
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    sha = hashlib.sha256(buf.getvalue()).hexdigest()
    if use_bucket:
        bucket = sha[:2]
        fpath = img_root / bucket / f"{sha}.jpg"
    else:
        fpath = img_root / f"{sha}.jpg"
    fpath.parent.mkdir(parents=True, exist_ok=True)
    fpath.write_bytes(buf.getvalue())
    rel_path = os.path.relpath(fpath, rel_base_dir)
    return rel_path, sha


def parse_expected_tsv(tsv_path: Path):
    label_lines = []
    with open(tsv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entries = line.split()
            result = {}
            for entry in entries:
                if "=" not in entry:
                    continue
                key, val = entry.split("=", 1)
                result.setdefault(key, []).append(val)
            label_lines.append(result)
    return label_lines


def render_prompt(keys, template_path):
    with open(template_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    tpl = Template(config["query_template"])
    query_keys = [f"the {key}" for key in keys]
    return tpl.render(doc_type="non-disclosure agreement", keys=query_keys)


def main():
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    IMAGE_ROOT.mkdir(parents=True, exist_ok=True)

    label_lines = parse_expected_tsv(LABEL_PATH)
    pdf_files = sorted(PDF_DIR.glob("*.pdf"))

    print(f"📂 PDF files found: {len(pdf_files)}")
    print(f"📑 Label lines parsed: {len(label_lines)}")

    rows = []

    for idx, pdf_file in enumerate(pdf_files):
        if idx >= len(label_lines):
            break
        label = label_lines[idx]

        doc = fitz.open(pdf_file)
        image_paths = []
        widths, heights = [], []

        for page_no in range(len(doc)):
            page = doc.load_page(page_no)
            pix = page.get_pixmap(dpi=150)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            image_path, _ = save_image_sha256(img, IMAGE_ROOT, BASE, use_bucket=True)
            image_paths.append(image_path)
            widths.append(pix.width)
            heights.append(pix.height)

        label_json = {}
        for key, values in label.items():
            for v in values:
                label_json_key = f"{key}::{v}" if len(values) > 1 else key
                label_json[label_json_key] = {"<|value|>": v}

        query = render_prompt(label_json.keys(), PROMPT_CONFIG_PATH)

        rows.append(
            {
                "image_paths": image_paths,
                "query": query,
                "label": json.dumps(label_json, ensure_ascii=False),
                "widths": widths,
                "heights": heights,
            }
        )

    df = pd.DataFrame(rows)
    pq.write_table(pa.Table.from_pandas(df), STAGING_DIR / "kleister_nda_train.parquet")
    print("✅ Saved:", STAGING_DIR / "kleister_nda_train.parquet")


if __name__ == "__main__":
    main()
