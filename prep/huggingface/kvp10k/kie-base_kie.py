import datasets
from datasets import load_dataset
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List
from functools import partial
from PIL import Image
# import fitz  # PyMuPDF
import pdfplumber
from PIL import Image, ImageDraw, ImageFont
import requests
from typing import Tuple, List, Dict
from io import BytesIO
import pdfplumber
import json
from urllib.parse import urlparse
import os
import numpy as np
import json
import requests
from pathlib import Path
import pandas as pd
from tqdm import tqdm

from utils import DATALAKE_DIR, get_safe_image_hash_from_pil


def open_pdf_from_url_safely(url: str) -> pdfplumber.PDF:
    # URL 분석
    parsed = urlparse(url)
    host = parsed.netloc
    referer = f"{parsed.scheme}://{host}/"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115 Safari/537.36"
        ),
        "Accept": "application/pdf",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": referer,
        "Host": host,
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return pdfplumber.open(BytesIO(response.content))


def extract_text_from_normalized_bbox(
    page,
    norm_bbox: Tuple[float, float, float, float],
    dpi: int = 144,
):
    width = page.width
    height = page.height
    cropped = page.crop(
        (
            norm_bbox[0] * width,
            norm_bbox[1] * height,
            norm_bbox[2] * width,
            norm_bbox[3] * height,
            )
    )
    text = cropped.extract_text()
    return (
        text.strip(),
        cropped.to_image(
            resolution=dpi,
        ).original,
    )


def process_dataset_to_parquet(
    dataset,
    images_dir: Path,
    output_parquet_path: Path,
    dpi: int = 144,
):
    images_dir = Path(images_dir).resolve()

    rows = []

    for example in tqdm(dataset, desc="Processing dataset"):
        pdf_url = example["image_url"]
        try:
            pdf = open_pdf_from_url_safely(pdf_url)
        except requests.exceptions.HTTPError:
            print(f"Failed to fetch PDF: {pdf_url}")
            continue

        try:
            page = pdf.pages[example["page_number"] - 1]
            image = page.to_image(
                resolution=dpi,
            ).original
            width, height = image.size
        except Exception as e:
            print(f"Failed to process page: {e}")
            continue

        data = {}
        for el in example["annotations"]:
            linking = el["attributes"]["Linking"]
            if linking is None:
                continue

            coords = el["coordinates"]
            points = [(pt['x'], pt['y']) for pt in coords]
            xs = [pt[0] for pt in points]
            ys = [pt[1] for pt in points]
            left = min(xs)
            right = max(xs)
            top = min(ys)
            bottom = max(ys)

            text, cropped = extract_text_from_normalized_bbox(
                page=page,
                norm_bbox=(left, top, right, bottom),
                dpi=dpi,
            )

            data[el["_id"]] = {
                "text": text,
                "type": el["label"],
                "key": linking["value"],
                "bbox": [
                    round(left * width),
                    round(top * height),
                    round(right * width),
                    round(bottom * height),
                ],
            }

        if data:
            label_dict = {}
            for k, v in data.items():
                if v["key"] in data:
                    label_dict[data[v["key"]]["text"]] = {
                        "<|value|>": data[k]["text"],
                        "<|bbox|>": data[k]["bbox"],
                    }

            label_str = json.dumps(label_dict, ensure_ascii=False, indent=None)
            image_hash = get_safe_image_hash_from_pil(cropped)
            image_path = images_dir / image_hash[:2] / f"{image_hash}.jpg"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            if not image_path.exists():
                image.save(image_path, format="JPEG")

            rows.append({
                "image_path": str(image_path),
                "width": width,
                "height": height,
                "label": label_str,
            })

    df = pd.DataFrame(rows)
    df.to_parquet(output_parquet_path, index=False)
    return df



datalake_dir = "W:/datalake"
dataset="kvp10k"
data_dir = Path(datalake_dir) / f"source/provider=huggingface/dataset={dataset}"
train_dataset, test_dataset = load_dataset(
    "parquet",
    data_files={
        "train": (data_dir / "train/*.parquet").as_posix(),
        "test": (data_dir / "test/*.parquet").as_posix(),
    },
    split=[
        "train",
        "test[:30]",
    ],
)

process_dataset_to_parquet(
    dataset=test_dataset,
    images_dir="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/images",
    output_parquet_path="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/data.parquet",
)
