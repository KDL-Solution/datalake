from datasets import load_dataset
from io import BytesIO
from pathlib import Path
import pdfplumber
import requests
from typing import Tuple, List, Dict
from io import BytesIO
import pdfplumber
import json
from urllib.parse import urlparse
import json
import requests
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import fitz  # PyMuPDF
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import json
from PIL import Image
from urllib.parse import urlparse, unquote
from io import StringIO
import contextlib

from prep.utils import DATALAKE_DIR, get_safe_image_hash_from_pil


# @contextlib.contextmanager
# def suppress_output():
#     with contextlib.redirect_stdout(StringIO()), contextlib.redirect_stderr(StringIO()):
#         yield


def build_dynamic_headers(pdf_url: str) -> dict:
    parsed = urlparse(pdf_url)
    host = parsed.netloc
    referer = f"{parsed.scheme}://{host}"
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": referer,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Host": host,
    }


def download_pdf_with_dynamic_headers(
    pdf_url: str,
    save_dir: str,
    chunk_size: int = 8192,
) -> Path:
    file_name = Path(unquote(urlparse(pdf_url).path)).name
    output_path = Path(save_dir) / file_name
    if output_path.exists():
        return output_path.as_posix()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    headers = build_dynamic_headers(pdf_url)

    response = requests.get(pdf_url, headers=headers, stream=True, timeout=10)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "")
    if "application/pdf" not in content_type:
        raise ValueError(f"Unexpected content type: {content_type}")

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            f.write(chunk)
    return output_path.as_posix()


def extract_text_from_normalized_bbox(page, norm_bbox, dpi=144):
    width, height = page.rect.width, page.rect.height
    rect = fitz.Rect(
        norm_bbox[0] * width,
        norm_bbox[1] * height,
        norm_bbox[2] * width,
        norm_bbox[3] * height,
    )
    text = page.get_text("text", clip=rect).strip()

    # render cropped image
    pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72), clip=rect)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return text, img


def process_dataset_to_parquet(
    dataset,
    data_dir,
    images_dir,
    output_parquet_path,
    dpi=144,
    indent=None,
):
    images_dir = Path(images_dir).resolve()

    # .pdf를 다운로드부터 받아 놓음:
    failed = set(),
    for example in tqdm(dataset, desc="Downloading dataset"):
        pdf_url = example["image_url"]
        try:
            pdf_path = download_pdf_with_dynamic_headers(
                pdf_url=pdf_url,
                save_dir=data_dir,
            )
        except requests.exceptions.HTTPError:
            if pdf_url not in failed:
                print(f"Failed to fetch PDF: {pdf_url}")
                failed.add(pdf_url)
            continue
        except Exception as e:
            continue

    rows = []
    for example in tqdm(dataset, desc="Processing dataset"):
        pdf_url = example["image_url"]
        try:
            pdf_path = download_pdf_with_dynamic_headers(
                pdf_url=pdf_url,
                save_dir=data_dir,
            )
            pdf = fitz.open(pdf_path)
        except requests.exceptions.HTTPError:
            continue
        except Exception as e:
            print(f"Failed to open PDF: {pdf_url}\n{e}")
            continue

        try:
            page = pdf[example["page_number"] - 1]
            pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72))
            image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            width, height = image.size
        except Exception as e:
            # print(len(pdf))
            # print(f"Failed to process page: {e}")
            continue

        data = {}
        for el in example["annotations"]:
            linking = el["attributes"].get("Linking")
            if linking is None:
                continue

            coords = el["coordinates"]
            points = [(pt['x'], pt['y']) for pt in coords]
            xs = [pt[0] for pt in points]
            ys = [pt[1] for pt in points]
            left, right = min(xs), max(xs)
            top, bottom = min(ys), max(ys)

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

        if not data:
            continue

        label_dict = {}
        for k, v in data.items():
            if v["key"] in data:
                value = data[k]["text"].strip()
                if value:
                    label_dict[data[v["key"]]["text"]] = {
                        "<|value|>": value,
                        "<|bbox|>": data[k]["bbox"],
                    }

        label_str = json.dumps(
            label_dict,
            ensure_ascii=False,
            indent=indent,
        )

        image_hash = get_safe_image_hash_from_pil(cropped)
        image_path = images_dir / image_hash[:2] / f"{image_hash}.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        if not image_path.exists():
            image.save(image_path, format="JPEG")

        rows.append({
            "image_path": Path(*image_path.parts[-2:]).as_posix(),
            "width": width,
            "height": height,
            "label": label_str,
        })

    df = pd.DataFrame(rows)
    df.to_parquet(output_parquet_path, index=False)
    return df


if __name__ == "__main__":
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
            "test",
        ],
    )

    process_dataset_to_parquet(
        dataset=train_dataset,
        data_dir="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/data",
        images_dir="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/images_train",
        output_parquet_path="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/train.parquet",
    )
    process_dataset_to_parquet(
        dataset=test_dataset,
        data_dir="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/data",
        images_dir="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/images_test",
        output_parquet_path="C:/Users/korea/workspace/datalake/prep/huggingface/kvp10k/test.parquet",
    )

    # example = test_dataset[0]
    # pdf_url = example["image_url"]
