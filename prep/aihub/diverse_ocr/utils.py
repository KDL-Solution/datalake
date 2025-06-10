import zipfile
import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from PIL import Image, ImageEnhance
import numpy as np
import cv2
from natsort import natsorted

from utils import DATALAKE_DIR, get_safe_image_hash_from_pil


def unzip_all_zips_in_dir(
    target_dir: str,
    save_dir: str,  # 추가: 압축 해제할 루트 디렉토리
) -> None:
    target_dir = Path(target_dir).resolve()
    save_dir = Path(save_dir).resolve()
    save_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 모든 하위 폴더를 포함해서 .zip 파일 찾기
    for zip_path in target_dir.rglob("*.zip"):
        if "필기체" not in zip_path.stem:
            continue

        # zip 이름만 가져와서 폴더명 생성 (확장자 제거)
        extract_subdir = save_dir / zip_path.stem
        extract_subdir.mkdir(parents=True, exist_ok=True)
        print(f"📦 Extracting {zip_path} -> {extract_subdir}")

        # 압축 해제
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_subdir)


def get_image_path(
    json_path: str,
) -> str:
    return json_path.replace("[라벨]", "[원천]").replace(".json", ".jpg")


def process_image(
    image: Image.Image,
    margin: int = 0,
    contrast_enhance: float = 1.3,
) -> Image.Image:
    # Convert PIL to grayscale NumPy array
    img = np.array(image.convert("L"))

    # Adaptive thresholding
    img_binary = cv2.adaptiveThreshold(
        img,
        maxValue=255,
        adaptiveMethod=cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        thresholdType=cv2.THRESH_BINARY,
        blockSize=11,
        C=2
    )

    # Invert to find black regions as white
    img_inv = 255 - img_binary

    # Find non-zero (was black in original)
    coords = cv2.findNonZero(img_inv)

    if coords is None:
        # No black pixels found, return original
        return image

    # Bounding box
    x, y, w, h = cv2.boundingRect(coords)

    # Add margin
    x = max(x - margin, 0)
    y = max(y - margin, 0)
    x2 = min(x + w + 2 * margin, img.shape[1])
    y2 = min(y + h + 2 * margin, img.shape[0])

    # Crop original image (PIL, so convert back to array)
    full_img_np = np.array(image)
    cropped_np = full_img_np[y:y2, x:x2]
    image = Image.fromarray(cropped_np)
    image = image.convert("L")  # 흑백으로 변환.

    enhancer = ImageEnhance.Contrast(image)
    return enhancer.enhance(contrast_enhance)


def main(
    ocr_unit: str,
    data_dir: str,
    parquet_dir: str,
    images_dir: str,
    datalake_dir: str = DATALAKE_DIR,
    unzip: bool = True,
    save_images: bool = True,
) -> None:
    if unzip:
        unzip_all_zips_in_dir(
            target_dir=Path(datalake_dir) / f"source/provider=aihub/dataset=diverse_ocr",
            save_dir=data_dir,
        )

    rows = []
    json_paths = [
        i for i in natsorted(list(Path(data_dir).glob("**/*.json")))
        if "필기체" in i.as_posix()
    ]
    for json_path in tqdm(json_paths):
        image_path = get_image_path(
            json_path.as_posix(),
        )
        image = Image.open(image_path).convert("RGB")
        image = process_image(
            image=image,
            contrast_enhance=1.3,
        )
        width, height = image.size
        image_hash = get_safe_image_hash_from_pil(
            image,
        )
        image_path = Path(images_dir) / image_hash[: 2] / f"{image_hash}.jpg"
        if save_images and not image_path.exists():
            image_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            image.save(
                image_path,
                format="JPEG",
            )

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        type_ = data["text"]["type"]
        if ocr_unit == "char" and type_ == "letter":
            text = data["text"]["letter"]["value"]
        if ocr_unit == "word" and type_ == "word":
            text = "".join([i["value"] for i in data["text"]["word"]])

        rows.append(
            {
                "image_path": image_path.as_posix(),
                "width": width,
                "height": height,
                "label": text,
            }
        )
        df = pd.DataFrame(rows)
    df.to_parquet(
        Path(parquet_dir) / "data.parquet",
        index=False,
    )
