from datasets import load_dataset, Dataset
from PIL import Image, ImageDraw
from io import BytesIO
import hashlib
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict, Any
from functools import partial
from datetime import datetime

SYMBOLS_TO_FILTER = [
    "<b>",
    "</b>",
    "<i>",
    "</i>",
]


def sha256_pil_image(
    image: Image.Image,
) -> str:
    h = hashlib.sha256()
    with BytesIO() as buffer:
        image.save(buffer, format="PNG")  # or "JPEG", depending on consistency needed
        h.update(buffer.getvalue())
    return h.hexdigest()


def generate_doctags(
    example: Dict[str, Any],
) -> str:
    otsl_tokens = example["otsl"]
    cells = example["cells"][0]
    contents = [
        "".join(
            [
                i for i in cell["tokens"] if i not in SYMBOLS_TO_FILTER
            ]
        )
        for cell in cells
    ]
    contents = [i for i in contents if i]

    rev_otsl_tokens = list(reversed(otsl_tokens))
    rev_contents = list(reversed(contents))
    label = []
    while rev_otsl_tokens and rev_contents:
        otsl_token = f"<{rev_otsl_tokens.pop()}>"
        label.append(otsl_token)
        if otsl_token == "<fcel>":
            content = rev_contents.pop()
            label.append(content)
    return "".join(label)


def extract_dimensions(
    example: Dict[str, Any],
    images_dir: str,
    save_images: bool = False,
) -> Dict[str, str]:
    images_dir = Path(images_dir)

    image = example["image"]
    width, height = image.size  # PIL.Image
    image_hash = sha256_pil_image(
        image,
    )
    image_path = (images_dir / image_hash[: 2] / image_hash).with_suffix(".png")
    if save_images and not image_path.exists():
        image_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        image.save(image_path)
    return {
        "image_path": str(image_path),
        "width": width,
        "height": height,
        "label": generate_doctags(
            example,
        )
    }


def export_to_parquet(
    dataset: Dict[str, Any],
    images_dir: str,
    parquet_path: str,
    save_images: bool = False,
) -> None:
    parquet_path = Path(parquet_path)

    dataset = dataset.map(
        partial(
            extract_dimensions,
            images_dir=images_dir,
            save_images=save_images,
        )
    )
    dataset = dataset.remove_columns(
        [
            col for col in dataset.column_names
            if col not in [
                "image_path",
                "width",
                "height",
                "label",
            ]
        ]
    )
    parquet_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    dataset.to_pandas().to_parquet(
        str(parquet_path),
        index=False,
    )


if __name__ == "__main__":
    provider = "huggingface"
    dataset_name = "ds4ds_pubtabnet_otsl"
    task = "document_conversion"
    variant = "table"

    today = datetime.today()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    
    # train_dataset, val_dataset = load_dataset(
    #     "ds4sd/PubTabNet_OTSL",
    #     split=[
    #         "train",
    #         "val",
    #     ],
    # )
    train_dataset, test_dataset = load_dataset(
        "parquet",
        data_files={
            "train": f"/mnt/AI_NAS/datalake/source/{provider}/{dataset_name}/data/train-*.parquet",
            "val": f"/mnt/AI_NAS/datalake/source/{provider}/{dataset_name}/data/val-*.parquet",
        },
        split=[
            "train",
            "val",
            # "train[:4]",
            # "val[:4]",
        ],
    )

    images_dir=f"/mnt/AI_NAS/datalake/catalog/{provider}/{dataset_name}/images"
    parquet_dir = f"/mnt/AI_NAS/datalake/catalog/{provider}/{dataset_name}/{task}/{variant}/{year}/{month}/{day}"
    export_to_parquet(
        dataset=train_dataset,
        images_dir=images_dir,
        parquet_path=str(Path(parquet_dir) / "train.parquet"),
    )
    export_to_parquet(
        dataset=test_dataset,
        images_dir=images_dir,
        parquet_path=str(Path(parquet_dir) / "test.parquet"),
    )
