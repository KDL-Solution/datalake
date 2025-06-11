import datasets
from datasets import load_dataset
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List
from functools import partial
from PIL import Image

from utils import DATALAKE_DIR, get_safe_image_hash_from_pil

SYMBOLS_TO_FILTER = [
    "<b>",
    "</b>",
    "<i>",
    "</i>",
]


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
    doctags = "".join(label)
    if "<otsl>" not in doctags:
        doctags = "<otsl>" + doctags
    if "</otsl>" not in doctags:
        doctags += "</otsl>"
    return doctags


def save_images_and_generate_labels(
    examples: Dict[str, List[Any]],
    images_dir: str,
) -> Dict[str, List[str]]:
    images_dir = Path(images_dir)

    image_paths = []
    widths = []
    heights = []
    labels = []

    for image in examples["image"]:
        try:
            if isinstance(image, dict):
                image = Image.open(BytesIO(image["bytes"])).convert("RGB")
            assert isinstance(image, Image.Image)

        except OSError:
            print(f"[Warning] Corrupt image.")
            # Create white canvas instead (default size 256x256)
            image = Image.new(
                "RGB",
                (256, 256),
                color="white",
            )

        width, height = image.size
        image_hash = get_safe_image_hash_from_pil(
            image,
        )
        image_path = Path(f"{images_dir / image_hash[: 2] / image_hash}.jpg")
        if not image_path.exists():
            image_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            image.save(
                image_path,
                format="JPEG",
            )

        image_paths.append(str(image_path))
        widths.append(width)
        heights.append(height)

    for example in zip(*examples.values()):
        example_dict = dict(zip(examples.keys(), example))
        labels.append(generate_doctags(example_dict))
    return {
        "image_path": image_paths,
        "width": widths,
        "height": heights,
        "label": labels,
    }


def export_to_parquet(
    dataset: Dict[str, Any],
    images_dir: str,
    parquet_path: str,
    batch_size: int = 32,
) -> None:
    parquet_path = Path(parquet_path)

    dataset = dataset.cast_column(
        "image",
        datasets.Image(
            decode=False,
        ),
    )  # Lazy decoding으로 .map() 전에 PIL로 이미지가 로드되지 않도록 함.
    dataset = dataset.map(
        partial(
            save_images_and_generate_labels,
            images_dir=images_dir,
        ),
        batched=True,
        batch_size=batch_size,
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


def main(
    dataset: str,
    save_dir: str,
    datalake_dir: str = DATALAKE_DIR,
) -> None:
    data_dir = Path(datalake_dir) / f"source/provider=huggingface/dataset={dataset}"
    train_dataset, val_dataset = load_dataset(
        "parquet",
        data_files={
            "train": (data_dir / "data/train-*.parquet").as_posix(),
            "val": (data_dir / "data/val-*.parquet").as_posix(),
        },
        split=[
            "train",
            "val",
        ],
    )

    save_dir = Path(save_dir)
    export_to_parquet(
        dataset=train_dataset,
        images_dir=save_dir / "images_train",
        parquet_path=(save_dir / "train.parquet").as_posix(),
    )
    export_to_parquet(
        dataset=val_dataset,
        images_dir=save_dir / "images_val",
        parquet_path=(save_dir / "val.parquet").as_posix(),
    )
