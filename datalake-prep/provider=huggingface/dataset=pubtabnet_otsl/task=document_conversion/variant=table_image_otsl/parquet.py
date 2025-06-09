import hashlib
import datasets
from datasets import load_dataset
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List
from functools import partial
from PIL import Image

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
        image.save(
            buffer,
            format="JPEG",
        )
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


def save_images_and_generate_labels(
    examples: Dict[str, List[Any]],
    images_dir: str,
) -> Dict[str, List[str]]:
    images_dir = Path(images_dir)

    image_paths = []
    widths = []
    heights = []
    labels = []
    exclude_indices = set()

    for idx, image in enumerate(examples["image"]):
        try:
            if isinstance(image, dict):
                image = Image.open(BytesIO(image["bytes"])).convert("RGB")
            assert isinstance(image, Image.Image)

            width, height = image.size
            image_hash = sha256_pil_image(image)
            image_path = (images_dir / image_hash[:2] / image_hash).with_suffix(".jpeg")

            if not image_path.exists():  # Only save if it doesn't exist.
                image_path.parent.mkdir(parents=True, exist_ok=True)
                image.save(
                    image_path,
                    format="JPEG",
                )

            image_paths.append(str(image_path))
            widths.append(width)
            heights.append(height)

        except OSError:
            exclude_indices.add(idx)
            print(f"Skipping corrupt image of index {idx}")
            continue

    for idx in range(len(examples["image"])):
        if idx in exclude_indices:
            continue

        example_dict = {k: examples[k][idx] for k in examples}
        labels.append(generate_doctags(example_dict))
    return {
        "image_path": image_paths,
        "width": widths,
        "height": heights,
        "label": labels
    }


def export_to_parquet(
    dataset: Dict[str, Any],
    images_dir: str,
    parquet_path: str,
    batch_size=32,
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


def main():
    from utils import DATALAKE_DIR

    script_dir = Path(__file__).resolve().parent
    data_dir = DATALAKE_DIR / f"source/{script_dir.parents[2].stem}/{script_dir.parents[1].stem}"

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

    export_to_parquet(
        dataset=train_dataset,
        images_dir=script_dir / "images_train",
        parquet_path=(script_dir / "train.parquet").as_posix(),
    )
    export_to_parquet(
        dataset=val_dataset,
        images_dir=script_dir / "images_val",
        parquet_path=(script_dir / "val.parquet").as_posix(),
    )


if __name__ == "__main__":
    # datalake/datalake-prep에서 실행하시오: e.g., `python -m provider=huggingface.dataset=pubtabnet_otsl.task=document_conversion.variant=table_image_otsl.parquet`.
    main()
