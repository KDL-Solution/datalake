import hashlib
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
        )  # or "JPEG", depending on consistency needed
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


def extract_dimensions_batch(
    examples: Dict[str, List[Any]],  # batched format: column-wise dict
    images_dir: str,
) -> Dict[str, List[str]]:
    images_dir = Path(images_dir)

    image_paths = []
    widths = []
    heights = []
    labels = []

    for image in examples["image"]:
        width, height = image.size
        image_hash = sha256_pil_image(image)
        image_path = (images_dir / image_hash[:2] / image_hash).with_suffix(".jpeg")
        
        if not image_path.exists():  # Only save if it doesn't exist
            image_path.parent.mkdir(parents=True, exist_ok=True)
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
        "label": labels
    }


def export_to_parquet(
    dataset: Dict[str, Any],
    images_dir: str,
    parquet_path: str,
    batch_size=16,
) -> None:
    parquet_path = Path(parquet_path)

    dataset = dataset.map(
        partial(
            extract_dimensions_batch,
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


if __name__ == "__main__":
    NAS_ROOT = Path("/mnt/AI_NAS/datalake")

    provider = "huggingface"
    dataset = "pubtabnet_otsl"
    data_dir = NAS_ROOT / f"source/provider={provider}/{dataset}"

    train_dataset, val_dataset = load_dataset(
        "parquet",
        data_files={
            "train": (data_dir / "data/train-*.parquet").as_posix(),
            "val": (data_dir / "data/val-*.parquet").as_posix(),
        },
        split=[
            "train",
            "val",
            # "train[:4]",
            # "val[:4]",
        ],
    )

    script_dir = Path(__file__).resolve().parent
    images_dir = script_dir / "images"
    export_to_parquet(
        dataset=train_dataset,
        images_dir=images_dir,
        parquet_path=(script_dir / "train.parquet").as_posix(),
    )
    export_to_parquet(
        dataset=val_dataset,
        images_dir=images_dir,
        parquet_path=(script_dir / "val.parquet").as_posix(),
    )
