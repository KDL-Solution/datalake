import datasets
from datasets import load_dataset
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List
from functools import partial
from PIL import Image

from prep.utils import DATALAKE_DIR, get_safe_image_hash_from_pil
from core.datalake import DatalakeClient

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


# def save_images_and_generate_labels(
#     examples: Dict[str, List[Any]],
#     images_dir: str,
# ) -> Dict[str, List[str]]:
#     images_dir = Path(images_dir)

#     image_paths = []
#     widths = []
#     heights = []
#     labels = []

#     for image in examples["image"]:
#         try:
#             if isinstance(image, dict):
#                 image = Image.open(BytesIO(image["bytes"])).convert("RGB")
#             assert isinstance(image, Image.Image)

#         except OSError:
#             print(f"[Warning] Corrupt image.")
#             # Create white canvas instead (default size 256x256)
#             image = Image.new(
#                 "RGB",
#                 (256, 256),
#                 color="white",
#             )

#         width, height = image.size
#         image_hash = get_safe_image_hash_from_pil(
#             image,
#         )
#         image_path = Path(f"{images_dir / image_hash[: 2] / image_hash}.jpg")
#         if not image_path.exists():
#             image_path.parent.mkdir(
#                 parents=True,
#                 exist_ok=True,
#             )
#             image.save(
#                 image_path,
#                 format="JPEG",
#             )

#         image_paths.append(
#             Path(*image_path.parts[-2:]).as_posix()
#         )
#         widths.append(width)
#         heights.append(height)

#     for example in zip(*examples.values()):
#         example_dict = dict(zip(examples.keys(), example))
#         labels.append(generate_doctags(example_dict))
#     return {
#         "image_path": image_paths,
#         "width": widths,
#         "height": heights,
#         "label": labels,
#     }
# def generate_labels(
#     examples: Dict[str, List[Any]],
# ) -> Dict[str, List[str]]:
    # new_labels = [generate_doctags(i) for i in examples["label"]]
    # examples["label"] = new_labels
    # examples["label"] = [generate_doctags(i) for i in examples["label"]]
    # return examples


def upload(
    dataset: Dict[str, Any],
    dataset_name: str,
    # images_dir: str,
    # parquet_path: str,
    batch_size: int = 32,
    num_procs: int = 16,
) -> None:
    manager = DatalakeClient()

    # task=raw가 업로드되어 있지 않다면 업로드.
    # dataset_name = "pubtables_otsl_v1_1_val"
    search_results = manager.search_catalog(
        datasets=[
            dataset_name,
        ],
        tasks=[
            "raw",
        ],
    )
    if search_results.empty:
        _, _ = manager.upload_raw_data(
            data_file=dataset,
            provider="huggingface",
            dataset=dataset_name,
        )

        job_id = manager.trigger_nas_processing()
        manager.wait_for_job_completion(
            job_id,
        )
        manager.build_catalog_db(
            force_rebuild=True,
        )

    # parquet_path = Path(parquet_path)

    # dataset = dataset.cast_column(
    #     "image",
    #     datasets.Image(
    #         decode=False,
    #     ),
    # )  # Lazy decoding으로 .map() 전에 PIL로 이미지가 로드되지 않도록 함.
    dataset = dataset.map(
        lambda batch: {
            **batch,
            "label": [
                generate_doctags(
                    {
                        k: v[i] for k, v in batch.items()
                    },
                )
                for i in range(len(batch["image"]))
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_procs,
        desc="Generating DocTags",
    )

    _, _ = manager.upload_task_data(
        data_file=dataset,
        provider="huggingface",
        dataset=dataset_name,
        task="document_conversion",
        variant="table_image_otsl",
        meta={
            "lang": "en",
            "src": "real",
            "mod": "table",
        },
        overwrite=True,
    )

    job_id = manager.trigger_nas_processing()
    manager.wait_for_job_completion(
        job_id,
    )
    manager.build_catalog_db(
        force_rebuild=True,
    )


def main(
    dataset_name: str,
    # save_dir: str,
    datalake_dir: str = DATALAKE_DIR,
) -> None:
    # dataset = "pubtables_otsl_v1_1"
    data_dir = Path(datalake_dir) / f"archive/source/provider=huggingface/dataset={dataset_name}"
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


    # save_dir = Path(save_dir)
    # upload(
    #     dataset=train_dataset,
    #     dataset_name=f"{dataset_name}_train",
    #     # images_dir=save_dir / "images_train",
    #     # parquet_path=(save_dir / "train.parquet").as_posix(),
    # )
    upload(
        dataset=val_dataset,
        dataset_name=f"{dataset_name}_val",
        # images_dir=save_dir / "images_val",
        # parquet_path=(save_dir / "val.parquet").as_posix(),
    )
