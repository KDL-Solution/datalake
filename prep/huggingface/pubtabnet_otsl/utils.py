from datasets import load_dataset
from pathlib import Path
from typing import Dict, Any

from prep.utils import DATALAKE_DIR
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


def upload(
    dataset: Dict[str, Any],
    dataset_name: str,
    batch_size: int = 32,
    num_procs: int = 16,
) -> None:
    manager = DatalakeClient()

    # task=raw가 업로드되어 있지 않다면 업로드.
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
    datalake_dir: str = DATALAKE_DIR,
) -> None:
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

    upload(
        dataset=train_dataset,
        dataset_name=f"{dataset_name}_train",
    )
    upload(
        dataset=val_dataset,
        dataset_name=f"{dataset_name}_val",
    )
