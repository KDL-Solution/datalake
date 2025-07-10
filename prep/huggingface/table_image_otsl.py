import regex
from datasets import load_dataset
from pathlib import Path
from typing import Dict, Any

from prep.utils import DATALAKE_DIR
from datalake.core.client import DatalakeClient


class DocTagsGenerator(object):
    def __init__(
        self,
    ):
        self.otsl_pattern = regex.compile(
                r"<otsl>.*?</otsl>",
                regex.DOTALL,
        )
        self.filter_symbols = [
            "<b>",
            "</b>",
            "<i>",
            "</i>",
        ]

    def extract_otsl(
        self,
        text: str,
    ) -> str:
        if not isinstance(text, str):
            return None
        # Find the content inside <otsl>...</otsl>:
        match = regex.search(
            self.otsl_pattern,
            text,
        )
        if match:
            return match.group(0).strip()
        else:
            return None

    def generate(
        self,
        example: Dict[str, Any],
    ) -> str:
        otsl_tokens = example["otsl"]
        cells = example["cells"][0]
        contents = [
            "".join(
                [
                    i for i in cell["tokens"] if i not in self.filter_symbols
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

        doctags = self.extract_otsl(
            doctags,
        )
        return doctags


def upload(
    client: DatalakeClient,
    generator: DocTagsGenerator,
    dataset: Dict[str, Any],
    dataset_name: str,
    batch_size: int = 32,
    num_proc: int = 16,
) -> None:
    # task=raw가 업로드되어 있지 않다면 업로드.
    search_results = client.search(
        datasets=[
            dataset_name,
        ],
        tasks=[
            "raw",
        ],
    )
    if search_results.empty:
        _ = client.upload_raw(
            dataset,
            provider="huggingface",
            dataset=dataset_name,
            original_source="https://huggingface.co/datasets/ds4sd/PubTabNet_OTSL",
            overwrite=True,
        )

        job_id = client.trigger_processing()
        _ = client.wait_for_job_completion(
            job_id,
        )

    dataset = dataset.map(
        lambda batch: {
            **batch,
            "label": [
                generator.generate(
                    {
                        k: v[i] for k, v in batch.items()
                    },
                )
                for i in range(len(batch["image"]))
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
        desc="Generating DocTags",
    )

    _, _ = client.upload_task(
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

    job_id = client.trigger_processing()
    _ = client.wait_for_job_completion(
        job_id,
    )


def main(
    user_id: str,
    dataset_name: str,
    datalake_dir: str = DATALAKE_DIR,
) -> None:
    # data_dir = Path(datalake_dir) / f"archive/source/provider=huggingface/dataset={dataset_name}"
    # train_dataset, val_dataset = load_dataset(
    #     "parquet",
    #     data_files={
    #         "train": (data_dir / "data/train-*.parquet").as_posix(),
    #         "val": (data_dir / "data/val-*.parquet").as_posix(),
    #     },
    #     split=[
    #         "train",
    #         "val",
    #     ],
    # )
    train_dataset, val_dataset = load_dataset(
        "ds4sd/PubTabNet_OTSL",
        split=[
            "train",
            "val",
        ],
    )

    client = DatalakeClient(
        user_id=user_id
    )
    generator = DocTagsGenerator()
    upload(
        client=client,
        generator=generator,
        dataset=train_dataset,
        dataset_name=f"{dataset_name}_train",
    )
    upload(
        client=client,
        generator=generator,
        dataset=val_dataset,
        dataset_name=f"{dataset_name}_val",
    )
