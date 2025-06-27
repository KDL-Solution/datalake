import numpy as np
from datasets import Dataset
from typing import List
from transformers import AutoProcessor, PreTrainedTokenizerBase

from core.datalake import DatalakeClient
from export.utils import (
    user_prompt_dict,
    extract_otsl,
)


class TableImageOTSLExporter:
    def __init__(
        self,
        quantiles: List[float] = [0.9, 0.95, 0.98, 0.99, 0.999, 1.],
        batch_size: int = 64,
        num_procs: int = 16,
    ):
        self.quantiles = quantiles
        self.batch_size = batch_size
        self.num_procs = num_procs

    def export(
        self,
        dataset: Dataset,
        save_path: str,
        tokenizer: PreTrainedTokenizerBase = None,
        token_length_quantile: float = 0.,
        user_prompt: str = user_prompt_dict["table"],
    ) -> None:
        # label이 `None`인 샘플 제거:
        print(f"# original samples: {len(dataset):,}")
        dataset = dataset.filter(
            lambda x: x["label"] is not None,
            desc="Removing empty labels",
        )
        print(f"# samples after removing empty labels: {len(dataset):,}")

        token_length_thresh = 0
        if tokenizer is not None and token_length_quantile > 0.:
            dataset = dataset.map(
                lambda x: {
                    "token_length": [
                        len(i)
                        for i in tokenizer(x["label"])["input_ids"]
                    ],
                },
                batched=True,
                batch_size=self.batch_size,
                num_proc=self.num_procs,
            )
            token_lengths = np.array(dataset["token_length"])
            print(
                np.quantile(
                    token_lengths,
                    self.quantiles,
                )
            )
            token_length_thresh = np.quantile(
                token_lengths,
                token_length_quantile,
            )
            dataset = dataset.filter(
                lambda x: x["token_length"] > token_length_thresh,
            )
        print(f"# samples after filtering using token length threshold {token_length_thresh:,}: {len(dataset):,}")

        dataset = dataset.map(
            lambda x: {
                "label": [
                    extract_otsl(
                        i,
                    ) for i in x["label"]
                ],
            },
            batched=True,
            batch_size=self.batch_size,
            num_proc=self.num_procs,
            desc="Extracting <otsl>...</otsl>",
        )
        dataset = dataset.filter(
            lambda x: "\\" not in x["label"],
            desc="Filtering LaTeX-style labels",
        )

        # user prompt 추가:
        dataset = dataset.map(
            lambda x: {
                **x,
                "query": user_prompt,
            },
            desc="Adding user prompt",
        )

        dataset.to_json(
            save_path,
            lines=True,
            force_ascii=False,
        )


def main(
    user_id: str,
    test_size: float = 0.0005,  # 0.5%
    val_size: float = 0.025,  # 2.5%
    token_length_quantile: float = 0.,
    model: str = None,
    quantiles: List[float] = [0.9, 0.95, 0.98, 0.99, 0.999, 1.],
    batch_size: int = 64,
    num_procs: int = 64,
) -> None:
    exporter = TableImageOTSLExporter(
        quantiles=quantiles,
        batch_size=batch_size,
        num_procs=num_procs,
    )
    client = DatalakeClient(
        user_id=user_id,
    )

    search_results = client.search(
        variants=[
            "table_image_otsl",
        ]
    )
    print(
        search_results.groupby(
            [
                "provider",
                "dataset",
            ],
        ).size()
    )
    # search_results=search_results.head(5000)

    dataset = client.to_dataset(
        search_results,
        absolute_paths=True,
    )
    dataset_train_val, dataset_test = dataset.train_test_split(
        test_size=test_size,
    ).values()
    dataset_train, dataset_val = dataset_train_val.train_test_split(
        test_size=val_size,
    ).values()
    print(f"Train: {len(dataset_train)}, Val: {len(dataset_val)}, Test: {len(dataset_test)}")

    processor = AutoProcessor.from_pretrained(
        model,
        use_fast=True,
        trust_remote_code=True,
        device_map="cpu",
    )
    exporter.export(
        dataset_train,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_train.jsonl",
        tokenizer=processor.tokenizer,
        token_length_quantile=token_length_quantile,
    )
    exporter.export(
        dataset_val,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_val.jsonl",
    )
    exporter.export(
        dataset_test,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_test.jsonl",
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
