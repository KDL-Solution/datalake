import regex
import numpy as np
from datasets import Dataset
from typing import List
from transformers import AutoProcessor, PreTrainedTokenizerBase

# import sys
# sys.path.insert(0, "/home/eric/workspace/datalake/")
from core.datalake import DatalakeClient
from export.utils import (
    EXPORT_DATA_DIR,
    user_prompt_dict,
    save_dataset_as_jsonl,
)


class Exporter:
    def __init__(
        self,
        batch_size: int = 64,
        num_procs: int = 16,
    ):
        self.batch_size = batch_size
        self.num_procs = num_procs

    def export(
        self,
        dataset: Dataset,
        save_path: str,
        user_prompt: str = user_prompt_dict["table"],
        keep_columns: List[str] = [
            "path",
            "query",
            "label",
        ],
    ) -> None:

        # user prompt 추가:
        dataset = dataset.map(
            lambda x: {
                "query": [user_prompt] * len(x["label"]),
            },
            batched=True,
            batch_size=self.batch_size,
            num_proc=self.num_procs,
            desc="Adding user prompt",
        )

        dataset = dataset.remove_columns(
            [col for col in dataset.column_names if col not in keep_columns]
        )
        dataset.to_json(
            save_path,
            lines=True,
            force_ascii=False,
        )


def main(
    user_id: str,
    test_size: float = 0.001,  # 1%
    val_size: float = 0.04,  # 4%
    model: str = None,
    batch_size: int = 1_024,
    num_procs: int = 16,
) -> None:
    exporter = Exporter(
        batch_size=batch_size,
        num_procs=num_procs,
    )
    # client = DatalakeClient(
    #     user_id=user_id,
    # )

    # search_results = client.search(
    #     variants=[
    #         "table_image_otsl",
    #     ],
    #     output_format="dataset",
    # )
    # print(
    #     search_results.to_pandas().groupby(
    #         [
    #             "provider",
    #             "dataset",
    #         ],
    #     ).size()
    # )

    from datasets import load_dataset
    dataset = load_dataset(
        "json",
        data_files="/home/eric/workspace/datalake/export/data/table_image_otsl.jsonl",
        split="train",
    )
    dataset[0]["path"]

    dataset_train_val, dataset_test = dataset.train_test_split(
        test_size=test_size,
    ).values()
    dataset_train, dataset_val = dataset_train_val.train_test_split(
        test_size=val_size,
    ).values()
    print(f"Train: {len(dataset_train)}, Val: {len(dataset_val)}, Test: {len(dataset_test)}")

    save_dataset_as_jsonl(
        dataset_train,
        jsonl_path=(EXPORT_DATA_DIR / "table_image_otsl-train.jsonl").as_posix(),
    )
    save_dataset_as_jsonl(
        dataset_val,
        jsonl_path=(EXPORT_DATA_DIR / "table_image_otsl-val.jsonl").as_posix()
    )
    save_dataset_as_jsonl(
        dataset_test,
        jsonl_path=(EXPORT_DATA_DIR / "table_image_otsl-test.jsonl").as_posix()
    )

    length_quantile = np.quantile(
        dataset_train["length"],
        0.5,
    )
    dataset_train = dataset_train.filter(
        lambda x: x["length"] > length_quantile,
    )
    print(f"Train: {len(dataset_train)}, Val: {len(dataset_val)}, Test: {len(dataset_test)}")
    save_dataset_as_jsonl(
        dataset_train,
        jsonl_path=(EXPORT_DATA_DIR / "table_image_otsl-train-length_quantile=0.5.jsonl").as_posix(),
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
