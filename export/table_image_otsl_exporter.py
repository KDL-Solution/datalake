import regex
import pandas as pd
import numpy as np
from datasets import Dataset
from typing import List
from sklearn.model_selection import train_test_split
from transformers import AutoProcessor

from core.datalake import DatalakeClient

# import sys
# sys.path.insert(0, "/home/eric/workspace/datalake/")
from export.utils import (
    save_df_as_jsonl,
    user_prompt_dict,
    extract_otsl,
)


class TableImageOTSLExporter(object):
    def export(
        self,
        df: pd.DataFrame,
        save_path: str,
        user_prompt: str = user_prompt_dict["table"],
        multiturn: bool = False,
    ) -> None:
        df_copied = df.copy()

        df_copied = df_copied[df_copied["label"].notna()]
        # `"<otsl>"`로 시작해서 `"</otsl>"`로 끝나는 행만 필터:
        df_copied["label"] = df_copied["label"].apply(
            lambda x: extract_otsl(x),
        )
        # 수식 등을 제외:
        df_copied = df_copied[~
            df_copied["label"].str.contains(
                r"\\",
                regex=True,
                na=False,
            )
        ]

        df_copied["query"] = user_prompt

        if multiturn:
            df_copied = df_copied.groupby(
                by=["path"],
            ).agg(list).reset_index()

        save_df_as_jsonl(
            df=df_copied,
            jsonl_path=save_path,
        )


class TableImageOTSLExporterForDataset:
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
    ) -> None:
        # label이 `None`인 샘플 제거:
        dataset = dataset.filter(
            lambda x: x["label"] is not None,
            desc="Removing empty labels",
        )
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


def count_total_tags(
    text: str,
):
    if not isinstance(text, str):
        return 0
    return len(regex.findall(r"</?[\p{L}_]+>", text))


def main(
    test_size: float = 0.0005,  # 0.5%
    val_size: float = 0.025,  # 2.5%
    # quantile: float = 0.,
    model: str = None,
    quantiles: List[float] = [0.9, 0.95, 0.98, 0.99, 0.999, 1.],
    batch_size: int = 64,
    num_procs: int = 32,
) -> None:
    # exporter = TableImageOTSLExporter()
    exporter = TableImageOTSLExporterForDataset()
    manager = DatalakeClient()

    search_results = manager.search(
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

    # df = manager.to_pandas(
    #     search_results,
    # )

    # if quantile > 0.:
    #     df["tag_count"] = df["label"].progress_apply(
    #         count_total_tags,
    #     )
    #     threshold = df["tag_count"].quantile(quantile)
    #     print(f"Threshold: {threshold}")
    #     df = df[df["tag_count"] >= threshold]

    dataset = manager.to_dataset(
        search_results
    )

    if model is not None:
        processor = AutoProcessor.from_pretrained(
            model,
            trust_remote_code=True,
            device_map="cpu",
        )
        dataset = dataset.map(
            lambda x: {
                "token_length": [len(i) for i in processor.tokenizer(x["label"])["input_ids"]]
            },
            batched=True,
            batch_size=batch_size,
            num_proc=num_procs,
        )
        token_lengths = np.array(dataset["token_length"])
        print(
            np.quantile(
                token_lengths,
                quantiles,
            )
        )

    # df_train_val, df_test = train_test_split(
    #     df,
    #     test_size=test_size,
    # )
    # df_train, df_val = train_test_split(
    #     df_train_val,
    #     test_size=val_size,
    # )
    # print(f"Train: {len(df_train)}, Val: {len(df_val)}, Test: {len(df_test)}")
    dataset_train_val, dataset_test = dataset.train_test_split(
        test_size=test_size,
    )
    dataset_train, dataset_val = dataset_train_val.train_test_split(
        test_size=val_size,
    )
    print(f"Train: {len(dataset_train)}, Val: {len(dataset_val)}, Test: {len(dataset_test)}")

    exporter.export(
        # df=df_train,
        dataset_train,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_train.jsonl",
    )
    exporter.export(
        # df=df_val,
        dataset_val,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_val.jsonl",
    )
    exporter.export(
        # df=df_test,
        dataset_test,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_test.jsonl",
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
