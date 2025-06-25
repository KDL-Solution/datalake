import pandas as pd
from datasets import Dataset
from sklearn.model_selection import train_test_split

# import sys
# sys.path.insert(0, "/home/eric/workspace/datalake/")
from export.utils import (
    save_df_as_jsonl,
    user_prompt_dict,
    extract_otsl,
)


class TableImageExporter(object):
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


class TableImageExporterForDataset:
    def __init__(
        self,
        batch_size: int = 4,
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
        # label이 `None`인 샘플 제거:
        dataset = dataset.filter(
            lambda x: x["label"] is not None,
            desc="Removing empty labels",
        )
        dataset = dataset.filter(
            lambda x: "\\" not in x["label"] if x["label"] is not None else False,
            desc="Filtering LaTeX-style labels",
        )

        # user prompt 추가:
        dataset = dataset.map(
            lambda x: {**x, "query": user_prompt},
            desc="Adding user prompt",
        )

        dataset.to_json(
            save_path,
            lines=True,
            force_ascii=False,
        )


def main(
    test_size: float = 0.05,
) -> None:
    from core.datalake import DatalakeClient

    exporter = TableImageExporter()
    manager = DatalakeClient()

    search_results = manager.search_catalog(
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

    df = manager.to_pandas(
        search_results,
    )
    condition = df["dataset"].str.contains("_train")
    df_train = df[condition]
    df_val = df[~condition]
    df_val, df_test = train_test_split(
        df_val,
        test_size=test_size,
    )
    print(len(df_train), len(df_val), len(df_test))

    # dataset = manager.to_dataset(
    #     search_results,
    #     absolute_paths=True,
    # )
    # dataset_train = dataset.filter(
    #     lambda x: "_train" in x["dataset"],
    # )
    # dataset_val_test = dataset.filter(
    #     lambda x: "_train" not in x["dataset"],
    # )

    # indices = list(range(len(dataset_val_test)))
    # val_indices, test_indices = train_test_split(
    #     indices,
    #     test_size=test_size,
    # )
    # dataset_val = dataset_val_test.select(val_indices)
    # dataset_test = dataset_val_test.select(test_indices)
    # print(len(dataset_train), len(dataset_val), len(dataset_test))

    exporter.export(
        df=df_train,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_train.jsonl",
    )
    exporter.export(
        df=df_val,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_val.jsonl",
    )
    exporter.export(
        df=df_test,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl_test.jsonl",
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
