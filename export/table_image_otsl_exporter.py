import regex
import pandas as pd
from typing import List
from sklearn.model_selection import train_test_split

from core.datalake import DatalakeClient
# import sys
# sys.path.insert(0, "/home/eric/workspace/datalake/")
from export.utils import (
    save_df_as_jsonl,
    user_prompt_dict,
)


class TableImageOTSLExporter(object):
    def __init__(
        self,
    ):
        self.otsl_pattern = regex.compile(
            r"<otsl>.*?</otsl>",
            regex.DOTALL,
        )

    def extract_otsl(
        self,
        text: str,
    ) -> str:
        """Find the content inside <otsl>...</otsl>
        """
        if not isinstance(text, str):
            return None
        match = regex.search(
            self.otsl_pattern,
            text,
        )
        if match:
            return match.group(0).strip()
        else:
            return None

    def export(
        self,
        df: pd.DataFrame,
        save_path: str,
        user_prompt: str = user_prompt_dict["table"],
    ) -> None:
        df_copy = df.copy()

        df_copy["label"] = df_copy["label"].swifter.apply(
            lambda x: self.extract_otsl(x),
        )
        df_copy = df_copy[df_copy["label"].notna()]
        # 수식 등 제거:
        df_copy = df_copy[
            ~df_copy["label"].str.contains(
                r"\\",
                # regex=True,
                na=False,
            )
        ]
        df_copy["query"] = user_prompt

        save_df_as_jsonl(
            df=df_copy,
            jsonl_path=save_path,
        )


# def count_total_tags(
#     text: str,
# ):
#     if not isinstance(text, str):
#         return 0
#     return len(regex.findall(r"</?[\p{L}_]+>", text))


def main(
    user_id: str,
    # test_size: float = 0.0005,  # 0.5%
    # val_size: float = 0.025,  # 2.5%
    test_size: float = 0.001,  # 1%
    val_size: float = 0.04,  # 4%
) -> None:
    exporter = TableImageOTSLExporter()
    client = DatalakeClient(
        user_id=user_id,
    )

    search_results = client.search(
        variants=[
            "table_image_otsl",
        ]
    )
    # search_results = search_results.head(2_000)
    print(
        search_results.groupby(
            [
                "provider",
                "dataset",
            ],
        ).size()
    )

    df = client.to_pandas(
        search_results,
        absolute_paths=True,
    )

    df_train_val, df_test = train_test_split(
        df,
        test_size=test_size,
    )
    df_train, df_val = train_test_split(
        df_train_val,
        test_size=val_size,
    )
    print(f"Train: {len(df_train)}, Val: {len(df_val)}, Test: {len(df_test)}")

    exporter.export(
        df=df_train,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl-train.jsonl",
    )
    exporter.export(
        df=df_val,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl-val.jsonl",
    )
    exporter.export(
        df=df_test,
        save_path="/home/eric/workspace/datalake/export/data/table_image_otsl-test.jsonl",
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )