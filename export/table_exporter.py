import pandas as pd
from sklearn.model_selection import train_test_split

from export.utils import save_df_as_jsonl


class TableImageExporter(object):
    def export(
        self,
        df: pd.DataFrame,
        save_path: str,
        multiturn: bool = True,
    ) -> None:
        df_copied = df.copy()

        if multiturn:
            df_copied = df_copied.groupby(
                by=["path"],
            ).agg(list).reset_index()

        save_df_as_jsonl(
            df=df_copied,
            jsonl_path=save_path,
        )


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '/home/eric/workspace/datalake/')
    from managers.datalake_client import DatalakeClient

    exporter = TableImageExporter()
    manager = DatalakeClient()

    search_result = manager.search_catalog(
        variants=[
            "table_image_otsl",
        ]
    )
    df = manager.to_pandas(
        search_result,
    )
    condition = df["dataset"].str.contains("_train")
    df_train = df[condition]
    df_val = df[~condition]
    df_val, df_test = train_test_split(
        df_val,
        test_size=0.1,
    )

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
