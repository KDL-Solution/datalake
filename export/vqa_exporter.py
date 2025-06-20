from pathlib import Path
import pandas as pd

from export.utils import save_df_as_jsonl


class VQAExporter(object):
    def export(
        self,
        df: pd.DataFrame,
        datalake_dir: str,
        save_path: str,
        multiturn: bool = True,
    ) -> None:
        df_copied = df.copy()

        df_copied["path"] = df_copied["path"].apply(
            lambda x: (Path(datalake_dir) / x).as_posix(),
        )

        if multiturn:
            df_copied = df_copied.groupby(
                by=["path"],
            ).agg(list).reset_index()

        save_df_as_jsonl(
            df=df_copied,
            jsonl_path=save_path,
        )


if __name__ == "__main__":
    # import sys
    # sys.path.insert(0, '/home/eric/workspace/datalake/')
    from managers.datalake_client import DatalakeClient

    exporter = VQAExporter()
    manager = DatalakeClient()
