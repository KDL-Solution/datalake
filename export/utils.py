import pandas as pd


def convert_df_to_jsonl(
    df: pd.DataFrame,
    save_path: str,
) -> None:
    df.to_json(
        save_path,
        orient="records",
        lines=True,
        force_ascii=False,  # 특수문자 처리 때문.
    )
