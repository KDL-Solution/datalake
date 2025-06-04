# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
from PIL import Image
from pathlib import Path
import json

import sys
import pandas as pd
sys.path.insert(0, "/home/eric/workspace/datalake/athena")
from athena.src.core.athena_client import AthenaClient


def to_chat_format(
    image_path,
    user_prompt,
    system_prompt,
):
    return {
        "messages": [
            {
                "role": "user",
                "content": f"<image>{user_prompt}",
            },
            {
                "role": "assistant",
                "content": system_prompt,
            },
        ],
        "images": [
            image_path,
        ],
    }


def save_df_as_jsonl(
    df: pd.DataFrame,
    output_path: str,
) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        for row in df.itertuples(index=False):
            json_obj = to_chat_format(
                image_path=row.image_path,
                user_prompt=row.query,
                system_prompt=row.label,
            )
            f.write(json.dumps(json_obj, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    client = AthenaClient()

    sql = """
    SELECT *
    FROM catalog
    WHERE dataset = 'pubtabnet_otsl_test'
    """
    df = client.execute_query(
        sql=sql
    )
    df

    DATALAKE = Path("/mnt/AI_NAS/datalake")
    df["image_path"] = df["image_path"].apply(lambda x: (DATALAKE / x).as_posix())
    df = df[["image_path", "query", "label"]]
    df.iloc[0].to_dict()

    save_df_as_jsonl(
        df=df,
        output_path="/home/eric/workspace/Qwen-SFT/vis_qa.jsonl",
    )
