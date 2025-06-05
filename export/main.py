# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
from PIL import Image
from pathlib import Path
import json
import re

import sys
import pandas as pd


KIE_STRUCT_PROMPT = "Extract the following information from the image. Return the result in the following structured JSON format (formatted with newlines and 2-space indentation), filling in both <|value|> and <|bbox|>:"


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
    save_path: str,
) -> None:
    with open(save_path, "w", encoding="utf-8") as f:
        for row in df.itertuples(index=False):
            json_obj = to_chat_format(
                image_path=row.image_path,
                user_prompt=row.query,
                system_prompt=row.label,
            )
            f.write(json.dumps(json_obj, ensure_ascii=False) + "\n")


def denormalize_bboxes(
    json_str: str,
    width: int,
    height: int,
) -> str:
    def replacer(
        match,
    ):
        bbox = eval(match.group(1))  # e.g., [0.1, 0.2, 0.3, 0.4]
        x1, y1, x2, y2 = bbox
        abs_bbox = [
            round(x1 * width),
            round(y1 * height),
            round(x2 * width),
            round(y2 * height)
        ]
        return f'"<|bbox|>": {abs_bbox}'
    return re.sub(
        r'"<\|bbox\|>":\s*(\[[^\]]+\])',
        replacer,
        json_str,
    )


class KIEDataExporter(object):
    def _blank_value_and_bbox(
        self,
        json_str: str,
        indent: int = 0,
    ) -> str:
        def recursively_blank(d):
            if isinstance(d, dict):
                return {
                    k: (
                        "" if k == "<|value|>" else
                        [] if k == "<|bbox|>" else
                        recursively_blank(v)
                    )
                    for k, v in d.items()
                }
            return d

        data = json.loads(json_str)
        blanked = recursively_blank(data)
        return json.dumps(
            blanked,
            ensure_ascii=False,
            indent=indent,
        )

    def _process_kie_label(
        self,
        label: str,
        width: int,
        height: int,
        indent: int = 0,
    ) -> str:
        label_dict = json.loads(label)
        label_str = json.dumps(
            label_dict,
            indent=indent,
            ensure_ascii=False,
        )
        return denormalize_bboxes(
            label_str,
            width=width,
            height=height,
        )

    def _make_target_schema(
        self,
        label: str,
        indent: int = 0,
    ) -> str:
        label_dict = json.loads(label)
        label_str = json.dumps(
            label_dict,
            indent=indent,
            ensure_ascii=False,
        )
        return self._blank_value_and_bbox(
            label_str,
            indent=indent,
        )

    def export(
        self,
        df: pd.DataFrame,
        datalake_dir: str,
        save_path: str,
        indent: int = 0,
    ) -> None:
        df_copied = df.copy()

        df_copied["image_path"] = df_copied["image_path"].apply(
            lambda x: (Path(datalake_dir) / x).as_posix(),
        )
        df_copied["query"] = df_copied.apply(
            lambda x: KIE_STRUCT_PROMPT + "\n" + self._make_target_schema(
                x["label"],
                indent=indent,
            ),
            axis=1,
        )
        df_copied["label"] = df_copied.apply(
            lambda x: self._process_kie_label(
                label=x["label"],
                width=x["width"],
                height=x["height"],
                indent=indent,
            ),
            axis=1,
        )
        save_df_as_jsonl(
            df=df_copied,
            save_path=save_path,
        )


if __name__ == "__main__":
    sys.path.insert(0, "/home/eric/workspace/datalake/")
    from athena.src.core.athena_client import AthenaClient

    client = AthenaClient()

    df = client.retrieve_with_existing_cols(
        tasks=["vqa"],
        variants=["kie_struct"],
        datasets=["funsd_plus"],
    )

    exporter = KIEDataExporter()
    exporter.export(
        df=df,
        datalake_dir="/mnt/AI_NAS/datalake",
        save_path="/home/eric/workspace/Qwen-SFT/funsd_plus.jsonl",
        indent=0,
    )
