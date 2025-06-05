# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
import json
import re
import sys
from PIL import Image
from pathlib import Path
import pandas as pd
from typing import List, Dict

from utils import save_df_as_jsonl, denormalize_bboxes


KIE_STRUCT_PROMPT = "Extract the following information from the image. Return the result in the following structured JSON format (formatted with newlines and 2-space indentation), filling in both <|value|> and <|bbox|>:"


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
            bbox_key="<|bbox|>",
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


class LayoutDataExporter(object):
    def _elements_to_label(
        self,
        elements: List[Dict[str, str]],
        width: int,
        height: int,
        indent: int = 0,
    ):
        elements = [
            {k: i.get(k) for k in ["type", "bbox"]}
            for i in elements
        ]
        label = json.dumps(
            elements,
            ensure_ascii=False,
            indent=indent,
        )
        return denormalize_bboxes(
            label,
            width=width,
            height=height,
            bbox_key="bbox",
        )

    def export(
        self,
        df: pd.DataFrame,
        datalake_dir: str,
        save_path: str,
        indent: int = 0,
    ) -> None:
        df_copied = df.copy()

        df_copied["label"] = df_copied.apply(
            lambda x: self._elements_to_label(
                json.loads(
                    x["label"],
                )["elements"],
                width=x["width"],
                height=x["height"],
                indent=indent,
            ),
            axis=1,
        )
        df_copied["image_path"] = df_copied["image_path"].apply(
            lambda x: (Path(datalake_dir) / x).as_posix(),
        )
        df_copied["query"] = "Parse the reading order of this document."

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

    df = client.retrieve_with_existing_cols(
        datasets=["office_docs"],
    )
    exporter = LayoutDataExporter()
    exporter.export(
        df=df,
        datalake_dir="/mnt/AI_NAS/datalake",
        save_path="/home/eric/workspace/Qwen-SFT/office_docs.jsonl",
        indent=0,
    )