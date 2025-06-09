# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
import sys
from pathlib import Path
import pandas as pd
from typing import List, Dict

# sys.path.insert(0, "/home/eric/workspace/datalake/")
from export.utils import save_df_as_jsonl, denormalize_bboxes


class BaseLayoutExporter(object):
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
        user_prompt: str = "Parse the reading order of this document.",
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
        df_copied["query"] = user_prompt

        save_df_as_jsonl(
            df=df_copied,
            save_path=save_path,
        )
