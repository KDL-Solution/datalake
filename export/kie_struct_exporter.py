# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
import sys
from pathlib import Path
import pandas as pd
from typing import Dict, Any

# sys.path.insert(0, "/home/eric/workspace/datalake/")
from export.utils import save_df_as_jsonl, denormalize_bboxes


def remove_none_values(
    obj: Any,
) -> Any:
    if isinstance(obj, dict):
        return {
            k: remove_none_values(v)
            for k, v in obj.items()
            if v is not None
        }
    elif isinstance(obj, list):
        return [remove_none_values(item) for item in obj]
    else:
        return obj


def truncate_lists(
    obj: Any,
) -> Any:
    if isinstance(obj, dict):
        return {
            k: truncate_lists(v)
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        if len(obj) > 1:
            return [truncate_lists(obj[0])]
        else:
            return [truncate_lists(obj[0])] if obj else []
    else:
        return obj


class KIEStructExporter(object):
    def _blank_value_and_bbox(
        self,
        json_str: str,
        indent: int = 0,
        value_key: str = "<|value|>",
        bbox_key: str = "<|bbox|>",
    ) -> str:
        def recursively_blank(
            d: Dict[str, Any],
        ) -> Dict[str, Any]:
            if isinstance(d, dict):
                return {
                    k: (
                        "" if k == value_key else
                        [] if k == bbox_key else
                        recursively_blank(v)
                    )
                    for k, v in d.items()
                }
            elif isinstance(d, list):
                return [recursively_blank(item) for item in d]
            else:
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
        bbox_key: str = "<|bbox|>",
    ) -> str:
        label_dict = json.loads(label)
        label_dict = remove_none_values(label_dict)
        label_str = json.dumps(
            label_dict,
            indent=indent,
            ensure_ascii=False,
        )
        return denormalize_bboxes(
            label_str,
            width=width,
            height=height,
            bbox_key=bbox_key,
        )

    def _make_target_schema(
        self,
        label: str,
        indent: int = 0,
        value_key: str = "<|value|>",
        bbox_key: str = "<|bbox|>",
    ) -> str:
        label_dict = json.loads(label)
        label_dict = remove_none_values(label_dict)
        label_dict = truncate_lists(label_dict)
        label_str = json.dumps(
            label_dict,
            indent=indent,
            ensure_ascii=False,
        )
        return self._blank_value_and_bbox(
            label_str,
            indent=indent,
            value_key=value_key,
            bbox_key=bbox_key,
        )

    def export(
        self,
        df: pd.DataFrame,
        datalake_dir: str,
        save_path: str,
        user_prompt: str = "Extract the following information from the image. Return the result in the following structured JSON format (formatted with newlines and zero-space indentation), filling in both <|value|> and <|bbox|>:",
        value_key: str = "<|value|>",
        bbox_key: str = "<|bbox|>",
        indent: int = 0,
    ) -> None:
        df_copied = df.copy()
        # df_copied["query"] = df_copied["query"].str.replace("<|value|>", "<|value|>", regex=False).str.replace("<|bbox|>", "<|bbox|>", regex=False)
        # df_copied["label"] = df_copied["label"].str.replace('"<|value|>"', '"<|value|>"', regex=False).str.replace('"<|bbox|>"', '"<|bbox|>"', regex=False)
        # df_copied["image_path"] = df_copied["image_path"].str.replace('images/images', 'images', regex=False)

        df_copied["image_path"] = df_copied["image_path"].apply(
            lambda x: (Path(datalake_dir) / x).as_posix(),
        )
        df_copied["query"] = df_copied.apply(
            lambda x: user_prompt + "\n" + self._make_target_schema(
                x["label"],
                indent=indent,
                value_key=value_key,
                bbox_key=bbox_key,
            ),
            axis=1,
        )
        df_copied["label"] = df_copied.apply(
            lambda x: self._process_kie_label(
                label=x["label"],
                width=x["width"],
                height=x["height"],
                indent=indent,
                bbox_key=bbox_key,
            ),
            axis=1,
        )

        save_df_as_jsonl(
            df=df_copied,
            save_path=save_path,
        )
