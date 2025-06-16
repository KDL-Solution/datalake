# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
from pathlib import Path
import pandas as pd
from typing import Dict, Any

from prep.utils import DATALAKE_DIR
from export.utils import (
    save_df_as_jsonl,
    denormalize_bboxes,
    smart_resize,
)


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
        user_prompt: str,
        jsonl_path: str,
        datalake_dir: str = DATALAKE_DIR.as_posix(),
        value_key: str = "<|value|>",
        bbox_key: str = "<|bbox|>",
        indent: int = None,
    ) -> None:
        df_copied = df.copy()

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
        df_copied[["width", "height"]] = df_copied.apply(
            lambda x: smart_resize(
                width=x["width"],
                height=x["height"],
            ),
            axis=1,
            result_type="expand",
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
            jsonl_path=jsonl_path,
        )


if __name__ == "__main__":
    from athena.src.core.athena_client import AthenaClient
    from export.utils import user_prompt_dict

    client = AthenaClient()
    exporter = KIEStructExporter()
    ROOT = Path(__file__).resolve().parent

    # df = client.retrieve_with_existing_cols(
    #     datasets=[
    #         "real_kie",
    #     ],
    # )
    # exporter.export(
    #     df=df,
    #     jsonl_path="/home/eric/workspace/Qwen-SFT/real_kie.jsonl",
    # )

    df = client.retrieve_with_existing_cols(
        datasets=[
            "post_handwritten_plain_text",
        ],
    )
    exporter.export(
        df=df,
        user_prompt=user_prompt_dict["post_handwritten_plain_text"],
        jsonl_path=(ROOT / "data/post_handwritten_plain_text.jsonl").as_posix(),
    )
