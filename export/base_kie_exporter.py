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
    def __init__(
        self,
        datalake_dir: str = DATALAKE_DIR.as_posix(),
    ):
        self.datalake_dir = datalake_dir

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
        value_key: str = "<|value|>",
        bbox_key: str = "<|bbox|>",
        indent: int = None,
    ) -> None:
        df_copied = df.copy()

        df_copied["path"] = df_copied["path"].apply(
            lambda x: (Path(self.datalake_dir) / "assets" / x).as_posix(),
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
    import duckdb
    from sklearn.model_selection import train_test_split

    from export.utils import user_prompt_dict

    exporter = KIEStructExporter()
    conn = duckdb.connect()
    ROOT = Path(__file__).resolve().parent
    seed = 42

    read_parquet = "read_parquet('/mnt/AI_NAS/datalake/catalog/provider=*/dataset=*/task=*/variant=*/data.parquet', union_by_name=True, filename=True, hive_partitioning=True)"

    sql=f"""SELECT *
    FROM {read_parquet}
    WHERE dataset = 'post_handwritten_plain_text'
        AND task != 'raw'"""
    df = conn.execute(
        sql,
    ).fetchdf()
    df_train, df_test = train_test_split(
        df,
        test_size=10,
        random_state=seed,
    )
    exporter.export(
        df=df_train,
        user_prompt=user_prompt_dict["post_handwritten_plain_text"],
        jsonl_path=(ROOT / "data/post_handwritten_plain_text_train.jsonl").as_posix(),
    )
    exporter.export(
        df=df_test,
        user_prompt=user_prompt_dict["post_handwritten_plain_text"],
        jsonl_path=(ROOT / "data/post_handwritten_plain_text_test.jsonl").as_posix(),
    )
