# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
from pathlib import Path
import pandas as pd
from typing import Dict, Any
from PIL import Image, ImageDraw, ImageFont

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


def vis_base_kie_gt(
    image: Image.Image,
    label_str: str,
    font_path: str = None,
    font_size: int = 16,
) -> Image.Image:
    """
    주어진 label_str (JSON 문자열) 기반으로 PIL 이미지 위에 bbox와 텍스트를 시각화합니다.

    Args:
        image (PIL.Image.Image): 원본 이미지
        label_str (str): JSON 형식의 라벨 문자열
        font_path (str, optional): 사용할 TTF 폰트 경로. 기본은 시스템 기본 폰트
        font_size (int, optional): 텍스트 크기

    Returns:
        PIL.Image.Image: 시각화된 이미지
    """
    draw = ImageDraw.Draw(image)

    # 폰트 설정
    try:
        font = ImageFont.truetype(font_path, font_size) if font_path else ImageFont.load_default()
    except Exception:
        font = ImageFont.load_default()

    label_dict = json.loads(label_str)
    for key, info in label_dict.items():
        text = f"{key} -> {info['<|value|>']}"
        bbox = info['<|bbox|>']
        draw.rectangle(bbox, outline="red", width=2)
        draw.text((bbox[0], bbox[1] - font_size - 2), text, fill="blue", font=font)
    return image


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
