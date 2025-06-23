# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
from pathlib import Path
import pandas as pd
from typing import List, Dict
from PIL import Image, ImageDraw, ImageFont
from tqdm import tqdm

from prep.utils import DATALAKE_DIR
from export.utils import (
    save_df_as_jsonl,
    denormalize_bboxes,
    smart_resize,
    mask_outside_bboxes,
    layout_category_dict,
    user_prompt_dict,
    filter_valid_image_paths,
)

tqdm.pandas()


class BaseLayoutExporter(object):
    def __init__(
        self,
        datalake_dir: str = DATALAKE_DIR.as_posix(),
    ):
        self.datalake_dir = datalake_dir

    def _elements_to_label(
        self,
        elements: List[Dict[str, str]],
        layout_category_dict: Dict[str, str] = layout_category_dict,
        indent: int = None,
    ):
        elements.sort(key=lambda x: x["idx"])  # idx 기준으로 정렬.
        elements = [
            {k: i[k] for k in ["type", "bbox"]} for i in elements
        ]  # type, bbox만 남김.
        elements = [
            {**i, "type": layout_category_dict.get(i["type"], i["type"])}
            for i in elements
        ]  # type을 통폐합.
        return json.dumps(
            elements,
            ensure_ascii=False,
            indent=indent,
        )

    def save_masked_image(
        self,
        image_path: str,
        bboxes: List[int],
        images_dir: str,
    ):
        try:
            image = Image.open(image_path).convert("RGB")
        except OSError:
            print(f"[ERROR] Cannot open image: {image_path}")
            return None
        else:
            new_image = mask_outside_bboxes(
                image=image,
                bboxes=bboxes,
            )
            new_image_path = Path(images_dir) / Path(image_path).name
            if not new_image_path.exists():
                new_image_path.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )
                new_image.save(
                    new_image_path.as_posix(),
                    format="JPEG",
                )
            return new_image_path.as_posix()

    def export(
        self,
        df: pd.DataFrame,
        jsonl_path: str,
        images_dir: str,
        user_prompt_reading_order: str = user_prompt_dict["base_layout_reading_order"],
        user_prompt_no_reading_order: str = user_prompt_dict["base_layout_no_reading_order"],
        indent: int = None,
    ) -> None:
        df_copied = df.copy()

        df_copied[["new_width", "new_height"]] = df_copied.apply(
            lambda x: smart_resize(
                width=x["width"],
                height=x["height"],
            ),
            axis=1,
            result_type="expand",
        )  # Smart resize.
        df_copied["label"] = df_copied.apply(
            lambda x: denormalize_bboxes(
                x["label"],
                width=x["new_width"],
                height=x["new_height"],
                bbox_key="bbox",
            ),
            axis=1,
        )  # Denormalize.
        df_copied["label"] = df_copied["label"].apply(
            lambda x: json.loads(x),
        )  # String to Dict.

        df_copied["path"] = df_copied["path"].apply(
            lambda x: (Path(self.datalake_dir) / "assets" / x).as_posix(),
        )  # Relative path to absolute path.
        df_copied = filter_valid_image_paths(
            df_copied,
        )
        df_copied["path"] = df_copied.progress_apply(
            lambda x: self.save_masked_image(
                image_path=x["path"],
                bboxes=[i["bbox"] for i in x["label"]["elements"]],
                images_dir=images_dir,
            ),
            axis=1,
        )
        df_copied = df_copied[df_copied["path"].notna()]

        df_copied["query"] = df_copied.apply(
            lambda x: user_prompt_reading_order if x["label"]["reading_order"] else user_prompt_no_reading_order,
            axis=1,
        )
        df_copied["label"] = df_copied.apply(
            lambda x: self._elements_to_label(
                x["label"]["elements"],
                indent=indent,
            ),
            axis=1,
        )

        save_df_as_jsonl(
            df=df_copied,
            jsonl_path=jsonl_path,
        )


def vis_elements(
    image: Image.Image,
    elements: List[Dict[str, str]],
):
    assert all(
        isinstance(x, float) for i in elements for x in i["bbox"]
    )
    elements_str = json.dumps(
        elements,
        ensure_ascii=False,
        indent=None,
    )
    width, height = image.size
    elements_str = denormalize_bboxes(
        elements_str,
        width=width,
        height=height,
        bbox_key="bbox",
    )
    elements = json.loads(
        elements_str,
    )

    draw = ImageDraw.Draw(image)
    for i in elements:
        idx = i["idx"]
        type_ = i["type"]
        box = i["bbox"]

        # 사각형 그리기
        draw.rectangle(
            box,
            outline="red",
            width=2,
        )

        # 텍스트: idx + type
        text = f"{idx}: {type_}"
        text_pos = (box[0], box[1] - 12)
        draw.text(
            text_pos,
            text,
            fill="blue",
            font=ImageFont.load_default(),
        )
    return image


if __name__ == "__main__":
    from athena.src.core.athena_client import AthenaClient

    client = AthenaClient()
    df = client.retrieve_with_existing_cols(
        datasets=[
            "office_docs",
        ],
    )

    exporter = BaseLayoutExporter()
    ROOT = Path(__file__).resolve().parent
    exporter.export(
        df=df,
        jsonl_path=(ROOT / "data/office_docs.jsonl").as_posix(),
        images_dir="/mnt/AI_NAS/datalake/catalog/"
    )

    # df = client.retrieve_with_existing_cols(
    #     datasets=[
    #         "doclaynet_train",
    #         "doclaynet_val",
    #     ],
    # )
    # exporter.export(
    #     df=df,
    #     jsonl_path=(ROOT / "data/doclaynet_train_val.jsonl").as_posix(),
    # )
    # df = client.retrieve_with_existing_cols(
    #     datasets=[
    #         "doclaynet_test",
    #     ],
    # )
    # exporter.export(
    #     df=df,
    #     jsonl_path=(ROOT / "data/doclaynet_test.jsonl").as_posix(),
    # )
