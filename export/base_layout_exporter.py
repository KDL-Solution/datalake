# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
from pathlib import Path
import pandas as pd
from typing import List, Dict
from PIL import Image, ImageDraw, ImageFont

from export.utils import (
    save_df_as_jsonl,
    denormalize_bboxes,
    smart_resize,
)


TYPE_MAP = {
    "text_plane": "plain_text",
    "title": "plain_text",
    "section_header": "plain_text",
    "list_item": "plain_text",
    "caption": "plain_text",
    "page_header": "plain_text",
    "page_footer": "plain_text",
    "abstract": "plain_text",
    "keywords": "plain_text",
    "footnote": "plain_text",
    "handwriting": "plain_text",
    "table_of_contents_entry": "plain_text",
    "text_inline_math": "plain_text",
    "table": "table",
    "figure": "image",
    "picture": "image",
    "flowchart": "image",
    "chart": "image",
    "chart_bar": "image",
    "chart_pie": "image",
    "chart_line": "image",
    "chart_area": "image",
    "chart_scatter": "image",
    "chart_radar": "image",
    "chart_mixed": "image",
    "diagram": "image",
    "diagram_functional_block": "image",
    "diagram_flowchart": "image",
    "diagram_characteristic_curve": "image",
    "diagram_timing": "image",
    "diagram_circuit": "image",
    "diagram_3d_schematic": "image",
    "diagram_appearance": "image",
    "diagram_pin": "image",
    "diagram_layout": "image",
    "diagram_engineering_drawing": "image",
    "diagram_data_structure": "image",
    "diagram_sampling": "image",
    "diagram_functional_register": "image",
    "diagram_marking": "image",
    "formula": "plain_text",
    "music_sheet": "plain_text",
    "chemical_formula_content": "plain_text",
    "publishing_info": "plain_text",
    "signature": "plain_text",
    "stamp": "plain_text",
}


class BaseLayoutExporter(object):
    def _elements_to_label(
        self,
        elements: List[Dict[str, str]],
        width: int,
        height: int,
        type_map: Dict[str, str] = TYPE_MAP,
        indent: int = None,
    ):
        elements.sort(key=lambda x: x["idx"])  # idx 기준으로 정렬.
        elements = [
            {k: i[k] for k in ["type", "bbox"]} for i in elements
        ]  # type, bbox만 남김.
        elements = [
            {**i, "type": type_map.get(i["type"], i["type"])}
            for i in elements
        ]  # type을 통폐합.

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
        user_prompt_reading_order: str = "Extract all layout elements. Reading order must be preserved.",
        user_prompt_no_reading_order: str = "Extract all layout elements. Reading order does not matter.",
        indent: int = None,
    ) -> None:
        df_copied = df.copy()

        df_copied["image_path"] = df_copied["image_path"].apply(
            lambda x: (Path(datalake_dir) / x).as_posix(),
        )
        df_copied["query"] = df_copied.apply(
            lambda x: user_prompt_reading_order if json.loads(
                x["label"],
            )["reading_order"] else user_prompt_no_reading_order,
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

        save_df_as_jsonl(
            df=df_copied,
            jsonl_path=save_path,
        )


def vis_elements(
    image: Image.Image,
    elements: List[Dict[str, str]],
):
    assert all(
        isinstance(x, int) for i in elements for x in i["bbox"]
    )  # 좌표는 이미 denormalized를 가정.

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
