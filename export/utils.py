import re
import json
import math
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from typing import List, Dict, Any
from PIL import Image, ImageDraw
from datasets import Dataset
from io import BytesIO
from docling.backend.html_backend import HTMLDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import InputDocument

EXPORT_DATA_DIR = Path(__file__).resolve().parent / "data"


def to_chat_format(
    image_paths: List[str],
    user_prompts: List[str],
    system_prompts: List[str],
) -> Dict[str, Any]:
    if isinstance(image_paths, str):
        image_paths = [image_paths]
    if isinstance(user_prompts, str):
        user_prompts = [user_prompts]
    if isinstance(system_prompts, str):
        system_prompts = [system_prompts]

    messages = []
    for idx, (user_prompt, system_prompt) in enumerate(
        zip(user_prompts, system_prompts),
    ):
        if idx < len(image_paths):
            user_prompt = "<image>" + user_prompt
        messages.extend(
            [
                {
                    "role": "user",
                    "content": user_prompt,
                },
                {
                    "role": "assistant",
                    "content": system_prompt,
                },
            ],
        )
    return {
        "messages": messages,
        "images": image_paths,
    }


def save_df_as_jsonl(
    df: pd.DataFrame,
    jsonl_path: str,
) -> None:
    Path(jsonl_path).parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    chat_format = False
    cols = df.columns.tolist()
    if "messages" in cols and "images" in cols:
        chat_format = True
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for row in df.itertuples(index=False):
            if chat_format:
                json_obj = {
                    "messages": row.messages,
                    "images": row.images,
                }
            else:
                json_obj = to_chat_format(
                    image_paths=row.path,
                    user_prompts=row.query,
                    system_prompts=row.label,
                )
            _ = f.write(json.dumps(json_obj, ensure_ascii=False) + "\n")


def save_dataset_as_jsonl(
    dataset: Dataset,
    jsonl_path,
    batch_size: int = 8_192,
    keep_columns: List[str] = [
        "path",
        "query",
        "label",
    ],
):
    dataset = dataset.remove_columns(
        [
            col for col in dataset.column_names
            if col not in keep_columns
        ],
    )

    with open(jsonl_path, "w", encoding="utf-8") as f:
        batch = []
        for example in tqdm(dataset, desc="Saving as JSONL..."):
            batch.append(json.dumps(example, ensure_ascii=False))
            if len(batch) >= batch_size:
                f.write("\n".join(batch) + "\n")
                batch = []
        if batch:
            f.write("\n".join(batch) + "\n")


def round_by_factor(
    number: int,
    factor: int,
) -> int:
    """Returns the closest integer to 'number' that is divisible by 'factor'."""
    return round(number / factor) * factor


def ceil_by_factor(
    number: int,
    factor: int,
) -> int:
    """Returns the smallest integer greater than or equal to 'number' that is divisible by 'factor'."""
    return math.ceil(number / factor) * factor


def floor_by_factor(
    number: int,
    factor: int,
) -> int:
    """Returns the largest integer less than or equal to 'number' that is divisible by 'factor'."""
    return math.floor(number / factor) * factor


IMAGE_FACTOR = 28
MIN_PIXELS = 4 * (IMAGE_FACTOR ** 2)
MAX_PIXELS = 16384 * (IMAGE_FACTOR ** 2)
MAX_RATIO = 200


def smart_resize(
    width: int,
    height: int,
    factor: int = IMAGE_FACTOR,
    min_pixels: int = MIN_PIXELS,
    max_pixels: int = MAX_PIXELS,
) -> tuple[int, int]:
    """https://github.com/QwenLM/Qwen2.5-VL/blob/main/qwen-vl-utils/src/qwen_vl_utils/vision_process.py#L60
    Rescales the image so that the following conditions are met:

    1. Both dimensions (height and width) are divisible by 'factor'.

    2. The total number of pixels is within the range ['min_pixels', 'max_pixels'].

    3. The aspect ratio of the image is maintained as closely as possible.
    """
    if max(height, width) / min(height, width) > MAX_RATIO:
        raise ValueError(
            f"absolute aspect ratio must be smaller than {MAX_RATIO}, got {max(height, width) / min(height, width)}"
        )
    h_bar = max(factor, round_by_factor(height, factor))
    w_bar = max(factor, round_by_factor(width, factor))
    if h_bar * w_bar > max_pixels:
        beta = math.sqrt((height * width) / max_pixels)
        h_bar = max(factor, floor_by_factor(height / beta, factor))
        w_bar = max(factor, floor_by_factor(width / beta, factor))
    elif h_bar * w_bar < min_pixels:
        beta = math.sqrt(min_pixels / (height * width))
        h_bar = ceil_by_factor(height * beta, factor)
        w_bar = ceil_by_factor(width * beta, factor)
    return w_bar, h_bar


def denormalize_bboxes(
    json_str: str,
    width: int,
    height: int,
    bbox_key: str = "bbox",
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
        return f'"{bbox_key}": {abs_bbox}'

    escaped_key = re.escape(bbox_key)
    # Build dynamic pattern: e.g., r'"<\|bbox\|>":\s*(\[[^\]]+\])'
    pattern = rf'"{escaped_key}":\s*(\[[^\]]+\])'
    return re.sub(pattern, replacer, json_str)


def mask_outside_bboxes(
    image: Image.Image,
    bboxes: list[tuple[int, int, int, int]],
) -> Image.Image:
    # Create a white image (same size, same mode)
    white_bg = Image.new(image.mode, image.size, color="white")

    # Create a mask where we draw the bboxes
    mask = Image.new("L", image.size, 0)  # Black mask
    draw = ImageDraw.Draw(mask)
    for bbox in bboxes:
        draw.rectangle(bbox, fill=255)  # White inside bboxes

    # Composite the original image over the white background using the mask
    result = Image.composite(image, white_bg, mask)
    return result


layout_category_dict = {
    "text_plain": "plain_text",
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

user_prompt_dict = {
    "recognition": "Read text in the image.",
    "base_kie_bbox": "Extract information from the image. Return the result in the following structured JSON format (formatted with zero-space indentation and without newlines), filling in both <|value|> and <|bbox|>:",
    "base_kie_no_bbox": "Extract information from the image. Return the result in the following structured JSON format (formatted with zero-space indentation and without newlines), filling in <|value|>:",
    "post_handwritten_plain_text": "Extract information from the image. Return the result in the following structured JSON format (formatted with zero-space indentation and without newlines), filling in both <|value|> and <|bbox|>. Read from left to right, top to bottom:",
    "base_layout_reading_order": "Extract all layout elements. Reading order must be preserved.",
    "base_layout_no_reading_order": "Extract all layout elements. Reading order does not matter.",
    "plain_text": "Read text in the image.",
    "table": "Parse the table in the image.",
    "image": "Read text in the image.",
}


def extract_otsl(
    text: str,
) -> str:
    # Find the content inside <otsl>...</otsl>:
    match = re.search(r"<otsl>.*?</otsl>", text, re.DOTALL)
    if match:
        return match.group(0).strip()
    else:
        return None


class HTMLToDogTags:
    def __init__(
        self,
    ):
        self.backend_class = HTMLDocumentBackend
        self.format = InputFormat.HTML

    def convert(
        self,
        html: str,
    ) -> str:
        html_bytes = html.encode("utf-8")
        bytes_io = BytesIO(html_bytes)
        in_doc = InputDocument(
            path_or_stream=bytes_io,
            format=self.format,
            backend=self.backend_class,
            filename="temp.html",
        )
        backend = self.backend_class(
            in_doc=in_doc,
            path_or_stream=bytes_io,
        )
        dl_document = backend.convert()
        doctags = dl_document.export_to_doctags()
        return extract_otsl(
            doctags,
        )
