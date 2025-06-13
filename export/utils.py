import re
import json
import math
import pandas as pd
from typing import List, Dict, Any


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
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for row in df.itertuples(index=False):
            json_obj = to_chat_format(
                image_paths=row.image_path,
                user_prompts=row.query,
                system_prompts=row.label,
            )
            _ = f.write(json.dumps(json_obj, ensure_ascii=False) + "\n")


def round_by_factor(number: int, factor: int) -> int:
    """Returns the closest integer to 'number' that is divisible by 'factor'."""
    return round(number / factor) * factor


def ceil_by_factor(number: int, factor: int) -> int:
    """Returns the smallest integer greater than or equal to 'number' that is divisible by 'factor'."""
    return math.ceil(number / factor) * factor


def floor_by_factor(number: int, factor: int) -> int:
    """Returns the largest integer less than or equal to 'number' that is divisible by 'factor'."""
    return math.floor(number / factor) * factor


IMAGE_FACTOR = 28
MIN_PIXELS = 4 * (IMAGE_FACTOR ** 2)
MAX_PIXELS = 16384 * (IMAGE_FACTOR ** 2)
MAX_RATIO = 200


def smart_resize(
    height: int,
    width: int,
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
    return h_bar, w_bar


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
