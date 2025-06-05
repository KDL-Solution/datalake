import re
import pandas as pd
import json


def to_chat_format(
    image_path,
    user_prompt,
    system_prompt,
):
    return {
        "messages": [
            {
                "role": "user",
                "content": f"<image>{user_prompt}",
            },
            {
                "role": "assistant",
                "content": system_prompt,
            },
        ],
        "images": [
            image_path,
        ],
    }


def save_df_as_jsonl(
    df: pd.DataFrame,
    save_path: str,
) -> None:
    with open(save_path, "w", encoding="utf-8") as f:
        for row in df.itertuples(index=False):
            json_obj = to_chat_format(
                image_path=row.image_path,
                user_prompt=row.query,
                system_prompt=row.label,
            )
            f.write(json.dumps(json_obj, ensure_ascii=False) + "\n")


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
