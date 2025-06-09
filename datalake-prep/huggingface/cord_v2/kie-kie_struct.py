import json
from datasets import load_dataset
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, Tuple
from functools import partial
from PIL import Image, ImageDraw
import ast
from typing import Union

from utils import DATALAKE_DIR, sha256_pil_image


def quad_to_ltrb(
    quad: Dict[str, int],
) -> Tuple[int]:
    xs = [quad[k] for k in quad if k.startswith("x")]
    ys = [quad[k] for k in quad if k.startswith("y")]
    return min(xs), min(ys), max(xs), max(ys)


def crop_using_roi(
    image: Image.Image,
    roi: dict,
) -> Image.Image:
    left, top, right, bottom = quad_to_ltrb(
        quad=roi,
    )
    width, height = image.size
    left = max(0, left)
    top = max(0, top)
    right = min(width, right)
    bottom = min(height, bottom)
    return image.crop((left, top, right, bottom))


def get_nested(
    d,
    key_str,
):
    keys = key_str.split(".")
    for key in keys:
        d = d[key]
    return d


def print_dict(
    d,
):
    print(json.dumps(
        d,
        indent=4,
        ensure_ascii=False,
    ))


def wrap_values_with_bbox(
    d: Any,
) -> Any:
    if isinstance(d, dict):
        return {k: wrap_values_with_bbox(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [wrap_values_with_bbox(item) for item in d]
    else:
        return {
            "<|value|>": d,
            "<|bbox|>": ""
        }


def set_bbox_at_path(
    d,
    key_str,
    text: str,
    bbox_value,
):
    keys = key_str.split(".")
    # print(keys)
    sub_d = d
    for key in keys[: -1]:
        sub_d = sub_d[key]
        # print(sub_d)
    last_key = keys[-1]
    # print(last_key)
    # Wrap if not already wrapped
    # print(last_key)
    # if isinstance(sub_d, list):
        # print(text)
        # print(sub_d)
        # for el in sub_d:
            # print(el.keys())
            # print(el[last_key])
    if not isinstance(sub_d[last_key], dict) or "<|bbox|>" not in sub_d[last_key]:
        sub_d[last_key] = {
            "<|value|>": sub_d[last_key],
            "<|bbox|>": ""
        }
    sub_d[last_key]["<|bbox|>"] = bbox_value


def union_bboxes(bboxes):
    lefts, tops, rights, bottoms = zip(*bboxes)
    return (
        min(lefts),
        min(tops),
        max(rights),
        max(bottoms)
    )


def vis_label(
    image: Image.Image,
    label: dict,
    outline="red",
    width=2,
    font=None,
) -> Image.Image:
    def recurse(
        obj: Union[dict, list],
        path="",
    ):
        if isinstance(obj, dict):
            if "<|value|>" in obj and "<|bbox|>" in obj:
                bbox = obj["<|bbox|>"]
                bbox_str = str(bbox)
                try:
                    bbox = ast.literal_eval(bbox_str)
                    if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                        draw.rectangle(bbox, outline=outline, width=width)
                        label_pos = (bbox[0], max(bbox[1] - 12, 0))  # label just above box
                        draw.text(label_pos, path, fill=outline, font=font)
                except Exception as e:
                    print(f"Skipping invalid bbox {bbox_str}: {e}")
            else:
                for k, v in obj.items():
                    recurse(v, f"{path}.{k}" if path else k)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                recurse(item, f"{path}[{i}]")

    draw = ImageDraw.Draw(image)
    recurse(
        json.loads(label),
    )
    return image


def generate_label(
    ground_truth: str,
    indent: int = None,
):
    idx = 6
    example = test_dataset[idx]
    image = Image.open(BytesIO(example["image"]["bytes"])).convert("RGB")
    image.save("/home/eric/workspace/sample.jpg")
    ground_truth=example["ground_truth"]
    
    
    gt_dict = json.loads(ground_truth)
    gt_parse = gt_dict["gt_parse"]
    # print_dict(gt_parse)
    label = wrap_values_with_bbox(gt_parse)
    print_dict(label)
    label_str = json.dumps(
        label,
        indent=indent,
        ensure_ascii=False,
    )

    # for row in gt_dict["valid_line"]:
        # print(row["category"])
        # print(row["words"])
        # print(text)
    import re
    value_pattern = r'"<\|value\|>":\s*"([^"]*?)"'
    valid_line = gt_dict["valid_line"]
    valid_line.sort(key=lambda x: x["group_id"])
    gt_parse
    for row, value in zip(valid_line, re.findall(value_pattern, label_str)):
        # for word in row["words"]:
        #     if word["is_key"] != 0:
        #         continue

        #     text = word["text"]
        #     quad = word["quad"]
        #     text
        text = " ".join(
            [word["text"] for word in row["words"] if not word["is_key"]],
        )
        text, value
        assert text == value
        # if text != value:
        #     print(f"|{text}|")
        #     print(f"|{value}|")

        try:
            bbox = union_bboxes(
                [
                    quad_to_ltrb(
                        word["quad"],
                    )
                    for word in row["words"]
                    if word["is_key"] == 0
                ],
            )
        except:
            print(text)
            print([(word["quad"], word["is_key"]) for word in row["words"]])


        label_str = label_str.replace('"<|bbox|>": ""', f'"<|bbox|>": "{list(bbox)}"', 1)
    # print_dict(json.loads(label_str))
    #     print(text)
        # set_bbox_at_path(
        #     d=label,
        #     text=text,
        #     key_str=row["category"],
        #     bbox_value=list(bbox),
        # )
    return label_str
    # return json.dumps(
    #     label,
    #     indent=indent,
    #     ensure_ascii=False,
    # )


def save_image_and_generate_label(
    example: Dict[str, Any],
    images_dir: str,
) -> Dict[str, Any]:
    images_dir = Path(images_dir)

    # print(example["image"].keys())
    # print(type(example))
    # print(example["ground_truth"])
    image = Image.open(BytesIO(example["image"]["bytes"])).convert("RGB")
    width, height = image.size
    image_hash = sha256_pil_image(image)
    image_path = (images_dir / image_hash[:2] / image_hash).with_suffix(".jpeg")

    if not image_path.exists():
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image.save(image_path, format="JPEG")

    label = generate_label(
        example["ground_truth"],
    )
    return {
        "image_path": str(image_path),
        "width": width,
        "height": height,
        "label": label
    }


def export_to_parquet(
    dataset: Dict[str, Any],
    images_dir: str,
    parquet_path: str,
) -> None:
    dataset = dataset.map(
        partial(
            save_image_and_generate_label,
            images_dir=images_dir,
        ),
        batched=False,
    )
    dataset = dataset.remove_columns(
        [
            col for col in dataset.column_names
            if col not in [
                "image_path",
                "width",
                "height",
                "label",
            ]
        ]
    )
    parquet_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    dataset.to_pandas().to_parquet(
        str(parquet_path),
        index=False,
    )


def main(
    save_dir: str,
    datalake_dir: str = DATALAKE_DIR,
) -> None:
    data_dir = Path(datalake_dir) / f"source/provider=huggingface/dataset=cord_v2"
    train_dataset, val_dataset, test_dataset = load_dataset(
        "parquet",
        data_files={
            "train": (data_dir / "data/train-*.parquet").as_posix(),
            "val": (data_dir / "data/validation-*.parquet").as_posix(),
            "test": (data_dir / "data/test-*.parquet").as_posix(),
        },
        split=[
            "train",
            "val",
            "test",
        ],
    )

    save_dir = Path(save_dir)
    # export_to_parquet(
    #     dataset=train_dataset,
    #     images_dir=save_dir / "images_train",
    #     parquet_path=(save_dir / "train.parquet").as_posix(),
    # )
    # export_to_parquet(
    #     dataset=val_dataset,
    #     images_dir=save_dir / "images_val",
    #     parquet_path=(save_dir / "val.parquet").as_posix(),
    # )
    export_to_parquet(
        dataset=test_dataset,
        images_dir=save_dir / "images_test",
        parquet_path=(save_dir / "test.parquet").as_posix(),
    )


if __name__ == "__main__":
    # datalake/datalake-prep에서 실행하시오: e.g., `python -m huggingface.cord_v2.kie-kie_struct`.
    main(
        save_dir=Path(__file__).resolve().parent.as_posix(),
    )
