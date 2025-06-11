import json
import re
from datasets import load_dataset
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, Tuple, List
from functools import partial
from PIL import Image
from rapidfuzz.distance import Levenshtein

from utils import DATALAKE_DIR, get_safe_image_hash_from_pil


def quad_to_ltrb(
    quad: Dict[str, int],
) -> Tuple[int]:
    xs = [quad[k] for k in quad if k.startswith("x")]
    ys = [quad[k] for k in quad if k.startswith("y")]
    return min(xs), min(ys), max(xs), max(ys)


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


def union_bboxes(
    bboxes: List[Tuple[int, int, int, int]],
) -> Tuple[int, int, int, int]:
    lefts, tops, rights, bottoms = zip(*bboxes)
    return (
        min(lefts),
        min(tops),
        max(rights),
        max(bottoms)
    )


def extract_value_bbox_pairs(
    json_str: str,
) -> List[Tuple[str, str]]:
    """Extract list of (<|value|>, <|bbox|>) tuples in order."""
    pattern = r'"<\|value\|>":\s*"([^"]*)",\s*"<\|bbox\|>":\s*"([^"]*)"'
    return re.findall(pattern, json_str)


def compute_cer(
    s1: str,
    s2: str,
) -> float:
    """Character Error Rate (CER) = Levenshtein distance / length of reference"""
    if not s2:
        return float("inf")
    return Levenshtein.distance(s1, s2) / len(s2)


def reorder_by_cer(
    json_str: str,
    texts: List[str],
    bboxes: List[str],
) -> str:
    # 1. Extract original <|value|>, <|bbox|> pairs
    ori_pairs = extract_value_bbox_pairs(
        json_str,
    )
    ori_vals = [v for v, _ in ori_pairs]
    # 길이가 서로 다르면 GT 생성 불가.
    assert len(ori_vals) == len(texts) and len(texts) == len(bboxes)

    # 2. Compute best match for each original_value using CER
    assigned = [False] * len(texts)
    new_pairs = []

    for ori_val in ori_vals:
        best_idx = None
        best_score = float("inf")

        for i, text in enumerate(texts):
            if assigned[i]:
                continue

            score = compute_cer(
                ori_val,
                text,
            )
            if score < best_score:
                best_score = score
                best_idx = i
        if best_idx is not None:
            assigned[best_idx] = True
            new_pairs.append(
                (texts[best_idx], bboxes[best_idx])
            )
        else:
            raise ValueError(f"No match found for original_value: {ori_val}")

    # 3. Replace <|value|> and <|bbox|> pairs in original string with new order
    def replacement_generator():
        for val, bbox in new_pairs:
            yield f'"<|value|>": "{val}", "<|bbox|>": "{bbox}"'

    replacer = replacement_generator()
    updated_str = re.sub(
        r'"<\|value\|>":\s*"[^"]*",\s*"<\|bbox\|>":\s*"[^"]*"',
        lambda _: next(replacer),
        json_str,
        count=len(new_pairs),
    )
    return updated_str


def generate_label(
    ground_truth: str,
    indent: int = None,
):
    gt_dict = json.loads(ground_truth)
    gt_parse = gt_dict["gt_parse"]
    label_dict = wrap_values_with_bbox(gt_parse)
    label_str = json.dumps(
        label_dict,
        indent=indent,
        ensure_ascii=False,
    )

    valid_line = gt_dict["valid_line"]
    valid_line.sort(key=lambda x: x["group_id"])

    bboxes = []
    for row in valid_line:
        ltrbs = [
            quad_to_ltrb(
                word["quad"],
            )
            for word in row["words"]
            if word["is_key"] == 0
        ]
        if ltrbs:
            bbox = union_bboxes(
                ltrbs,
            )
            bboxes.append(
                str(list(bbox))
            )

    texts = [
        " ".join([word["text"] for word in row["words"] if not word["is_key"]])
        for row in valid_line
        if any(not word["is_key"] for word in row["words"])
    ]
    try:
        label_str = reorder_by_cer(
            label_str,
            texts=texts,
            bboxes=bboxes,
        )
    except AssertionError:
        return ""

    label_dict = json.loads(label_str)
    # Wrap 'menu' value into a list if it's not already.
    if isinstance(label_dict.get("menu"), dict):
        label_dict["menu"] = [label_dict["menu"]]
    label_str = json.dumps(
        label_dict,
        indent=indent,
        ensure_ascii=False,
    )
    return label_str


def process_all_nm_and_cnt(
    json_str: str,
    indent: int = None,
) -> str:
    def recursive_process(d: Any) -> Any:
        if isinstance(d, dict):
            new_dict = {}
            for k, v in d.items():
                # 키가 'nm'이면 'name'으로 바꿈
                new_key = "name" if k == "nm" else k
                new_val = recursive_process(v)

                # cnt 안의 <|value|> 값이 있으면 숫자만 추출
                if new_key == "cnt" and isinstance(new_val, dict) and "<|value|>" in new_val:
                    original = new_val["<|value|>"]
                    match = re.search(
                        r"\d+",
                        original,
                    )
                    new_val["<|value|>"] = match.group(0) if match else ""

                new_dict[new_key] = new_val
            return new_dict

        elif isinstance(d, list):
            return [recursive_process(item) for item in d]

        else:
            return d

    # 문자열이 비어있거나 유효하지 않으면 예외 처리
    if not json_str or not json_str.strip():
        raise ValueError("Input JSON string is empty or blank")

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON string: {e}")

    processed = recursive_process(data)
    return json.dumps(
        processed,
        indent=indent,
        ensure_ascii=False,
    )


def save_image_and_generate_label(
    example: Dict[str, Any],
    images_dir: str,
) -> Dict[str, Any]:
    images_dir = Path(images_dir)

    image = Image.open(BytesIO(example["image"]["bytes"])).convert("RGB")
    width, height = image.size
    image_hash = get_safe_image_hash_from_pil(
        image,
    )
    image_path = Path(f"{images_dir / image_hash[: 2] / image_hash}.jpg")
    if not image_path.exists():
        image_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        image.save(
            image_path,
            format="JPEG",
        )

    label = generate_label(
        example["ground_truth"],
    )
    if label:
        label = process_all_nm_and_cnt(
            label,
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
    dataset = dataset.filter(
        lambda example: example["label"].strip() != "",
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
    Path(parquet_path).parent.mkdir(
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
    export_to_parquet(
        dataset=train_dataset,
        images_dir=save_dir / "images_train",
        parquet_path=(save_dir / "train.parquet").as_posix(),
    )
    export_to_parquet(
        dataset=val_dataset,
        images_dir=save_dir / "images_val",
        parquet_path=(save_dir / "val.parquet").as_posix(),
    )
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
