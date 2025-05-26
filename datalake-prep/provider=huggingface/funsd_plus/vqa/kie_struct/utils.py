from datasets import Dataset
from pathlib import Path
from PIL import Image
import hashlib, io, json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import Counter
from typing import List, Dict, Any
import numpy as np
from collections import defaultdict
import cv2
import yaml
from jinja2 import Template
import os
from PIL import Image, ImageDraw


def find_adjacent_box_direction(
    current_box, template, image_width, image_height, step_value=1, direction="right"
):
    """
    주어진 current_box의 오른쪽 또는 왼쪽에 위치한 인접 박스를 찾습니다.

    Args:
        current_box (TextBox): 현재 탐색 중인 텍스트 박스.
        template (np.array): 텍스트 박스 인덱스가 표시된 템플릿 이미지.
        image_width (int): 템플릿 이미지의 너비.
        image_height (int): 템플릿 이미지의 높이.
        step_value (int): 탐색 시 이동하는 픽셀 수. 기본값은 1.
        direction (str): 'right' 또는 'left'. 기본값은 'right'.

    Returns:
        Tuple[bool, Optional[int]]:
            - 첫 번째 값은 인접 박스를 찾았는지 여부(True이면 인접 박스가 없음을 의미).
            - 두 번째 값은 인접 박스의 인덱스 (찾으면 해당 번호, 없으면 None).
    """
    box_min_x = int(current_box.box[:, 0].min())
    box_max_x = int(current_box.box[:, 0].max())
    box_width = box_max_x - box_min_x

    if direction == "right":
        right_mid = ((current_box.box[1] + current_box.box[2]) / 2).astype(np.int32)
        search_start_x, search_y = right_mid[0], right_mid[1]
        search_end_x = min(search_start_x + box_width, image_width - 1)
        for x in range(search_start_x, search_end_x, step_value):
            y = np.clip(search_y, 0, image_height - 1)
            pixel_value = template[y, x]

            if pixel_value != 0 and pixel_value != current_box.index:
                return False, pixel_value
    elif direction == "left":
        left_mid = ((current_box.box[0] + current_box.box[3]) / 2).astype(np.int32)
        search_start_x, search_y = left_mid[0], left_mid[1]
        search_end_x = max(search_start_x - box_width, -1)

        for x in range(search_start_x - 1, search_end_x, -step_value):
            y = np.clip(search_y, 0, image_height - 1)
            pixel_value = template[y, x]
            if pixel_value != 0 and pixel_value != current_box.index:
                return False, pixel_value

    return True, None


def get_angles(points):
    x1, y1 = points[0]
    x2, y2 = points[1]
    x3, y3 = points[2]
    x4, y4 = points[3]

    bt_ang = np.arctan2(y3 - y4, x3 - x4) * 180 / np.pi
    tp_ang = np.arctan2(y2 - y1, x2 - x1) * 180 / np.pi

    return bt_ang, tp_ang


class TextBox:
    def __init__(self, box, orig_info, index):
        """
        Args:
                box: 텍스트 영역 좌표 리스트 [4,2]
        orig_info: 텍스트 원본 정보  dict keys =(x1, y1, x2, y2, x3, y3, x4, y4, confidence, angle, transcript)
        index: 고유 인덱스 (영역 식별)
        """
        self.box = box
        self.orig_info = orig_info
        self.index = index

        self.processed = False
        self.line_id = None
        self.parent = None


def get_location_v3(text_lst: List[Dict[str, Any]], W: int, H: int, auto_rotate: bool):
    angles = []
    all_points = []

    if len(text_lst) == 0:
        return [], 0.0

    for line_dict in text_lst:
        points = []
        x1 = line_dict["x1"]
        y1 = line_dict["y1"]
        x2 = line_dict["x2"]
        y2 = line_dict["y2"]
        x3 = line_dict["x3"]
        y3 = line_dict["y3"]
        x4 = line_dict["x4"]
        y4 = line_dict["y4"]
        points = [[x1, y1], [x2, y2], [x3, y3], [x4, y4]]

        all_points.append(points)
        angles.extend(get_angles(points))

    angles_median = np.median(angles)

    if not auto_rotate:
        angles_median = 0

    center = (W // 2, H // 2)

    all_points = np.array(all_points)
    rotate_matrix = cv2.getRotationMatrix2D(center=center, angle=angles_median, scale=1)

    # all_points = (N, 4, 2)
    # rotate_matrix = (2, 3)

    # rot : (2,2) @ (N,2,4) = (N, 2, 4)
    # move : (N, 2, 4) + (1, 2, 1) = (N, 2, 4)
    rotated_points = (
        rotate_matrix[:2, :2] @ all_points.transpose(0, 2, 1)
    ) + rotate_matrix[:, 2].reshape(1, 2, 1)
    # (N, 4, 2)
    rotated_points = rotated_points.transpose(0, 2, 1).astype(np.int32)

    # tl의 y값 기준 정렬
    centers = np.mean(rotated_points, axis=1)
    sorted_y_order = np.argsort(centers[:, 1])
    sorted_rotated_points = rotated_points[sorted_y_order, :, :]

    template_min_x = rotated_points[:, :, 0].min()
    template_max_x = rotated_points[:, :, 0].max()
    template_min_y = rotated_points[:, :, 1].min()
    template_max_y = rotated_points[:, :, 1].max()

    offset_x, offset_y = max(0, -template_min_x), max(0, -template_min_y)

    template_width = template_max_x + offset_x
    template_height = template_max_y + offset_y
    template_image = np.zeros((template_height, template_width), dtype=np.int32)

    sorted_rotated_points += np.array([offset_x, offset_y]).reshape(1, 1, 2)

    textbox_classes = []

    for index, (current_point, sorted_index) in enumerate(
        zip(sorted_rotated_points, sorted_y_order)
    ):

        orig_info = text_lst[int(sorted_index)]
        textbox_classes.append(TextBox(current_point, orig_info, index + 1))
        cv2.fillConvexPoly(
            template_image, textbox_classes[-1].box, color=textbox_classes[-1].index
        )

    line_counter = 1

    for text_point in textbox_classes:
        if not text_point.processed:

            text_point.line_id = line_counter
            line_counter += 1
            text_point.processed = True
            starting_box = text_point

            current_box = text_point
            while True:
                reached_end, next_index = find_adjacent_box_direction(
                    current_box,
                    template_image,
                    template_width,
                    template_height,
                    step_value=1,
                    direction="left",
                )
                if reached_end:
                    break
                next_text_point = textbox_classes[next_index - 1]
                if not next_text_point.processed:
                    next_text_point.line_id = starting_box.line_id
                    next_text_point.parent = current_box
                    next_text_point.processed = True
                    current_box = next_text_point
                else:
                    break

            current_box = starting_box
            while True:
                reached_end, next_index = find_adjacent_box_direction(
                    current_box,
                    template_image,
                    template_width,
                    template_height,
                    step_value=1,
                    direction="right",
                )
                if reached_end:
                    break
                next_text_point = textbox_classes[next_index - 1]
                if not next_text_point.processed:
                    next_text_point.line_id = starting_box.line_id
                    next_text_point.parent = current_box
                    next_text_point.processed = True
                    current_box = next_text_point
                else:
                    break

    line_grouping_final = defaultdict(list)
    line_centroid = defaultdict(list)

    for text_point in textbox_classes:
        line_grouping_final[text_point.line_id].append(
            (text_point.box[0, 0], text_point)
        )
        line_centroid[text_point.line_id].append(text_point.box.mean(0))

    column_sorted = []
    for current_line_id, line_info in line_grouping_final.items():
        current_line_centroid = np.mean(line_centroid[current_line_id], 0)
        line_centroid_x, line_centroid_y = float(current_line_centroid[0]), float(
            current_line_centroid[1]
        )
        col_sorted_words = sorted(line_info, key=lambda x: x[0])
        col_sorted_words = [i[1] for i in col_sorted_words]
        column_sorted.append((line_centroid_y, line_centroid_x, col_sorted_words))

    row_sorted = sorted(column_sorted, key=lambda x: x[0])
    row_sorted = [i[2] for i in row_sorted]

    line_ordered_output = []
    for line_no, line_info in enumerate(row_sorted):
        for block_no, word_info in enumerate(line_info):
            orig_dict = word_info.orig_info
            orig_dict["line"] = line_no + 1
            orig_dict["block"] = block_no + 1
            orig_dict["x1"] = int(orig_dict["x1"])
            orig_dict["x2"] = int(orig_dict["x2"])
            orig_dict["x3"] = int(orig_dict["x3"])
            orig_dict["x4"] = int(orig_dict["x4"])
            orig_dict["y1"] = int(orig_dict["y1"])
            orig_dict["y2"] = int(orig_dict["y2"])
            orig_dict["y3"] = int(orig_dict["y3"])
            orig_dict["y4"] = int(orig_dict["y4"])
            line_ordered_output.append(orig_dict)

    return line_ordered_output, float(angles_median)


def save_image_sha256(img: Image.Image, img_root: Path, rel_base_dir: Path):
    if img.mode != "RGB":
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)

    sha = hashlib.sha256(buf.getvalue()).hexdigest()
    fpath = img_root / f"{sha}.jpg"
    fpath.parent.mkdir(parents=True, exist_ok=True)
    fpath.write_bytes(buf.getvalue())

    rel_path = os.path.relpath(fpath, rel_base_dir)
    return rel_path, sha


def get_text_from_group(group, words, bboxes, W, H):
    items = []
    for i in group:
        x0, y0, x1, y1 = bboxes[i]
        items.append(
            {
                "text": words[i],
                "x1": int(x0),
                "y1": int(y0),
                "x2": int(x1),
                "y2": int(y0),
                "x3": int(x1),
                "y3": int(y1),
                "x4": int(x0),
                "y4": int(y1),
            }
        )

    items_sorted, _ = get_location_v3(items, W, H, auto_rotate=True)
    merged = " ".join(it["text"] for it in items_sorted).strip()

    x0 = min(it["x1"] for it in items_sorted) / W
    y0 = min(it["y1"] for it in items_sorted) / H
    x1 = max(it["x3"] for it in items_sorted) / W
    y1 = max(it["y3"] for it in items_sorted) / H
    return merged, [round(x0, 4), round(y0, 4), round(x1, 4), round(y1, 4)]


def format_query_key(parent, child=None):
    return f"the {child} included in the {parent}" if child else f"the {parent}"


def convert_arrow_to_vqa_parquet(
    arrow_path: str,
    output_dir: Path,
    image_root: Path,
    base_dir: Path,
    prompt_config_path: Path,
    output_file_name: str = "data.parquet",
):
    with open(prompt_config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        tpl = Template(config["query_template"])

    ds = Dataset.from_file(str(arrow_path))
    output_dir.mkdir(parents=True, exist_ok=True)
    image_root.mkdir(parents=True, exist_ok=True)

    rows = []

    for item in ds:
        image, words, bboxes = item["image"], item["words"], item["bboxes"]
        labels, grouped, links = (
            item["labels"],
            item["grouped_words"],
            item["linked_groups"],
        )
        W, H = image.size
        image_path, _ = save_image_sha256(image, image_root, base_dir)

        group_info = []
        for grp in grouped:
            txt, gbbox = get_text_from_group(grp, words, bboxes, W, H)
            maj = Counter(labels[i] for i in grp).most_common(1)[0][0]
            group_info.append({"text": txt, "bbox": gbbox, "label": maj})

        doc_type = next((g["text"] for g in group_info if g["label"] == 1), None)

        key2vals = defaultdict(set)
        for link in links:
            gset = {gid for gid in link if gid < len(group_info)}
            keys = [g for g in gset if group_info[g]["label"] == 2]
            values = [g for g in gset if group_info[g]["label"] == 3]
            for k in keys:
                key2vals[k].update(values)

        label_json = {}
        for k_gid, v_gids in key2vals.items():
            k_txt = group_info[k_gid]["text"]
            v_txts = {group_info[g]["text"] for g in v_gids}
            v_txts.discard(k_txt)

            if not v_txts:
                continue

            if len(v_txts) == 1:
                val_gid = next(iter(v_gids))
                val_text = next(iter(v_txts))
                label_json[k_txt] = {
                    "<|value|>": val_text,
                    "<|bbox|>": group_info[val_gid]["bbox"],
                }
            else:
                for g in v_gids:
                    val_text = group_info[g]["text"]
                    if val_text != k_txt:
                        label_json[f"{k_txt}::{val_text}"] = {
                            "<|value|>": val_text,
                            "<|bbox|>": group_info[g]["bbox"],
                        }

        if not label_json:
            continue

        q_keys = [format_query_key(k) for k in label_json]
        query = tpl.render(doc_type=doc_type, keys=q_keys)

        rows.append(
            {
                "image_path": image_path,
                "query": query,
                "label": json.dumps(label_json, ensure_ascii=False),
                "width": W,
                "height": H,
            }
        )

    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame(rows)), output_dir / output_file_name
    )
    print(f"✅ Saved: {output_dir / output_file_name}")


def draw_vqa_label_on_image(image_path: Path, label_dict: dict, out_path: Path):
    image = Image.open(image_path).convert("RGB")
    draw = ImageDraw.Draw(image)

    for key, val in label_dict.items():
        if "<|value|>" in val:
            bbox = val["<|bbox|>"]
            color = (0, 255, 0)
            draw.rectangle(
                [
                    bbox[0] * image.width,
                    bbox[1] * image.height,
                    bbox[2] * image.width,
                    bbox[3] * image.height,
                ],
                outline=color,
                width=2,
            )
            draw.text(
                (bbox[0] * image.width, bbox[1] * image.height - 12),
                key,
                fill=color,
            )
        elif "<|children|>" in val:
            for child_key, child_val in val["<|children|>"].items():
                bbox = child_val["<|bbox|>"]
                color = (0, 128, 255)
                draw.rectangle(
                    [
                        bbox[0] * image.width,
                        bbox[1] * image.height,
                        bbox[2] * image.width,
                        bbox[3] * image.height,
                    ],
                    outline=color,
                    width=2,
                )
                draw.text(
                    (bbox[0] * image.width, bbox[1] * image.height - 12),
                    f"{key}.{child_key}",
                    fill=color,
                )

    image.save(out_path)
