import json, logging
from pathlib import Path
from typing import Any, Dict, List
import hashlib, io
import pandas as pd
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s"
)

SRC_ROOT = Path("/home/ian/workspace/data/doclaynet_extracted")
OUT_ROOT = SRC_ROOT

CLASS_MAP = {
    "Caption": "caption",
    "Footnote": "footnote",
    "Formula": "formula",
    "List-item": "list_item",
    "Page-footer": "page_footer",
    "Page-header": "page_header",
    "Picture": "figure",
    "Section-header": "section_header",
    "Table": "table",
    "Text": "text_plane",
    "Title": "title",
}
IMG_EXT = ".jpg"


def norm_bbox_xywh(b: List[float], w: int, h: int) -> List[float]:
    """COCO bbox [x,y,w,h] → 0‒1 [x1,y1,x2,y2]"""
    x1, y1, bw, bh = b
    x2, y2 = x1 + bw, y1 + bh
    return [
        round(max(0, x1 / w), 4),
        round(max(0, y1 / h), 4),
        round(min(1, x2 / w), 4),
        round(min(1, y2 / h), 4),
    ]


from PIL import Image


def copy_image(src: Path, dst: Path):
    """
    PNG → JPEG(95%) 복사.
    dst 는 최종 .jpg 경로
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(src) as im:
        im.convert("RGB").save(dst, quality=95, optimize=True)


def process_split(split: str):
    coco_dir = SRC_ROOT / "COCO"
    png_dir = SRC_ROOT / "PNG"

    with open(coco_dir / f"{split}.json", encoding="utf-8") as f:
        data = json.load(f)

    cat_id2name = {c["id"]: c["name"] for c in data["categories"]}
    images_meta = {img["id"]: img for img in data["images"]}

    anns_by_img: Dict[int, List[Dict[str, Any]]] = {}
    for ann in data["annotations"]:
        anns_by_img.setdefault(ann["image_id"], []).append(ann)

    split_dir = OUT_ROOT / f"doclaynet_{split}"
    rows = []

    for img_id, img_info in tqdm(images_meta.items(), desc=split):
        src_png = png_dir / img_info["file_name"]
        if not src_png.exists():
            logging.warning("누락 PNG: %s", src_png)
            continue

        rel_img_path = f"images/{img_info['file_name']}"
        sha = hashlib.sha256(src_png.stem.encode()).hexdigest()
        rel_img_path = f"images/{sha[:2]}/{sha}{IMG_EXT}"
        copy_image(src_png, split_dir / rel_img_path)

        W, H = img_info["width"], img_info["height"]

        elems_raw = anns_by_img.get(img_id, [])
        has_ro = any(e.get("precedence") not in (None, 0) for e in elems_raw)
        elems_sorted = (
            sorted(elems_raw, key=lambda x: x.get("precedence", 0))
            if has_ro
            else elems_raw
        )

        elements = []
        for i, ann in enumerate(elems_sorted, 1):
            cls_raw = cat_id2name.get(ann["category_id"], "other")
            elements.append(
                {
                    "idx": ann.get("precedence", i) if has_ro else i,
                    "type": CLASS_MAP.get(cls_raw, "other"),
                    "value": "",
                    "description": "",
                    "bbox": norm_bbox_xywh(ann["bbox"], W, H),
                }
            )

        label = {
            "page": img_info["page_no"],
            "reading_order": has_ro,
            "elements": elements,
        }

        rows.append(
            {
                "image_path": rel_img_path,
                "width": W,
                "height": H,
                "label": json.dumps(label, ensure_ascii=False),
            }
        )

    split_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(split_dir / "data.parquet", index=False)
    logging.info("✓ %s 저장 (%d rows)", split_dir / "data.parquet", len(rows))


if __name__ == "__main__":
    for sp in ["train", "val", "test"]:
        process_split(sp)
    logging.info("ALL DONE ✓")
