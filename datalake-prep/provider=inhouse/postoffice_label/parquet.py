from __future__ import annotations

import json, re, hashlib, logging, shutil
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from PIL import Image
from alive_progress import alive_it

SRC_DIRS: list[Path] = [
    Path("/mnt/AI_NAS/Data/LabelStudio/label_post_detection_partition1_connetion"),
    Path("/mnt/AI_NAS/Data/LabelStudio/label_post_detection"),
]
LS_ROOT = Path("/mnt/AI_NAS/Data/LabelStudio")
OUT_ROOT = Path("/home/ian/workspace/data/datalake/postoffice_labeling")
IMG_ROOT = OUT_ROOT / "images"
PARQUET_FP = OUT_ROOT / "parquet" / "data.parquet"
SKIP_LIST = OUT_ROOT / "skip_no_lines.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)8s  %(message)s",
)
logger = logging.getLogger("postoffice")

sha256_hex = lambda s: hashlib.sha256(s.encode()).hexdigest()
NULL_SET = {"none", "null", ""}


class PostOfficeProcessor:
    """전체 JSON → parquet 변환 + 이미지 리사이즈"""

    def __init__(self) -> None:
        IMG_ROOT.mkdir(parents=True, exist_ok=True)
        PARQUET_FP.parent.mkdir(parents=True, exist_ok=True)
        self.skip_no_lines: list[str] = []

    @staticmethod
    def _resolve_image_path(raw: str) -> Optional[Path]:
        m = re.search(r"\?d=(.*)", raw)
        if not m:
            return None
        path_str = m.group(1).lstrip("/")
        for prefix in ("label-studio/data/", "data/"):
            if path_str.startswith(prefix):
                path_str = path_str[len(prefix) :]
        return LS_ROOT / path_str

    def _hash_path(self, abs_src: Path, uniq_key: str, two_tier: bool) -> Path:
        h = sha256_hex(uniq_key)
        if two_tier:
            return Path("images", h[:2], f"{h}{abs_src.suffix.lower()}")
        return Path("images", f"{h}{abs_src.suffix.lower()}")

    def _resize_and_copy(
        self, src: Path, dst: Path, max_size: int = 2048
    ) -> tuple[int, int]:
        try:
            with Image.open(src) as img:
                orig_w, orig_h = img.width, img.height
                if orig_w > max_size or orig_h > max_size:
                    img.thumbnail((max_size, max_size), Image.LANCZOS)
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    img.save(dst)
                    # logger.info(
                    #     "리사이즈 %s → (%d, %d)", src.name, img.width, img.height
                    # )
                    return img.width, img.height
                else:
                    shutil.copy2(src, dst)
                    return orig_w, orig_h
        except Exception as e:
            logger.warning("이미지 리사이즈/복사 실패 %s → %s : %s", src, dst, e)
            return 0, 0

    def _process_json(self, jf: Path, two_tier: bool) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(jf.read_text("utf-8"))
        except Exception as e:
            logger.warning("json load err  %s : %s", jf, e)
            return None

        ann = data.get("annotations", {})
        lines = ann.get("lines", {}).get("items")
        if not lines:
            self.skip_no_lines.append(str(jf))
            return None

        meta = data.get("metadata", {})
        W, H = int(meta.get("image_width", 0)), int(meta.get("image_height", 0))
        if not W or not H:
            return None

        abs_img = self._resolve_image_path(meta.get("image_path", ""))
        if not abs_img or not abs_img.exists():
            logger.warning("missing image %s", meta.get("image_path", ""))
            return None

        cls_info = meta.get("class_info", {})
        id2name = {int(cid): info["name"] for cid, info in cls_info.items()}
        dup_ok = {
            int(cid): ("중복 태깅 가능" in info.get("tags", []))
            for cid, info in cls_info.items()
        }

        words_items: dict[str, Any] = ann["words"]["items"]
        label_core: dict[str, list | dict] = defaultdict(list)

        for line in lines.values():
            word_ids: list[str] = line.get("word_ids", [])
            if not word_ids:
                continue

            xs, ys = [], []
            for wid in word_ids:
                w = words_items.get(wid)
                if not w:
                    continue
                for x, y in w["coordinates"]["points"]:
                    xs.append(x)
                    ys.append(y)
            if not xs:
                continue
            lx1, ly1, lx2, ly2 = min(xs) * W, min(ys) * H, max(xs) * W, max(ys) * H
            line_bbox_norm = [
                round(lx1 / W, 4),
                round(ly1 / H, 4),
                round(lx2 / W, 4),
                round(ly2 / H, 4),
            ]

            classes_in_line: set[int] = set()
            for wid in word_ids:
                w = words_items.get(wid)
                if w and w["class"]["content"]:
                    classes_in_line.update(w["class"]["content"])

            if len(classes_in_line) == 1:
                cid = next(iter(classes_in_line))
                cname = id2name.get(cid, "Unknown")
                texts_in_line = [
                    (words_items[wid]["text"]["content"] or "").strip()
                    for wid in word_ids
                    if (words_items[wid]["text"]["content"] or "").strip().lower()
                    not in NULL_SET
                ]
                joined_txt = " ".join(texts_in_line).strip() if texts_in_line else ""
                entry = {"<|value|>": joined_txt, "<|bbox|>": line_bbox_norm}
                label_core[cname].append(entry)
                continue

            class_to_words: dict[int, list[dict]] = defaultdict(list)
            for wid in word_ids:
                w = words_items.get(wid)
                if not w:
                    continue
                raw_txt = (w["text"]["content"] or "").strip()
                if raw_txt.lower() in NULL_SET:
                    continue
                wxs, wys = zip(*w["coordinates"]["points"])
                wx1, wy1, wx2, wy2 = (
                    min(wxs) * W,
                    min(wys) * H,
                    max(wxs) * W,
                    max(wys) * H,
                )
                for cid in w["class"]["content"] or []:
                    class_to_words[cid].append(
                        {
                            "text": raw_txt,
                            "x1": wx1,
                            "y1": wy1,
                            "x2": wx2,
                            "y2": wy2,
                        }
                    )

            for cid, ws in class_to_words.items():
                if not ws:
                    continue
                cname = id2name.get(cid, "Unknown")
                ws_sorted = sorted(ws, key=lambda d: d["x1"])
                txt_join = " ".join(w["text"] for w in ws_sorted).strip()
                xs_all = [w["x1"] for w in ws] + [w["x2"] for w in ws]
                ys_all = [w["y1"] for w in ws] + [w["y2"] for w in ws]
                entry = {
                    "<|value|>": txt_join,
                    "<|bbox|>": [
                        round(min(xs_all) / W, 4),
                        round(min(ys_all) / H, 4),
                        round(max(xs_all) / W, 4),
                        round(max(ys_all) / H, 4),
                    ],
                }
                label_core[cname].append(entry)

        clean_core: dict[str, Any] = {}
        for cname, vals in label_core.items():
            if not vals:
                continue
            cid_list = [cid for cid, n in id2name.items() if n == cname]
            cid = cid_list[0] if cid_list else None
            allow_dup = dup_ok.get(cid, True)
            clean_core[cname] = vals if allow_dup else vals[0]

        if not clean_core:
            return None

        rel_img_path = self._hash_path(abs_img, abs_img.as_posix(), two_tier)
        dst_img = OUT_ROOT / rel_img_path
        resized_w, resized_h = self._resize_and_copy(abs_img, dst_img)
        if resized_w > 0 and resized_h > 0:
            W, H = resized_w, resized_h

        return {
            "image_path": rel_img_path.as_posix(),
            "width": W,
            "height": H,
            "label": json.dumps({"post_office": clean_core}, ensure_ascii=False),
        }

    def run(self) -> None:
        json_files: list[Path] = [p for src in SRC_DIRS for p in src.rglob("*.json")]
        if not json_files:
            logger.error("JSON 파일이 없습니다.")
            return

        two_tier = len(json_files) >= 10_000
        logger.info(
            "총 JSON %d → %s-tier SHA-256 경로 사용",
            len(json_files),
            "2" if two_tier else "1",
        )

        rows: list[dict[str, Any]] = []
        for jf in alive_it(json_files, title="PostOffice"):
            rec = self._process_json(jf, two_tier)
            if rec:
                rows.append(rec)

        if rows:
            pd.DataFrame(rows).to_parquet(PARQUET_FP, index=False)
            logger.info("✓ saved %s  (%d rows)", PARQUET_FP, len(rows))
        else:
            logger.warning("저장할 레코드가 없습니다.")

        if self.skip_no_lines:
            SKIP_LIST.write_text("\n".join(self.skip_no_lines), "utf-8")
            logger.info("라인 없는 JSON %d건 → %s", len(self.skip_no_lines), SKIP_LIST)


if __name__ == "__main__":
    PostOfficeProcessor().run()
