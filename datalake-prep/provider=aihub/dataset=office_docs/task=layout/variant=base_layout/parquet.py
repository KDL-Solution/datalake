import zipfile
import json
import hashlib
import pandas as pd
from pathlib import Path
from collections import defaultdict
from io import BytesIO
from PIL import Image
from tqdm import tqdm
from natsort import natsorted
from typing import List, Dict, Any
from multiprocessing import Pool

from utils import NAS_ROOT


def unzip_all_zips_in_dir(
    target_dir: str,
    save_dir: str,  # 추가: 압축 해제할 루트 디렉토리
) -> None:
    target_dir = Path(target_dir).resolve()
    save_dir = Path(save_dir).resolve()
    save_dir.mkdir(parents=True, exist_ok=True)

    # 모든 하위 폴더를 포함해서 .zip 파일 찾기
    for zip_path in target_dir.rglob("*.zip"):
        zip_path_str = zip_path.as_posix()

        if "원문문서" in zip_path_str or "원천데이터(pdf)" in zip_path_str:
            continue

        # zip 이름만 가져와서 폴더명 생성 (확장자 제거)
        extract_subdir = save_dir / zip_path.stem
        extract_subdir.mkdir(parents=True, exist_ok=True)

        print(f"📦 Extracting {zip_path} -> {extract_subdir}")

        # 압축 해제
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_subdir)


def read_gt(
    json_path: str,
) -> Dict[str, Any]:
    with open(json_path.as_posix(), "r", encoding="utf-8") as f:
        gt = json.load(f)

    page = int(gt["learning_data_info"]["page_num"])
    assert page == int(Path(json_path).stem.rsplit("_", 3)[-3])
    return {
        "page": page,
        "idx": int(Path(json_path).stem.rsplit("_", 2)[-2]),
        "type": TYPE_DICT[gt["learning_data_info"]["class_name"]],
        "value": gt["learning_data_info"].get("plain_text", ""),
        "description": gt["learning_data_info"].get("visual_description", ""),
        "bbox": gt["learning_data_info"]["bounding_box"],
        "resolution": gt["source_data_info"]["document_resolution"]
    }


def get_image_path(
    json_path: str,
    page: int,
) -> str:
    json_path = Path(json_path)

    split = json_path.parent.stem.split("_", 1)[0][0]
    cat = json_path.parent.stem.split('_', 2)[1]
    image_parent = f"{split}S_{cat}_03.원천이미지데이터(jpg)"
    image_name = f"OC2_{json_path.stem.rsplit('_', 3)[0].split('_', 1)[1]}_{str(page).zfill(3)}.jpg"
    image_path = json_path.parents[1] / image_parent / image_name
    return image_path.as_posix()


TYPE_DICT = {
    "T01": "title",
    "T02": "title",
    "T03": "text_plain",
    "T04": "list_item",
    "V01": "table",
    "V02": "chart",
    "V03": "digram",
    "V04": "picture",
    "V05": "figure",
}


def sha256_pil_image(
    image: Image.Image,
) -> str:
    h = hashlib.sha256()
    with BytesIO() as buffer:
        image.save(buffer, format="JPEG")
        h.update(buffer.getvalue())
    return h.hexdigest()


def process_image(
    args,
) -> List[Dict[str, Any]]:
    image_path, data_dir, images_dir, save_images = args

    try:
        ori_image = Image.open(image_path).convert("RGB")
        ori_w, ori_h = ori_image.size

        # DPI 300 -> DPI 144 변환:
        w = int(ori_w * 0.48)
        h = int(ori_h * 0.48)
        image = ori_image.resize((w, h), resample=Image.LANCZOS)

        sha256 = sha256_pil_image(image)
        save_path = images_dir / f"{sha256[:2]}/{sha256}.jpg"
        if save_images and not save_path.exists():
            save_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            image.save(
                save_path,
                format="JPEG",
            )

        split = image_path.parent.stem.split("_", 1)[0][0]
        cat = image_path.parent.stem.split('_', 2)[1]
        trg_json_dir = data_dir / f"{split}L_{cat}_01.json/"

        results = []
        for json_path in natsorted(trg_json_dir.glob("*")):
            if f"OC3_{image_path.stem.split('_', 1)[1]}" not in json_path.as_posix() \
                or json_path.is_dir() \
                or "D01" in json_path.as_posix():
                continue

            gt = read_gt(
                json_path=json_path,
            )

            assert [ori_w, ori_h] == gt["resolution"]

            l, t, r, b = gt["bbox"]
            element = {
                "idx": gt["idx"],
                "type": gt["type"],
                "value": gt["value"],
                "description": gt["description"],
                "bbox": [l / ori_w, t / ori_h, r / ori_w, b / ori_h],
            }
            results.append(
                (save_path.as_posix(), element, gt["page"], w, h)
            )
        return results

    except Exception as e:
        print(f"{e}:\n    {image_path}")
        return [
            (
                "error",
                [],
                0,
                0,
                0,
            ),
        ]


def main(
    cpu_cnt: int = 32,
    unzip: bool = True,
    save_images: bool = True,
) -> None:
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "data"
    images_dir = script_dir / "images"

    if unzip:
        unzip_all_zips_in_dir(
            target_dir=NAS_ROOT / f"source/{script_dir.parents[2].stem}/{script_dir.parents[1].stem}",
            save_dir=data_dir,
        )

    args_ls = [
        (image_path, data_dir, images_dir, save_images)
        for image_path in natsorted(list(Path(data_dir).glob("**/*.jpg")))
    ]

    elements_dict = defaultdict(list)
    image_info_dict = {}
    with Pool(processes=cpu_cnt) as pool:
        for results in tqdm(
            pool.imap_unordered(
                process_image,
                args_ls,
            ),
            total=len(args_ls),
        ):
            for image_path, element, page, w, h in results:
                if image_path == "error":
                    continue

                elements_dict[image_path].append(element)
                image_info_dict[image_path] = {
                    "page": page,
                    "width": w,
                    "height": h,
                }

    rows = []
    for image_path, elements in elements_dict.items():
        rel_image_path = Path(image_path).relative_to(
            images_dir,
        ).as_posix()
        label = {
            "page": image_info_dict[image_path]["page"],
            "reading_order": False,
            "elements": elements,
        }
        rows.append(
            {
                "image_path": rel_image_path,  # `str`.
                "width": image_info_dict[image_path]["width"],  # `int`.
                "height": image_info_dict[image_path]["height"],  # `int`.
                "label": json.dumps(
                    label,
                    ensure_ascii=False,
                ),  # `str`.
            }
        )

    df = pd.DataFrame(rows)
    df.to_parquet(
        script_dir / "data.parquet",
        index=False,
    )


if __name__ == "__main__":
    # datalake/datalake-prep에서 실행하시오: e.g., `python -m provider=aihub.dataset=office_docs.task=layout.variant=base_layout.parquet`.
    import fire

    fire.Fire(main)
