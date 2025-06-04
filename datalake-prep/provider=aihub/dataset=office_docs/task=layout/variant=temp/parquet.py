import zipfile
import json
import pdfplumber
import hashlib
import pandas as pd
from pathlib import Path
from collections import defaultdict
from io import BytesIO
from PIL import Image
from tqdm import tqdm
from natsort import natsorted


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
        if "V" not in zip_path_str:
            continue

        if "원문문서" in zip_path_str or "원천데이터(pdf)" in zip_path_str:
            continue

        # zip 이름만 가져와서 폴더명 생성 (확장자 제거)
        extract_subdir = save_dir / zip_path.stem
        extract_subdir.mkdir(parents=True, exist_ok=True)

        print(f"📦 Extracting {zip_path} -> {extract_subdir}")

        # 압축 해제
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_subdir)


def get_pdf_path(
    json_path: str,
) -> str:
    json_path = Path(json_path)

    pdf_parent = f"VS_{json_path.parent.stem.split('_', 2)[1]}_02.원천데이터(pdf)"
    pdf_name = f"OC2_{json_path.stem.rsplit('_', 3)[0].split('_', 1)[1]}.pdf"
    pdf_path = json_path.parents[1] / pdf_parent / pdf_name
    return pdf_path.as_posix()


def read_gt(
    json_path: str,
):
    with open(json_path.as_posix(), "r", encoding="utf-8") as f:
        gt = json.load(f)

    page = int(gt["learning_data_info"]["page_num"])
    desc_key = "plain_text"
    if desc_key not in gt["learning_data_info"]:
        desc_key = "visual_description"
    desc = gt["learning_data_info"][desc_key]
    l, t, r, b = gt["learning_data_info"]["bounding_box"]
    return {
        "page": page,
        "type": TYPE_DICT[gt["learning_data_info"]["class_name"]],
        "description": desc,
        "bbox": [l, t, r, b],
        "resolution": gt["source_data_info"]["document_resolution"]
    }


def get_image_path(
    json_path: str,
    page: int,
) -> str:
    # C:\Users\korea\workspace\office_docs\VS_01.보고서(설명형)_02.원천데이터(pdf)\OC2_240829_TY1-1_1009.pdf
    # C:\Users\korea\workspace\office_docs\VS_01.보고서(설명형)_03.원천이미지데이터(jpg)\OC2_240829_TY1-1_1009_001.jpg
    json_path = Path(json_path)

    image_parent = f"VS_{json_path.parent.stem.split('_', 2)[1]}_03.원천이미지데이터(jpg)"
    image_name = f"OC2_{json_path.stem.rsplit('_', 3)[0].split('_', 1)[1]}_{str(page).zfill(3)}.jpg"
    image_path = json_path.parents[1] / image_parent / image_name
    return image_path.as_posix()


TYPE_DICT = {
    "T01": "title",
    "T02": "???",
    "T03": "text_plain",
    "T04": "list_item",
    "V01": "table",
    "V02": "chart",
    "V03": "digram",
    "V04": "picture",  # picture?
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


if __name__ == "__main__":
    NAS_ROOT = Path("W:/datalake")
    provider = "aihub"
    dataset = "office_docs"
    # script_dir = Path(__file__).resolve().parent
    # images_dir = script_dir / "images"
    data_dir = NAS_ROOT / f"source/provider={provider}/{dataset}"
    unzip = False

    save_dir = Path("C:/Users/korea/workspace/office_docs")

    if unzip:
        unzip_all_zips_in_dir(
            target_dir=data_dir,
            # save_dir=script_dir / "data",
            save_dir=save_dir,
        )

    dpi = 144
    images_dir = Path("C:/Users/korea/workspace/office_docs_done/images")
    target_dir = Path("C:/Users/korea/workspace/office_docs_done/")
    save_images = True
    image_path_dict = defaultdict(lambda: defaultdict(dict))
    elements_dict = defaultdict(list)
    page_dict = {}
    for idx, json_path in enumerate(
        tqdm(
            natsorted(list(save_dir.glob("**/*.json"))),
        ),
        start=1,
    ):
        if json_path.is_dir() or "D01" in json_path.as_posix():
            continue

        data = read_gt(
            json_path=json_path,
        )

        image_path = get_image_path(
            json_path=json_path.as_posix(),
            page=data["page"],
        )
        if not Path(image_path).exists():
            print(image_path)

        ori_image = Image.open(image_path).convert("RGB")
        ori_w, ori_h = ori_image.size
        assert [ori_w, ori_h] == data["resolution"]

        w = int(ori_w * 0.48)
        h = int(ori_h * 0.48)
        image = ori_image.resize(
            (w, h),
            resample=Image.LANCZOS,
        )
        sha256 = sha256_pil_image(
            image,
        )
        save_path = images_dir / f"{sha256[: 2]}/{sha256}.jpg"
        if save_images and not save_path.exists():
            save_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            image.save(
                save_path,
                format="JPEG",
            )

        l, t, r, b = data["bbox"]
        elements_dict[save_path].append(
            {
                "idx": idx,
                "type": data["type"],
                "value": "",
                "description": data["description"],
                "bbox": [l / ori_w, t / ori_h, r / ori_w, b / ori_h],
            }
        )

        # 이미지 경로별 페이지는 별도로 저장:
        page_dict[save_path] = data["page"]

    rows = []
    for image_path, elements in elements_dict.items():
        rel_image_path = Path(image_path).relative_to(
            images_dir,
        ).as_posix()
        page = page_dict[image_path]
        label = {
            "page": page,
            "reading_order": True,
            "elements": elements,
        }
        rows.append(
            {
                "image_path": rel_image_path,
                "width": w,
                "height": h,
                "label": label,
            }
        )
    # Image.open((images_dir / rows[0]["image_path"])).save("C:/Users/korea/workspace/pil_image.jpg")
    df = pd.DataFrame(rows)
    df.to_parquet(
        target_dir / "data.parquet",
        index=False,
    )
