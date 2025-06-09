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
        # zip 이름만 가져와서 폴더명 생성 (확장자 제거)
        extract_subdir = save_dir / zip_path.stem
        extract_subdir.mkdir(parents=True, exist_ok=True)

        print(f"📦 Extracting {zip_path} -> {extract_subdir}")

        # 압축 해제
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_subdir)


def sha256_pil_image(
    image: Image.Image,
) -> str:
    h = hashlib.sha256()
    with BytesIO() as buffer:
        image.save(buffer, format="JPEG")
        h.update(buffer.getvalue())
    return h.hexdigest()


def main(
    unzip: bool = True,
    save_images: bool = True,
    dpi: int = 144,
) -> None:
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "data"
    images_dir = script_dir / "images"

    if unzip:
        unzip_all_zips_in_dir(
            target_dir=NAS_ROOT / f"source/{script_dir.parents[2].stem}/{script_dir.parents[1].stem}",
            save_dir=script_dir / "data",
        )

    gt_paths = [
        i for i in list(data_dir.glob("**/*.json"))
        if "라벨링데이터" in i.parent.as_posix()
    ]
    doc_ids = [gt_path.stem.rsplit("_", 1)[-1]  for gt_path in gt_paths]
    # GT에서 doc id에 중복이 있는지 확인:
    assert len(doc_ids) == len(set(doc_ids))

    # doc id와 page별로 이미지의 경로를 저장:
    rows = []
    data_dict = defaultdict(dict)
    for pdf_path in data_dir.glob("**/*.pdf"):
        if "원천데이터" not in pdf_path.parent.as_posix():
            continue

        doc_id = pdf_path.stem.rsplit("_", 1)[-1]
        # .pdf에서 doc id에 중복이 있는지 확인:
        if doc_id in data_dict:
            print(f"Duplication in doc id: {doc_id}")

        with pdfplumber.open(pdf_path) as pdf:
            for page_no, page in enumerate(
                pdf.pages,
                start=1,
            ):
                image = page.to_image(
                    resolution=dpi,
                    antialias=False,
                )
                pil_image = image.original.convert("RGB")
                width, height = pil_image.size
                sha256 = sha256_pil_image(
                    pil_image,
                )
                save_path = images_dir / f"{sha256[: 2]}/{sha256}.jpg"
                if save_images and not save_path.exists():
                    save_path.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )
                    pil_image.save(
                        save_path,
                        format="JPEG",
                    )

                data_dict[doc_id][page_no] = {
                    "image_path": save_path.relative_to(
                        images_dir,
                    ).as_posix(),
                    "width": width,
                    "height": height,
                }

    for gt_path in tqdm(gt_paths):
        with open(gt_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        doc_id = gt_path.stem.rsplit("_", 1)[-1]

        for row in data["source_data_info"]:
            pages = row["page_no"]

            # QA와 짝지어진 페이지가 여럿인 경우는 제외:
            if len(pages) != 1:
                continue

            for qa in row["qa_data"]:
                user_prompt = qa["question"]
                assistant_prompt = qa["answer"]

                rows.append(
                    {
                        "query": user_prompt,
                        "label": assistant_prompt,
                    } | data_dict[doc_id][pages[0]]
                )

    df = pd.DataFrame(rows)
    df.to_parquet(
        script_dir / "data.parquet",
        index=False,
    )


if __name__ == "__main__":
    # datalake/datalake-prep에서 실행하시오: e.g., `python -m provider=aihub.vis_qa.qa.parquet`.
    import fire

    fire.Fire(main)
