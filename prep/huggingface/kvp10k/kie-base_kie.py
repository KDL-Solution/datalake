import requests
import json
import json
import requests
import fitz  # PyMuPDF
import requests
import json
import re
import subprocess
from datasets import load_dataset, Dataset
from pathlib import Path
from typing import List, Dict, Any
from urllib.parse import urlparse
from pathlib import Path
from pathlib import Path
from PIL import Image
from urllib.parse import urlparse, unquote
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# import sys
# sys.path.insert(0, "/home/eric/workspace/datalake/")
from prep.utils import get_safe_image_hash_from_pil


def sanitize(
    s: str,
) -> str:
    """Windows에서 사용할 수 없는 문자 제거
    """
    return re.sub(r'[<>:"/\\|?*]', '_', s)


def url_to_filename(
    url: str,
) -> str:
    filename = url.split("/")[-1].split("?")[0]
    filename = sanitize(
        filename,
    )
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    return filename


def download(
    url: str,
    pdfs_dir: str,
) -> str:
    filename = url_to_filename(
        url,
    )

    output_path = Path(pdfs_dir) / filename
    if output_path.exists():
        return

    try:
        subprocess.run(
            [
                "wget",
                "--quiet",
                "--tries=1",
                "--timeout=20",
                "--max-redirect=20",
                "-nc",
                url,
                "-O",
                output_path.as_posix(),
            ],
            check=True
        )
        # return f"✅ Downloaded: {url}"
    except subprocess.CalledProcessError:
        # return f"❌ Failed: {url}"
        return


def download_pdfs(
    pdf_urls: List[str],
    save_dir: str,
):
    if not isinstance(pdf_urls, list):
        pdf_urls = list(pdf_urls)
    pdf_urls.sort()

    # .txt 파일로 저장:
    txt_path = Path(__file__).resolve().parent / "pdf_urls.txt"
    with open(txt_path.as_posix(), "w") as f:
        for pdf_url in pdf_urls:
            if pdf_url:  # 빈 URL 제외
                _ = f.write(pdf_url + "\n")

    # pdfs_dir = Path(__file__).resolve().parent / "pdfs"
    save_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 병렬 다운로드 수행
    max_workers = 16  # 병렬 개수 조정 가능
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download, url) for url in pdf_urls]
        # for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading PDFs"):
        #     result = future.result()
        #     if result:
        #         tqdm.write(result)  # 다운로드되었거나 실패한 경우만 출력
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Downloading PDFs"):
            pass


# def build_dynamic_headers(
#     url: str,
# ) -> dict:
#     parsed = urlparse(url)
#     host = parsed.netloc
#     referer = f"{parsed.scheme}://{host}"
#     return {
#         "User-Agent": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/122.0.0.0 Safari/537.36"
#         ),
#         "Accept": "application/pdf,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#         "Accept-Encoding": "gzip, deflate, br",
#         "Accept-Language": "en-US,en;q=0.5",
#         "Referer": referer,
#         "Connection": "keep-alive",
#         "Upgrade-Insecure-Requests": "1",
#         "Host": host,
#     }


class KVP10kProcessor(object):
    def __init__(
        self,
        max_redirects: int = 5,
    ):
        self.failed = set()
        self.session = requests.Session()
        self.session.max_redirects = max_redirects

    # def download_pdf(
    #     self,
    #     pdf_url: str,
    #     save_dir: str,
    #     chunk_size: int = 8192,
    #     timeout: int = 5,
    # ) -> Path:
    #     if pdf_url in self.failed:
    #         return False

    #     filename = Path(unquote(urlparse(pdf_url).path)).name
    #     filename = sanitize_filename(
    #         filename,
    #     )
    #     output_path = Path(save_dir) / filename
    #     if output_path.exists():
    #         return True

    #     try:
    #         response = self.session.get(
    #             pdf_url,
    #             headers=build_dynamic_headers(
    #                 pdf_url,
    #             ),
    #             stream=True,
    #             timeout=timeout,
    #             allow_redirects=False,
    #         )
    #         response.raise_for_status()

    #         content_type = response.headers.get("Content-Type", "")
    #         if "application/pdf" not in content_type:
    #             self.failed.add(pdf_url)
    #             return False

    #         output_path.parent.mkdir(
    #             parents=True,
    #             exist_ok=True,
    #         )
    #         with open(output_path.as_posix(), "wb") as f:
    #             for chunk in response.iter_content(chunk_size=chunk_size):
    #                 if chunk:
    #                     f.write(chunk)
    #         return True
    #     except:
    #         self.failed.add(pdf_url)
    #         return False

    def save_images_and_generate_labels(
        self,
        batch: Dict[str, List[Any]],
        pdfs_dir: str,
        images_dir: str,
        dpi: int = 144,
        indent: int = None,
    ) -> Dict[str, List[Any]]:
        images_dir = Path(images_dir).resolve()

        image_paths = []
        widths = []
        heights = []
        labels = []
        for example in zip(*batch.values()):
            image_path = width = height = label_str = None

            example = dict(zip(batch.keys(), example))

            # filename = Path(unquote(urlparse(example["image_url"]).path)).name
            filename = url_to_filename(
                example["image_url"],
            )
            pdf_path = Path(pdfs_dir) / filename
            if not pdf_path.exists():
                continue

            try:
                pdf = fitz.open(pdf_path)
                # pymupdf.FileDataError
                page = pdf[example["page_number"] - 1]
            except:
                continue

            pix = page.get_pixmap(
                matrix=fitz.Matrix(
                    dpi / 72,
                    dpi / 72,
                ),
            )
            image = Image.frombytes(
                "RGB",
                [pix.width, pix.height],
                pix.samples,
            )
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

            data = {}
            for annot in example["annotations"]:
                linking = annot["attributes"].get("Linking")
                if linking is None:
                    continue

                points = [(pt['x'], pt['y']) for pt in annot["coordinates"]]
                xs = [pt[0] for pt in points]
                ys = [pt[1] for pt in points]
                left = min(xs)
                top = min(ys)
                right = max(xs)
                bottom = max(ys)
                rect = fitz.Rect(
                    left * page.rect.width,
                    top * page.rect.height,
                    right * page.rect.width,
                    bottom * page.rect.height,
                )
                text = page.get_text(
                    "text",
                    clip=rect,
                ).strip()

                data[annot["_id"]] = {
                    "text": text,
                    "key": linking["value"],
                    "bbox": [
                        round(left * width),
                        round(top * height),
                        round(right * width),
                        round(bottom * height),
                    ],
                }

            if data:
                label_dict = {}
                for k, v in data.items():
                    if v["key"] in data:
                        value = data[k]["text"].strip()
                        if value:
                            label_dict[data[v["key"]]["text"]] = {
                                "<|value|>": value,
                                "<|bbox|>": data[k]["bbox"],
                            }

                if label_dict:
                    label_str = json.dumps(
                        label_dict,
                        ensure_ascii=False,
                        indent=indent,
                    )
            image_paths.append(
                Path(*image_path.parts[-2:]).as_posix()
            )
            widths.append(width)
            heights.append(height)
            labels.append(label_str)
        return {
            "image_path": image_paths,
            "width": widths,
            "height": heights,
            "label": labels,
        }

    def filter_valid_pdfs(
        self,
        batch: Dict[str, List[Any]],
            pdfs_dir: str,
        ) -> List[bool]:
            keep = []
            for example in zip(*batch.values()):
                example = dict(zip(batch.keys(), example))

                keep.append(
                    self.download_pdf(
                        pdf_url=example["image_url"],
                        save_dir=pdfs_dir,
                    )
                )
            return keep

    def export(
        self,
        dataset: Dataset,
        pdfs_dir: str,
        images_dir: str,
        parquet_path: str,
        dpi: int = 144,
        indent: int = None,
        batch_size: int = 32,
    ):
        print(len(dataset))
        dataset = dataset.filter(
            lambda x: (
                Path(pdfs_dir) / url_to_filename(
                    x["image_url"],
                )
            ).exists()
        )
        print(len(dataset))

        dataset = dataset.map(
            lambda x: self.save_images_and_generate_labels(
                batch=x,
                pdfs_dir=pdfs_dir,
                images_dir=images_dir,
                dpi=dpi,
                indent=indent,
            ),
            batched=True,
            batch_size=batch_size,
            remove_columns=dataset.column_names,
        )
        dataset = dataset.filter(
            lambda x: x["label"] is not None,
            desc="Filtering valid samples",
        )
        print(len(dataset))

        parquet_path = Path(parquet_path).resolve()
        parquet_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        dataset.to_pandas().to_parquet(
            parquet_path.as_posix(),
            index=False,
        )


def main(
    download_pdfs: bool = True,
) -> None:
    from prep.utils import DATALAKE_DIR

    DATALAKE_DIR = "/mnt/AI_NAS/datalake"
    dataset="kvp10k"
    pdfs_dir = Path(DATALAKE_DIR) / f"archive/source/provider=huggingface/dataset={dataset}"
    train_dataset, test_dataset = load_dataset(
        "parquet",
        data_files={
            "train": (pdfs_dir / "train/*.parquet").as_posix(),
            "test": (pdfs_dir / "test/*.parquet").as_posix(),
        },
        split=[
            "train",
            "test",
        ],
    )
    pdfs_dir = "/home/eric/workspace/datalake/prep/huggingface/kvp10k/pdfs"

    if download_pdfs:
        # pdf_urls = list(set(train_dataset["image_url"]) | set(test_dataset["image_url"]))
        # download_pdfs(
        #     pdf_urls=pdf_urls,
        #     save_dir = Path("/home/eric/workspace/datalake/prep/huggingface/kvp10k/pdfs"),
        # )

    processor = KVP10kProcessor()
    processor.export(
        dataset=train_dataset,
        pdfs_dir=pdfs_dir,
        images_dir="/home/eric/workspace/datalake/prep/huggingface/kvp10k/images_train",
        parquet_path="/home/eric/workspace/datalake/prep/huggingface/kvp10k/train.parquet",
    )
    processor.export(
        dataset=test_dataset,
        pdfs_dir=pdfs_dir,
        images_dir="/home/eric/workspace/datalake/prep/huggingface/kvp10k/images_test",
        parquet_path="/home/eric/workspace/datalake/prep/huggingface/kvp10k/test.parquet",
    )
if __name__ == "__main__":
    import fire

    fire.Fire(
        main,
    )
