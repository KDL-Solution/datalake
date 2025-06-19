import random
import pandas as pd
from pathlib import Path
from typing import List
from PIL import Image
from typing import List, Tuple
from pathlib import Path
from PIL import Image

from prep.utils import DATALAKE_DIR
from export.utils import save_df_as_jsonl, user_prompt_dict


class RecogntionCharExporter(object):
    def __init__(
        self,
        datalake_dir: str = DATALAKE_DIR.as_posix(),
        user_prompt = user_prompt_dict["recognition"],
        charset_path: str = (
            Path(__file__).resolve().parent / "charset.txt"
        ).as_posix(),
    ):
        self.user_prompt = user_prompt
        self.datalake_dir = datalake_dir

        with open(charset_path, encoding="utf-8") as f:
            self.charset = set(f.read().strip())

    def _is_text_valid(
        self,
        text: str,
    ) -> bool:
        return all(i in self.charset for i in text)

    def export(
        self,
        df: pd.DataFrame,
        jsonl_path: str,
    ) -> None:
        df_copied = df.copy()

        df = df[
            df["label"].apply(
                lambda x: self._is_text_valid(
                    str(x),
                ),
            )
        ]

        df_copied["path"] = df_copied["path"].apply(
            lambda x: (Path(self.datalake_dir) / "assets" / x).as_posix(),
        )
        df_copied["query"] = self.user_prompt

        save_df_as_jsonl(
            df=df_copied,
            jsonl_path=jsonl_path,
        )

    def _compose(
        self, 
        images: List[Image.Image],
        labels: List[str],
        num_paragraphs: int,
        images_per_paragraph: int,
        x_padding: int = 10,
        y_padding: int = 10,
        bg_color: Tuple[int, int, int] = (255, 255, 255)
    ) -> Tuple[Image.Image, str]:
        assert len(images) == len(labels), "Number of images and labels must match"

        # 1. shuffle
        paired = list(zip(images, labels))
        random.shuffle(paired)
        images, labels = zip(*paired)

        assert num_paragraphs * images_per_paragraph <= len(images), \
            "Not enough images to fill all paragraphs."

        paragraph_images = []
        paragraph_texts = []

        for p in range(num_paragraphs):
            # 이미지 및 텍스트 모으기
            start = p * images_per_paragraph
            end = start + images_per_paragraph
            imgs = images[start:end]
            txts = labels[start:end]

            # 세로로 쌓기
            widths, heights = zip(*(img.size for img in imgs))
            max_width = max(widths)
            total_height = sum(heights) + y_padding * (len(imgs) - 1)

            paragraph = Image.new("RGB", (max_width, total_height), color=bg_color)
            y = 0
            for img in imgs:
                paragraph.paste(img, (0, y))
                y += img.height + y_padding

            paragraph_images.append(paragraph)
            paragraph_texts.append("\n".join(txts))  # 하나의 문단 텍스트

        # 문단들을 좌→우로 붙이기
        total_width = sum(p.width for p in paragraph_images) + x_padding * (num_paragraphs - 1)
        max_height = max(p.height for p in paragraph_images)

        final_image = Image.new("RGB", (total_width, max_height), color=bg_color)
        x = 0
        for p_img in paragraph_images:
            final_image.paste(p_img, (x, 0))
            x += p_img.width + x_padding

        # 전체 읽기 순서 기준 텍스트
        full_text = "\n".join(paragraph_texts)
        return final_image, full_text

    def export_with_composition(
        self,
        df: pd.DataFrame,
        images_dir: str,
        num_paragraphs: int,
        images_per_paragraph: int,
        jsonl_path: str,
        x_padding: int = 10,
        y_padding: int = 10,
    ) -> None:
        df_copied = df.copy()

        df_copied["path"] = df_copied["path"].apply(
            lambda x: (Path(self.datalake_dir) / x).as_posix(),
        )

        image_paths = df_copied["path"].tolist()
        labels = df_copied["label"].astype(str).tolist()
        total = len(image_paths)

        group_size = num_paragraphs * images_per_paragraph
        num_batches = total // group_size

        export_data = []

        for i in range(num_batches):
            start = i * group_size
            end = start + group_size

            batch_paths = image_paths[start:end]
            batch_labels = labels[start:end]
            batch_images = [Image.open(p).convert("RGB") for p in batch_paths]

            composed_image, composed_text = self._compose(
                images=batch_images,
                labels=batch_labels,
                num_paragraphs=num_paragraphs,
                images_per_paragraph=images_per_paragraph,
                x_padding=x_padding,
                y_padding=y_padding,
            )

            image_filename = f"composed_{i:04d}.jpg"
            image_path = Path(images_dir) / image_filename
            image_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            composed_image.save(image_path)

            export_data.append(
                {
                    "path": image_path.as_posix(),
                    "query": self.user_prompt,
                    "label": composed_text,
                }
            )

        save_df_as_jsonl(
            df=pd.DataFrame(export_data),
            jsonl_path=jsonl_path,
        )


if __name__ == "__main__":
    import duckdb

    conn = duckdb.connect()
    read_parquet = "read_parquet('/mnt/AI_NAS/datalake/catalog/provider=*/dataset=*/task=*/variant=*/data.parquet', union_by_name=True, filename=True, hive_partitioning=True)"
    sql=f"""SELECT *
    FROM {read_parquet}
    WHERE dataset = 'diverse_ocr_char' or dataset = 'diverse_ocr_word'
        AND task != 'raw'"""
    df = conn.execute(
        sql,
    ).fetchdf()

    exporter = RecogntionCharExporter()

    exporter.export(
        df=df,
        jsonl_path="/home/eric/workspace/datalake/export/data/diverse_ocr.jsonl",
    )
    # exporter.export_with_composition(
    #     df=df,
    #     num_paragraphs=8,
    #     images_per_paragraph=20,
    #     images_dir="/home/eric/workspace/diverse_ocr_images",
    #     jsonl_path="/home/eric/workspace/Qwen-SFT/diverse_ocr_char_composed.jsonl",
    #     x_padding=80,
    #     y_padding=20,
    # )
