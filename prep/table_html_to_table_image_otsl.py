import imgkit
import random
import regex
import re
import cv2
import textwrap
import numpy as np
from pathlib import Path
from io import BytesIO
from PIL import Image
import latex2mathml.converter

# import sys
# sys.path.insert(0, '/home/eric/workspace/datalake/')
from core.datalake import DatalakeClient
from prep.utils import HTMLToOTSL


class HTMLStyler(object):
    def __init__(
        self,
        font_dir: str = "/mnt/AI_NAS/OCR/Font/",
        seed: int = 42,
    ) -> None:
        self.font_dir = Path(font_dir).resolve()
        self.font_paths = [i.as_posix() for i in self.font_dir.glob("*")]
        self.pattern_word_split = regex.compile(
            r"\S+|\s+",
        )
        self.pattern_text_node = re.compile(
            r">(.*?)<",
            flags=re.DOTALL,
        )

        self.rng = random.Random(seed)

    def style(
        self,
        html: str,
        grid: bool =True,
        header: bool = True,
        padding: bool = True,
        style_words: bool = True,
        shadow_prob: float = 0.,
        bold_prop: float = 0.3,
        color_underline_prob: float = 0.3,
    ) -> str:
        def _get_random_pastel_color():
            r = self.rng.randint(150, 220)
            g = self.rng.randint(150, 220)
            b = self.rng.randint(150, 220)
            return f"rgb({r}, {g}, {b})"

        def _randomly_style(
            text_node: str,
        ) -> str:
            if "\\" in text_node:  # 수식 처리.
                latex = text_node.replace("\\( (", "(").replace(") \\)", ")")
                mathml = latex2mathml.converter.convert(
                    latex,
                )
                mathml = mathml.replace("\\", "")
                return f"{mathml}"
            else:
                words = regex.findall(
                    self.pattern_word_split,
                    text_node,
                )

                styled_words = []
                for word in words:
                    if word.isspace():
                        styled_words.append(word)
                        continue

                    styled = word
                    # 확률적으로 bold:
                    if self.rng.random() < bold_prop:
                        styled = f"<b>{styled}</b>"

                    # 확률적으로 색상/밑줄 스타일 적용:
                    if self.rng.random() < color_underline_prob:
                        style_choice = self.rng.choice(
                            [
                                "blue_underline",
                                # "underline",
                                "red",
                            ],
                        )
                        if style_choice == "blue_underline":
                            styled = f"<span style='color:blue;text-decoration:underline'>{styled}</span>"
                        elif style_choice == "underline":
                            styled = f"<span style='text-decoration:underline'>{styled}</span>"
                        elif style_choice == "red":
                            styled = f"<span style='color:red'>{styled}</span>"
                    styled_words.append(styled)
                return "".join(styled_words)

        styles = [
            textwrap.dedent("""
                <style>
                    /* 1) 테이블 고정 레이아웃 & 100% 폭 */
                    table {
                        table-layout: auto;
                        border-collapse: collapse;
                    }
                    /* 2) 셀 내부 넘치는 부분 숨기기 */
                    td, th {
                        overflow: hidden;
                    }
                    /* 3) 이미지가 셀 크기에 맞춰 축소되도록 */
                    td img {
                        display: block;       /* inline 여백 제거 */
                        max-width: 100%;      /* 부모 td 폭을 넘지 않음 */
                        height: auto;         /* 가로세로 비율 유지 */
                    }
            """)
        ]

        font_path = self.rng.choice(self.font_paths)
        font_uri = f"file://{font_path}"
        # font_path에 한글이 포함되어 있는지 확인:
        if any("\uac00" <= ch <= "\ud7a3" for ch in Path(font_path).name):  # 한글 유니코드 범위
            font_size = "1.125rem"
        else:
            font_size = "1rem"

        styles.append(
            f"@font-face{{font-family:'CustomFont';src:url('{font_uri}') format('truetype');}}"
            f"""*{{
                font-family:'CustomFont', sans-serif;
                font-size: {font_size};
            }}"""
        )

        if grid:
            styles.append(
                """td, th {
                    border: 1px solid #333;
                }"""
            )
        if header:
            pastel_color = _get_random_pastel_color()
            styles.append(
                f"""th {{
                    background-color: {pastel_color};
                    color: black;
                    font-weight: bold;
                }}"""
            )
        if padding:
            styles.append(
                """td, th {
                    padding: 0.5em;
                    text-align: center;
                }"""
            )
        if self.rng.random() < shadow_prob:
            styles.append(
                "table { box-shadow: 6px 6px 6px rgba(0,0,0,0.5); }"
            )

        styles.append("</style>")

        # 스타일 적용:
        if style_words:
            html = re.sub(
                self.pattern_text_node,
                lambda x: f">{_randomly_style(x.group(1))}<",
                html,
            )
        return regex.sub(r"(?=<table\b)", "\n".join(styles), html)


class HTMLRenderer(object):
    def __init__(
        self,
        seed: int = 42,
        min_margin: int = 10,
        max_margin: int = 40,
    ) -> None:
        self.min_margin = min_margin
        self.max_margin = max_margin

        self.rng = random.Random(seed)

    def _crop(
        self,
        image_bytes: bytes,
    ) -> bytes:
        left_margin = self.rng.randint(
            self.min_margin,
            self.max_margin,
        )
        top_margin = self.rng.randint(
            self.min_margin,
            self.max_margin,
        )
        right_margin = self.rng.randint(
            self.min_margin,
            self.max_margin,
        )
        bottom_margin = self.rng.randint(
            self.min_margin,
            self.max_margin,
        )

        img = np.array(Image.open(BytesIO(image_bytes)).convert("RGB"))
        img = cv2.copyMakeBorder(
            img,
            left=left_margin,
            top=top_margin,
            right=right_margin,
            bottom=bottom_margin,
            borderType=cv2.BORDER_REPLICATE,
        )
        image = Image.fromarray(img)

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)
        ys, xs = np.where(thresh == 255)
        left = xs.min() - left_margin
        top = xs.max() + top_margin
        right = ys.min() - right_margin
        bottom = ys.max() + bottom_margin
        image_crop = image.crop(
            (left, right, top, bottom),
        )
        buffer = BytesIO()
        image_crop.save(
            buffer,
            format="JPEG",
        )
        return buffer.getvalue()

    def render(
        self,
        html: str,
        zoom: float = 1.0,
    ) -> bytes:
        image_bytes = imgkit.from_string(
            html,
            output_path=False,
            options={
                "enable-local-file-access": "",
                "quiet": "",
                "zoom": zoom,
            },
        )
        return self._crop(
            image_bytes,
        )


def main(
    batch_size: int = 16,
    num_proc: int = 64,
    mod: str = "table",
) -> None:
    client = DatalakeClient()

    search_results = client.search(
        variants=[
            "table_html",
        ],
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    dataset = client.to_dataset(
        search_results,
        absolute_paths=False,
    )
    # dataset = dataset.filter(
    #     lambda x: x["dataset"] == "tech_sci_mrc",
    # )  # TEMP!
    # 수식이 포함된 표 제거:
    dataset = dataset.filter(
        lambda example: "\\" not in example["label"]
    )
    # dataset = dataset.shuffle()  # TEMP!
    # dataset = dataset.select(range(16))  # TEMP!

    html_styler = HTMLStyler()
    dataset = dataset.map(
        lambda x: {
            "html": [
                html_styler.style(
                    i,
                ) for i in x["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    dataset = dataset.map(
        lambda x: {
            "image": [
                render(
                    i,
                ) for i in x["html"]
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )

    converter = HTMLToOTSL()
    dataset = dataset.map(
        lambda x: {
            "label": [
                converter.convert(
                    i,
                ) for i in x["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )

    for dataset_name in dataset.unique("dataset"):
        dataset_filter = dataset.filter(
            lambda x: x["dataset"] == dataset_name,
        )
        _, _ = client.upload_task(
            data_file=dataset_filter,
            provider=dataset_filter.unique("provider")[0],
            dataset=dataset_name,
            task="document_conversion",
            variant="table_image_otsl",
            meta={
                "lang": dataset_filter.unique("lang")[0],
                "src": dataset_filter.unique("src")[0],
                "mod": mod,
            },
            overwrite=True,
        )

    job_id = client.trigger_nas_processing()
    client.wait_for_job_completion(
        job_id,
    )
    client.build_catalog_db(
        force_rebuild=True,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main,
    )
