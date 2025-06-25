import random
import imgkit
import random
import regex
import re
import cv2
import numpy as np
from pathlib import Path
from io import BytesIO
from PIL import Image
import latex2mathml.converter

# import sys
# sys.path.insert(0, '/home/eric/workspace/datalake/')
from export.utils import HTMLToDogTags
from core.datalake import DatalakeClient


class HTMLRenderer(object):
    def __init__(
        self,
        font_dir: str = "/mnt/AI_NAS/OCR/Font/",
    ) -> None:
        self.font_paths = [i.as_posix() for i in Path(font_dir).glob("*")]
        self.pattern_word_split = regex.compile(
            r"\S+|\s+",
        )
        self.pattern_text_node = re.compile(
            r">(.*?)<",
            flags=re.DOTALL,
        )
        # self.mathjax_script = """
        # <script>
        #     window.MathJax = {
        #         tex: { inlineMath: [['$', '$'], ['\\(', '\\)']] },
        #         svg: { fontCache: 'global' }
        #     };
        # </script>
        # <script src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-svg.js" async></script>
        # """

    def style_table(
        self,
        html: str,
        grid: bool =True,
        header: bool = True,
        padding: bool = True,
        font_path: str = None,
        style_words: bool = True,
        shadow_prob: float = 0.5,
        zebra_prob: float = 0.5,
        bold_prop: float = 0.3,
        color_underline_prob: float = 0.3,
    ) -> str:
        def _get_random_pastel_color():
            r = random.randint(150, 220)
            g = random.randint(150, 220)
            b = random.randint(150, 220)
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
                    if random.random() < bold_prop:
                        styled = f"<b>{styled}</b>"

                    # 확률적으로 색상/밑줄 스타일 적용:
                    if random.random() < color_underline_prob:
                        style_choice = random.choice(
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
            "<style>",
            """table {
                border-collapse: collapse;
            }""",
        ]
        if font_path is not None:
            font_uri = f"file://{Path(font_path).resolve().as_posix()}"
            # font_path에 한글이 포함되어 있는지 확인:
            if any("\uac00" <= ch <= "\ud7a3" for ch in font_path):  # 한글 유니코드 범위
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
        if random.random() < shadow_prob:
            styles.append(
                "table { box-shadow: 6px 6px 6px rgba(0,0,0,0.5); }"
            )

        if random.random() < zebra_prob:
            styles.append(
                """tr:nth-child(even) {
                    background-color: rgb(240, 240, 240);
                }"""
            )
        styles.append("</style>")

        # 스타일 적용:
        if style_words:
            html = re.sub(
                self.pattern_text_node,
                lambda x: f">{_randomly_style(x.group(1))}<",
                html,
            )
        # return "\n".join(styles) + html + self.mathjax_script
        return "\n".join(styles) + html

    def render(
        self,
        html: str,
        engine: str = "imgkit",
    ):
        def _crop(
            image_bytes: bytes,
            margin: int = 10,
        ) -> bytes:
            image = Image.open(BytesIO(image_bytes)).convert("RGB")

            img = np.array(image)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            # Apply binary threshold:
            _, thresh = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)
            ys, xs = np.where(thresh == 255)
            left = xs.min() - margin
            top = xs.max() + margin
            right = ys.min() - margin
            bottom = ys.max() + margin

            image_crop = image.crop(
                (left, right, top, bottom),
            )
            buffer = BytesIO()
            image_crop.save(
                buffer,
                format="JPEG",
            )
            return buffer.getvalue()

        html = self.style_table(
            html,
            font_path=random.choice(self.font_paths),
        )

        if engine == "imgkit":
            image_bytes = imgkit.from_string(
                html,
                output_path=False,
                options={
                    "enable-local-file-access": "",
                    "quiet": "",
                },
            )
        return _crop(
            image_bytes,
        )


def main(
    batch_size: int = 32,
    mod: str = "table",
) -> None:
    manager = DatalakeClient()

    search_results = manager.search_catalog(
        variants=[
            "table_html",
        ],
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    dataset = manager.to_dataset(
        search_results,
        absolute_paths=False,
    )
    # dataset = dataset.filter(
    #     lambda x: x["dataset"] == "tech_sci_mrc",
    # )  # TEMP!
    # 수식이 포함된 것 제거:
    dataset = dataset.filter(
        lambda example: "\\" not in example["label"]
    )
    # dataset = dataset.shuffle()  # TEMP!
    # dataset = dataset.select(range(16))  # TEMP!

    renderer = HTMLRenderer()
    dataset = dataset.map(
        lambda batch: {
            "image_bytes": [
                renderer.render(
                    i,
                ) for i in batch["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
    )

    converter = HTMLToDogTags()
    dataset = dataset.map(
        lambda batch: {
            "label": [
                converter.convert(
                    i,
                ) for i in batch["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
    )

    for dataset_name in dataset.unique("dataset"):
        dataset_filter = dataset.filter(
            lambda x: x["dataset"] == dataset_name,
        )
        _, _ = manager.upload_task_data(
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

    job_id = manager.trigger_nas_processing()
    manager.wait_for_job_completion(
        job_id,
    )
    manager.build_catalog_db(
        force_rebuild=True,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main,
    )
