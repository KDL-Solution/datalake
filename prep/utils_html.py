import regex
import random
import latex2mathml.converter
import imgkit
import cv2
import base64
import imghdr
import numpy as np
from pathlib import Path
from io import BytesIO
from bs4 import BeautifulSoup, NavigableString
from docling.backend.html_backend import HTMLDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import InputDocument
from typing import List, Union, Tuple
from PIL import Image, ImageDraw, ImageFont
from copy import deepcopy

from prep.utils import (
    bytes_to_pil,
    pil_to_bytes,
)


class HTMLProcessor(object):
    def __init__(
        self,
        replace_with_bullet_symbol: bool = False,
    ) -> None:
        self.unify_bullet_symbol = replace_with_bullet_symbol

    def remove_ws_between_tags(
        self,
        html: str,
    ) -> str:
        """
        HTML 문자열에서 태그 사이에 있는, 공백 문자(\s)만으로 이루어진 텍스트 노드를 모두 제거한다.
        """
        soup = BeautifulSoup(html, "html.parser")

        # 모든 텍스트 노드를 순회
        for text_node in soup.find_all(
            string=True,
        ):
            if isinstance(text_node, NavigableString) and text_node.strip() == "":
                text_node.extract()  # 노드 자체를 트리에서 제거
        return soup.decode(
            formatter=None,
        )

    def process(
        self,
        html: str,
    ) -> str:
        if self.unify_bullet_symbol:
            html = html.replace("·", "•")

        html = self.remove_ws_between_tags(
            html,
        )
        return html


class HTMLToOTSL:
    def __init__(
        self,
    ):
        self.backend_class = HTMLDocumentBackend
        self.format = InputFormat.HTML
        self.otsl_pattern = regex.compile(
            r"<otsl>.*?</otsl>",
            regex.DOTALL,
        )

    def extract_otsl(
        self,
        text: str,
    ) -> str:
        """Find the content <otsl>...</otsl>
        """
        if not isinstance(text, str):
            return None
        match = regex.search(
            self.otsl_pattern,
            text,
        )
        if match:
            return match.group(0).strip()
        else:
            return None

    def convert(
        self,
        html: str,
    ) -> str:
        html_bytes = html.encode("utf-8")
        bytes_io = BytesIO(html_bytes)
        in_doc = InputDocument(
            path_or_stream=bytes_io,
            format=self.format,
            backend=self.backend_class,
            filename="temp.html",
        )
        backend = self.backend_class(
            in_doc=in_doc,
            path_or_stream=bytes_io,
        )
        dl_document = backend.convert()
        doctags = dl_document.export_to_doctags()
        return self.extract_otsl(
            doctags,
        )


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
        self.pattern_text_node = regex.compile(
            r">(.*?)<",
            flags=regex.DOTALL,
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
            "<style>"
            "table{table-layout:fixed;border-collapse:collapse;}"
            "table table{"
              "display:inline-table;"              # fixed layout 제약 일부 무시
              "table-layout:fixed!important;"       # 칼럼 폭을 콘텐츠 기준으로
              "width:auto!important;"              # 부모 cell 너비 제한 해제
              "max-width:none!important;"          # 부모 max-width 제한 해제
            "}"
            "table table td,table table th{"
              "white-space:nowrap!important;"      # 셀 안 줄바꿈 방지 → 높이 최소화
              "padding:0.25em!important;"          # (선택) inner 셀 패딩 축소
            "}"
            "td,th{white-space:normal;}"
            "td img{display:block;max-width:none;height:auto;}"
        ]
        # print(styles[0])

        font_path = self.rng.choice(self.font_paths)
        font_uri = f"file://{font_path}"
        # font_path에 한글이 포함되어 있는지 확인:
        if any("\uac00" <= ch <= "\ud7a3" for ch in Path(font_path).name):  # 한글 유니코드 범위
            font_size = "1.35rem"
        else:
            font_size = "1.1rem"

        styles.append(
            f"@font-face{{font-family:'CustomFont'; src:url('{font_uri}') format('truetype');}}"
            f"*{{font-family:'CustomFont', sans-serif; font-size:{font_size};}}"
        )

        if grid:
            styles.append(
                "td, th {border:0.5px solid #333;}"
            )
        if header:
            pastel_color = _get_random_pastel_color()
            styles.append(
                f"th {{background-color:{pastel_color}; color:black; font-weight:bold;}}"
            )
        if padding:
            styles.append(
                "td, th {padding:0.5em; text-align:center;}"
            )
        if self.rng.random() < shadow_prob:
            styles.append(
                "table {box-shadow:6px 6px 6px rgba(0,0,0,0.5);}"
            )

        styles.append("</style>")

        if style_words:
            html = self.pattern_text_node.sub(
                lambda x: f">{_randomly_style(x.group(1))}<",
                html
            )
        return "\n".join(styles) + html


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
                # 로컬 파일 접근 허용 (base64 외 로컬 파일을 사용할 때)
                "enable-local-file-access": "",
                "quiet": "",
                "zoom": zoom,
                # 자바스크립트 활성화
                "enable-javascript": None,
                # JS 실행 후 대기할 시간(ms). 이미지가 로드되고 onload 핸들러가 실행될 충분한 시간.
                "javascript-delay": "500",  
                # 느린 스크립트도 중단하지 않기
                "no-stop-slow-scripts": None,
            },
        )
        return self._crop(
            image_bytes,
        )


def remove_img_tags(
    html: str,
) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for img in soup.find_all("img"):
        img.decompose()
    html_new = soup.decode(
        formatter=None,
    )
    return html_new.replace("<br/>", "<br>")


class TableNester(object):
    def __init__(
        self,
        outer_htmls: List[str],
        inner_htmls: List[str],
        inner_images: List[bytes],
        seed: int = 42,
        mask: bool = True,
        image_size_factor: float = 0.5,
        min_num_inner_tables: int = 1,
        max_num_inner_tables: int = 3,
        min_num_inner_images: int = 0,
        max_num_inner_images: int = 6,
        min_font_size: int = 30,
        max_font_size: int = 60,
    ):
        self.outer_htmls = outer_htmls
        self.inner_htmls = inner_htmls
        self.inner_images = inner_images
        self.mask = mask
        self.image_size_factor = image_size_factor
        self.min_font_size = min_font_size
        self.max_font_size = max_font_size
        self.min_num_inner_tables = min_num_inner_tables
        self.max_num_inner_tables = max_num_inner_tables
        self.min_num_inner_images = min_num_inner_images
        self.max_num_inner_images = max_num_inner_images

        self.rng = random.Random(seed)

    def _mask(
        self,
        image: Image.Image,
        text: str,
        font_path: str = "DejaVuSans.ttf",
        text_color: Tuple[int, int, int] = (0, 0, 0),
    ) -> Image.Image:
        """
        Return a new white PIL image (same size as image) with `text` centered.
        """
        w, h = image.size
        image_new = Image.new("RGB", (w, h), "white")
        draw = ImageDraw.Draw(image_new)

        # Load font
        if font_path:
            font_size = max(self.min_font_size, int(min(w, h) * 0.05))
            font_size = min(self.max_font_size, font_size)
            # print(font_size)
            font = ImageFont.truetype(font_path, font_size)
        else:
            font = ImageFont.load_default()

        # Compute text bounding box
        # textbbox returns (x0, y0, x1, y1)
        bbox = draw.textbbox((0, 0), text, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]

        # Center coordinates
        x = (w - text_w) / 2 - bbox[0]
        y = (h - text_h) / 2 - bbox[1]

        # Draw centered text
        draw.text((x, y), text, fill=text_color, font=font)
        return image_new

    def nest(
        self,
        outer_html: str,
        inner_items: List[Union[str, bytes]],
    ) -> str:
        """
        inner_items 에는 HTML 문자열(str) 또는 이미지 바이트(bytes)를 모두 넣을 수 있습니다.
        bytes 가 들어오면 <img> 태그로 cell 에 삽입합니다.
        """
        outer_soup = BeautifulSoup(
            outer_html,
            "html.parser",
        )
        outer_table = outer_soup.find("table")
        if outer_table is None:
            raise ValueError("No <table> found in outer_html")

        # candidate <td> 계산 (첫 행/열 제외 로직 생략)
        rows = outer_table.find_all("tr")
        if not rows:
            raise ValueError("No eligible tr tag found")

        first_row = set(rows[0].find_all("td"))
        first_col = {r.find_all("td")[0] for r in rows if r.find_all("td")}
        candidate_tds = [
            td for td in outer_table.find_all("td")
            if td not in first_row and td not in first_col
        ]
        if not candidate_tds:
            raise ValueError("No eligible td tag found")

        mask_idx = 1
        for item in inner_items:
            trg_td = self.rng.choice(candidate_tds)

            if isinstance(item, bytes):
                image = bytes_to_pil(
                    item,
                )
                width, height = image.size

                if self.mask:
                    trg_td.append(
                        f"<br><|IMG-{mask_idx:02d}|>"
                    )
                    mask_idx += 1

                    image_new = self._mask(
                        image,
                        text="",
                    )
                    item = pil_to_bytes(
                        image_new,
                    )

                img_type = imghdr.what(None, h=item) or "png"
                b64 = base64.b64encode(item).decode("ascii")
                data_uri = f"data:image/{img_type};base64,{b64}"
                img_tag = outer_soup.new_tag(
                    "img",
                    src=data_uri,
                    width=f"{int(width * self.image_size_factor)}",
                    height=f"{int(height * self.image_size_factor)}",
                )
                trg_td.append(
                    img_tag
                )
            else:
                inner_soup = BeautifulSoup(
                    item,
                    "html.parser",
                )
                inner_table = inner_soup.find("table")
                trg_td.append(
                    "<br>"
                )
                trg_td.append(
                    deepcopy(
                        inner_table,
                    )
                )

        html_for_rendering = outer_table.decode(
            formatter=None,
        )
        html_for_gt = remove_img_tags(
            html_for_rendering,
        )
        html_for_gt = html_for_gt.replace(
            "<br><|IMG-", "\n<|IMG-"
        )
        html_for_gt = html_for_gt.replace(
            "<br><table>", "\n<table>"
        )
        return {
            "html_for_rendering": html_for_rendering,
            "html_for_gt": html_for_gt,
        }

    def synthesize(
        self,
    ):
        while True:
            outer_html = self.rng.choice(self.outer_htmls)

            inner_htmls_choices = []
            if self.inner_htmls:
                inner_htmls_choices = self.rng.choices(
                    self.inner_htmls,
                    k=self.rng.randint(
                        self.min_num_inner_tables,
                        self.max_num_inner_tables,
                    ),
                )
            inner_images_choices = []
            if self.inner_images:
                inner_images_choices = self.rng.choices(
                    self.inner_images,
                    k=self.rng.randint(
                        self.min_num_inner_images,
                        self.max_num_inner_images,
                    ),
                )
            inner_items = inner_htmls_choices + inner_images_choices
            self.rng.shuffle(
                inner_items,
            )
            try:
                return self.nest(
                    outer_html=outer_html,
                    inner_items=inner_items,
                )
            except ValueError:
                continue
