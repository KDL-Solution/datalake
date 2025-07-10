import regex
import random
import imgkit
import textwrap
import base64
import imghdr
import latex2mathml.converter
import numpy as np
from pathlib import Path
from io import BytesIO, StringIO
from bs4 import BeautifulSoup
from docling_core.types.doc.document import DocTagsDocument, DoclingDocument
from docling.backend.html_backend import HTMLDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import InputDocument
from typing import List, Union, Tuple, Callable, Pattern
from PIL import Image, ImageDraw, ImageFont, ImageOps
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
        self.table_pat = regex.compile(
            r"<table>.*?</table>",
            regex.DOTALL,
        )
        self.ws_pat = regex.compile(r"(>)(\s+)(<)")

    def extract_table(
        self,
        text: str,
    ) -> str:
        """Find the content <table>...</table>
        """
        if not isinstance(text, str):
            return

        text = text.strip()
        match = regex.search(
            self.table_pat,
            text,
        )
        if match:
            return match.group(0).strip()

        if not text.startswith("<table>"):
            text = "<table>" + text
        if not text.endswith("</table>"):
            text = text + "</table>"
        return text

    def remove_whitespaces(
        self,
        html: str,
    ) -> str:
        """
        HTML 문자열에서 태그 사이에 있는, 공백 문자(스페이스, 탭, 개행)만으로 이루어진
        텍스트 노드를 정규표현식으로 제거합니다.

        예: "<TAG>   \n   </TAG>" -> "<TAG></TAG>"
        """
        # '>' 다음에 공백(스페이스, 탭, 개행)이 1개 이상 있고 '<'로 이어지는 부분을 잡음
        # 반복해서 모든 공백 노드를 제거
        while True:
            new_html = self.ws_pat.sub(r"\1\3", html)
            if new_html == html:
                break
            html = new_html
        return html

    def process(
        self,
        html: str,
    ) -> str:
        if self.unify_bullet_symbol:
            html = html.replace("·", "•")

        html = self.remove_whitespaces(
            html,
        )
        return html


class HTMLDocTagsConverter:
    def __init__(
        self,
        newline_tag = "[|NL|]",
        repl_tag_pref = "[|REPL-",
        repl_tag_suff = "|]",
    ):
        self.newline_tag = newline_tag
        self.repl_tag_pref = repl_tag_pref
        self.repl_tag_suff = repl_tag_suff

        self.backend_class = HTMLDocumentBackend
        self.format = InputFormat.HTML
        self.otsl_pat = regex.compile(
            r"<otsl>.*?</otsl>",
            regex.DOTALL,
        )
        self.table_pat = regex.compile(
            r"<table>.*?</table>",
            regex.DOTALL,
        )
        self.pat_inner_table = regex.compile(
            r"<table>(?:(?!<table>).)*?</table>",
            regex.DOTALL,
        )
        self.pat_inner_otsl = regex.compile(
            r"<otsl>(?:(?!<otsl>).)*?</otsl>",
            regex.DOTALL,
        )
        self.pat_cap = regex.compile(
            r"<caption>.*?</caption>",
            regex.DOTALL,
        )

    def extract_otsl(
        self,
        text: str,
    ) -> str:
        """Find the content <otsl>...</otsl>
        """
        if not isinstance(text, str):
            return

        text = text.strip()
        match = regex.search(
            self.otsl_pat,
            text,
        )
        if match:
            return match.group(0).strip()

        if not text.startswith("<otsl>"):
            text = "<otsl>" + text
        if not text.endswith("</otsl>"):
            text = text + "</otsl>"
        return text

    def __to_doctags(
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

    def _to_doctags(
        self,
        html: str,
    ) -> str:
        """
        1) HTML 문자열에서 <caption>...</caption> 부분을 추출하고,
        2) 나머지 HTML에 repl 함수를 적용한 뒤,
        3) 가장 첫 번째 <table> 바로 다음에 캡션을 삽입하여 반환합니다.

        :param html: 입력 HTML 문자열
        :param repl: 캡션을 제외한 본문에 적용할 함수
        :return: 캡션이 다시 삽입된 최종 HTML 문자열
        """
        # 1) 캡션 추출
        cap_match = self.pat_cap.search(html)
        if cap_match:
            caption = cap_match.group(0)
            body = html[:cap_match.start()] + html[cap_match.end():]
        else:
            caption = ''
            body = html

        # 2) 본문에 repl 적용
        processed_body = self.__to_doctags(body)

        # 3) 첫 번째 <table> 뒤에 캡션 삽입
        if caption:
            processed_html = regex.sub(
                r'(<otsl[^>]*>)',
                lambda m: f"{m.group(1)}{caption}",
                processed_body,
                count=1
            )
        else:
            processed_html = processed_body
        return processed_html

    def _to_html(
        self,
        doctags: str,
    ) -> str:
        doctags = doctags.replace("\n", self.newline_tag)
        stream = StringIO(doctags)
        table_tag = DocTagsDocument.from_doctags_and_image_pairs(stream, images=None)
        doc = DoclingDocument.load_from_doctags(table_tag)
        html = "".join(
            [i.export_to_html(doc=doc) for i in doc.tables],
        )
        html = html.replace(self.newline_tag, "\n")
        return html

    def convert(
        self,
        text: str,
        pat: Pattern[str],
        repl: Callable[[str], str],
    ) -> str:
        """
        중첩된 <table>…</table> 구조를 가장 안쪽부터 순차적으로 처리하되,
        즉시 치환하지 않고 [|REPL-n|] 형태의 플레이스홀더로 대체합니다.
        마지막에 이 플레이스홀더를 repl() 호출 결과로 한 번에 복원하여 반환합니다.

        :param html: 입력 HTML 문자열
        :param repl: 각 <table>…</table> 블록에 적용할 함수
        :return: 플레이스홀더가 repl 결과로 대체된 최종 HTML
        """
        def _inner_sub(m):
            counter[0] += 1
            ph = f"{self.repl_tag_pref}{counter[0]:01d}{self.repl_tag_suff}"
            placeholder_map[ph] = repl(m.group(0))
            return ph

        def _outer_sub(m):
            counter[0] += 1
            ph = f"{self.repl_tag_pref}{counter[0]:01d}{self.repl_tag_suff}"
            placeholder_map[ph] = repl(m.group(0))
            return ph

        placeholder_map = {}
        counter = [0]
        # 1) 가장 안쪽 테이블부터 플레이스홀더로 치환
        while True:
            text_temp = pat.sub(
                _inner_sub,
                text,
            )
            if text_temp == text:
                break

            text = text_temp

        # 2) 남은 모든 테이블(바깥쪽)에 플레이스홀더 적용
        text = pat.sub(
            _outer_sub,
            text,
        )

        # 3) 플레이스홀더를 한 번에 repl 결과로 복원
        for ph, processed in placeholder_map.items():
            text = text.replace(ph, processed)
        for ph, processed in placeholder_map.items():
            text = text.replace(ph, processed)
        return text

    def to_doctags(
        self,
        html: str,
    ) -> str:
        """
        HTML 문자열을 DocTags 형식으로 변환합니다.
        """
        return self.convert(
            html,
            pat=self.pat_inner_table,
            repl=self._to_doctags,
        )

    def to_html(
        self,
        doctags: str,
    ) -> str:
        """
        HTML 문자열을 DocTags 형식으로 변환합니다.
        """
        return self.convert(
            doctags,
            pat=self.pat_inner_otsl,
            repl=self._to_html,
        )


class HTMLStyler(object):
    def __init__(
        self,
        font_dir: str = "/mnt/AI_NAS/OCR/Font/",
        header: bool = True,
        padding: bool = True,
        words_style: bool = True,
        shadow_prob: float = 0.,
        bold_prob: float = 0.3,
        color_underline_prob: float = 0.2,
        pre_white_space_prob: float = 0.8,
        seed: int = 42,
        font_size: str = "1.35rem",
    ) -> None:
        self.font_size = font_size
        self.header = header
        self.padding = padding
        self.words_style = words_style
        self.shadow_prob = shadow_prob
        self.bold_prob = bold_prob
        self.color_underline_prob = color_underline_prob
        self.pre_white_space_prob = pre_white_space_prob

        self.font_dir = Path(font_dir).resolve()
        self.font_paths = [i.as_posix() for i in self.font_dir.glob("*")]
        self.pat_word_split = regex.compile(
            r"\S+|\s+",
        )
        self.pat_text_node = regex.compile(
            r">(.*?)<",
            flags=regex.DOTALL,
        )

        self.rng = random.Random(seed)

    def style(
        self,
        html: str,
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
                    self.pat_word_split,
                    text_node,
                )

                styled_words = []
                for word in words:
                    if word.isspace():
                        styled_words.append(word)
                        continue

                    styled = word
                    # 확률적으로 bold:
                    if self.rng.random() < self.bold_prob:
                        styled = f"<b>{styled}</b>"

                    # 확률적으로 색상/밑줄 스타일 적용:
                    if self.rng.random() < self.color_underline_prob:
                        style_choice = self.rng.choice(
                            [
                                "blue_underline",
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

        style = (
            "<style>"
            "table{"
                "table-layout:fixed;"
                "border-collapse:collapse;"
                "white-space:break-spaces;"
                "overflow-wrap:break-word;"
                "width:auto;"
            "}"
            "td,th{"
                "white-space:break-spaces;"
            "}"
            "table table{"
                "display:inline-table;"              # fixed layout 제약 일부 무시
                # "display:table;"
                # "table-layout:fixed!important;"       # 칼럼 폭을 콘텐츠 기준으로
                "width:auto!important;"              # 부모 cell 너비 제한 해제
                "max-width:none!important;"          # 부모 max-width 제한 해제
            "}"
            "table table td,table table th{"
        )
        if self.rng.random() < self.pre_white_space_prob:
            style += "white-space:pre!important;"      # 셀 안 줄바꿈 방지 → 높이 최소화
        else:
            style += "white-space:break-spaces!important;"
        style += (
            "}"
            "td img{"
                "display:block;"
                "max-width:none;"
                "height:auto;"
            "}"
        )
        styles = [
            style,
        ]

        font_path = self.rng.choice(self.font_paths)
        font_uri = f"file://{font_path}"

        styles.append(
            f"@font-face{{font-family:'CustomFont'; src:url('{font_uri}') format('truetype');}}"
            f"*{{font-family:'CustomFont', sans-serif; font-size:{self.font_size};}}"
        )

        # Grid:
        styles.append(
            "td, th {border:0.5px solid #333;}"
        )
        if self.header:
            pastel_color = _get_random_pastel_color()
            styles.append(
                f"th {{background-color:{pastel_color}; color:black; font-weight:bold;}}"
            )
        if self.padding:
            styles.append(
                "td, th {padding:0.5em; text-align:center;}"
            )
        if self.rng.random() < self.shadow_prob:
            styles.append(
                "table {box-shadow:6px 6px 6px rgba(0,0,0,0.5);}"
            )

        styles.append("</style>")

        if self.words_style:
            html = self.pat_text_node.sub(
                lambda x: f">{_randomly_style(x.group(1))}<",
                html
            )
        return "\n".join(styles) + html


class HTMLRenderer(object):
    def __init__(
        self,
        min_pad: int = 20,
        max_pad: int = 50,
        min_margin: int = 50,
        max_margin: int = 80,
        width: int = 6,
        zoom: float = 0.5,
        seed: int = 42,
        bg_color: Tuple[int, int, int] = (255, 255, 255),
    ) -> None:
        self.min_pad = min_pad
        self.max_pad = max_pad
        self.min_margin = min_margin
        self.max_margin = max_margin
        self.bg_color = bg_color
        self.width = width
        self.zoom = zoom

        self.rng = random.Random(seed)

        self.style_pat = regex.compile(
            r"<style\b[^>]*>.*?</style>",
            flags=regex.IGNORECASE | regex.DOTALL,
        )
        self.tag_pat = regex.compile(
            r"<[^>]+>",
            flags=regex.DOTALL,
        )

    def _tile_or_crop_from(
        self,
        image: Image.Image,
        target_size: Tuple[int, int],
        origin: str,
    ) -> Image.Image:
        w, h = target_size
        pw, ph = image.size

        num_x = -(-w // pw)
        num_y = -(-h // ph)

        tiled = Image.new("RGB", (num_x * pw, num_y * ph), color=self.bg_color)
        for y in range(num_y):
            for x in range(num_x):
                tiled.paste(image, (x * pw, y * ph))

        if origin == "top-left":
            return tiled.crop((0, 0, w, h))
        elif origin == "top-right":
            return tiled.crop((tiled.width - w, 0, tiled.width, h))
        elif origin == "bottom-left":
            return tiled.crop((0, tiled.height - h, w, tiled.height))
        elif origin == "bottom-right":
            return tiled.crop((tiled.width - w, tiled.height - h, tiled.width, tiled.height))
        else:
            print("B")
            raise ValueError(f"Invalid origin: {origin}")

    def _crop_pad_add_margin(
        self,
        center_image: Image.Image,
        margin_image1: Image.Image,
        margin_image2: Image.Image,
    ) -> Image.Image:
        # RGB가 아니면 변환
        if center_image.mode != "RGB":
            center_image = center_image.convert("RGB")

        arr = np.array(center_image)              # shape: (H, W, 3)
        bg  = np.array(self.bg_color)           # shape: (3,)

        # 배경색과 다른 픽셀 마스크: shape (H, W)
        mask = np.any(arr != bg, axis=2)

        # 행/열 단위로 비-배경이 하나라도 있는지 확인
        row_has_content = np.any(mask, axis=1)  # shape: (H,)
        col_has_content = np.any(mask, axis=0)  # shape: (W,)

        # 콘텐츠가 있는 첫/마지막 행·열 인덱스 찾기
        non_bg_rows = np.nonzero(row_has_content)[0]
        non_bg_cols = np.nonzero(col_has_content)[0]

        # 전부 배경이면 원본 반환
        if non_bg_rows.size == 0 or non_bg_cols.size == 0:
            return center_image

        left_pad = self.rng.randint(self.min_pad, self.max_pad)
        left = max(non_bg_cols[0] - left_pad, 0)
        top_pad = self.rng.randint(self.min_pad, self.max_pad)
        top = max(non_bg_rows[0] - top_pad, 0)
        right_pad = self.rng.randint(self.min_pad, self.max_pad)
        right = min(non_bg_cols[-1] + right_pad + 1, center_image.width)
        bottom_pad = self.rng.randint(self.min_pad, self.max_pad)
        bottom = min(non_bg_rows[-1] + bottom_pad + 1, center_image.height)
        center_image = center_image.crop((left, top, right, bottom))

        center_image = ImageOps.expand(
            center_image,
            border=(left_pad, top_pad, right_pad, bottom_pad),
            fill=self.bg_color,
        )

        W, H = center_image.size
        (
            left_margin,
            top_margin,
            right_margin,
            bottom_margin,
        ) = self.rng.choices(
            range(self.min_margin, self.max_margin + 1),
            k=4,
        )
        left_margin = max(0, left_margin - left_pad)
        top_margin = max(0, top_margin - top_pad)
        right_margin = max(0, right_margin - right_pad)
        bottom_margin = max(0, bottom_margin - bottom_pad)        

        # 1. Top padding (좌하단 기준 crop)
        top_pad = self._tile_or_crop_from(margin_image2, (W, top_margin), origin="bottom-left")
        # 2. Bottom padding (좌상단 기준 crop)
        bottom_pad = self._tile_or_crop_from(margin_image2, (W, bottom_margin), origin="top-left")
        # 3. Left padding (우상단 기준 crop)
        left_pad = self._tile_or_crop_from(margin_image1, (left_margin, H), origin="top-right")
        # 4. Right padding (좌상단 기준 crop)
        right_pad = self._tile_or_crop_from(margin_image1, (right_margin, H), origin="top-left")

        # 5. 수직 병합
        vert = Image.new("RGB", (W, top_margin + H + bottom_margin), self.bg_color)
        vert.paste(top_pad, (0, 0))
        vert.paste(center_image, (0, top_margin))
        vert.paste(bottom_pad, (0, top_margin + H))

        # 6. 수평 병합
        final = Image.new("RGB", (left_margin + W + right_margin, vert.height), self.bg_color)
        final.paste(left_pad, (0, 0))
        final.paste(vert, (left_margin, 0))
        final.paste(right_pad, (left_margin + W, 0))
        return final

    def _get_margin_images(
        self,
        html: str,
    ) -> str:
        style = self.style_pat.search(html).group(0)

        html = self.style_pat.sub("", html)
        text_nodes = self.tag_pat.split(html)
        text_nodes = [i.strip() for i in text_nodes if i.strip()]
        text = " ".join(text_nodes)

        hor_margin_text = textwrap.fill(text, width=self.width)
        hor_bytes = imgkit.from_string(
            style + hor_margin_text,
            output_path=False,
            options={
                "enable-local-file-access": "",
                "quiet": "",
            },
        )
        ver_bytes = imgkit.from_string(
            style + text,
            output_path=False,
            options={
                "enable-local-file-access": "",
                "quiet": "",
            },
        )
        hor_image = bytes_to_pil(hor_bytes)
        ver_image = bytes_to_pil(ver_bytes)
        return hor_image, ver_image

    def render(
        self,
        html: str,
    ) -> bytes:
        try:
            image_bytes = imgkit.from_string(
                html,
                output_path=False,
                options={
                    "enable-local-file-access": "",
                    "quiet": "",
                },
            )
            if image_bytes is None:
                raise RuntimeError("imgkit.from_string returned None")

            hor_image, ver_image = self._get_margin_images(
                html,
            )
            hor_image = hor_image.resize(
                (int(hor_image.width * self.zoom), int(hor_image.height * self.zoom)),
                resample=Image.LANCZOS,
            )
            ver_image = ver_image.resize(
                (int(ver_image.width * self.zoom), int(ver_image.height * self.zoom)),
                resample=Image.LANCZOS,
            )

            image = bytes_to_pil(image_bytes)
            image = image.resize(
                (int(image.width * self.zoom), int(image.height * self.zoom)),
                resample=Image.LANCZOS,
            )
            image = self._crop_pad_add_margin(
                center_image=image,
                margin_image1=hor_image,
                margin_image2=ver_image,
            )
            return pil_to_bytes(
                image,
            )

        except Exception as e:
            import traceback
            traceback.print_exc()
            return


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

        self.converter = HTMLDocTagsConverter()

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
                        f"<br>[|IMG-{mask_idx:02d}|]<br>"
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
                inner_table_copy = deepcopy(
                    inner_table,
                )
                trg_td.append(
                    f"<br><br>{inner_table_copy}<br><br>"
                )

        html = outer_table.decode(
            formatter=None,
        )
        html = html.replace(
            "<br>" * 4,
            "<br>" * 2,
        )

        label_html = remove_img_tags(
            html,
        )
        label_html = label_html.replace(
            "<br>[|IMG-",
            "\n[|IMG-",
        ).replace(
            "|]<br>",
            "|]\n",
        )
        label_html = label_html.replace(
            "<br><br><table>",
            "\n\n<table>",
        ).replace(
            "</table><br><br>",
            "</table>\n\n",
        )
        label_html = label_html.replace(
            "\n" * 4,
            "\n" * 2,
        )

        label_doctags = self.converter.to_doctags(
            label_html,
        )
        return {
            "html": html,
            "label_html": label_html,
            "label_doctags": label_doctags,
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
