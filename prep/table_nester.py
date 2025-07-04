import random
import imgkit
import random
import regex
import re
import cv2
import base64
import imghdr
import numpy as np
from pathlib import Path
from io import BytesIO
from PIL import Image
import latex2mathml.converter
import random
from bs4 import BeautifulSoup
from typing import List, Union, Tuple
from PIL import Image, ImageDraw, ImageFont

import sys
sys.path.insert(0, '/home/eric/workspace/datalake/')
import importlib
from prep.utils import HTMLToOTSL, bytes_to_pil, pil_to_bytes
from core.datalake import DatalakeClient
import prep.table_html_to_table_image_otsl
importlib.reload(prep.table_html_to_table_image_otsl)


class TableNester(object):
    def __init__(
        self,
        seed: int = 42,
        mask: bool = True,
        min_font_size: int = 30,
        max_font_size: int = 60,
    ):
        self.rng = random.Random(seed)
        self.mask = mask
        self.min_font_size = min_font_size
        self.max_font_size = max_font_size

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
            print(font_size)
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
        soup = BeautifulSoup(outer_html, "html.parser")
        table = soup.find("table")
        if table is None:
            raise ValueError("No <table> found in outer_html")

        # candidate <td> 계산 (첫 행/열 제외 로직 생략)
        rows = table.find_all("tr")
        first_row = set(rows[0].find_all("td"))
        first_col = {r.find_all("td")[0] for r in rows if r.find_all("td")}
        candidate_tds = [
            td for td in table.find_all("td")
            if td not in first_row and td not in first_col
        ]
        if not candidate_tds:
            raise ValueError("No eligible <td> found")

        for idx, item in enumerate(inner_items):
            target_td = self.rng.choice(candidate_tds)

            if isinstance(item, bytes):
                if self.mask:
                    image = bytes_to_pil(
                        item,
                    )
                    image_new = self._mask(
                        image,
                        text=f"<|IMAGE-{str(idx).zfill(2)}|>",
                    )
                    item = pil_to_bytes(
                        image_new,
                    )

                # 1) 이미지 타입 감지
                img_type = imghdr.what(None, h=item) or "png"
                # 2) base64 인코딩
                b64 = base64.b64encode(item).decode("ascii")
                data_uri = f"data:image/{img_type};base64,{b64}"
                # 3) <img> 태그 생성·삽입
                img_tag = soup.new_tag("img", src=data_uri)
                target_td.append(img_tag)

            else:
                # 기존 HTML 문자열 처리
                inner_soup = BeautifulSoup(item, "html.parser")
                inner_table = inner_soup.find("table")
                if inner_table is None:
                    raise ValueError("No <table> in inner HTML")
                # clone & append
                clone = BeautifulSoup(str(inner_table), "html.parser").find("table")
                target_td.append(clone)
        return str(table)


if __name__ == "__main__":
    client = DatalakeClient(
        user_id="eric,"
    )

    search_results = client.search(
        variants=[
            "table_html",
            "table_image_html",
        ],
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    # idx = 10
    # example = search_results.iloc[idx].to_dict()
    # example.keys()
    # html = example["label"]

    import prep.table_html_to_table_image_otsl
    importlib.reload(prep.table_html_to_table_image_otsl)

    html_styler = prep.table_html_to_table_image_otsl.HTMLStyler()
    renderer = prep.table_html_to_table_image_otsl.HTMLRenderer()
    nester = TableNester(
        mask=True,
        min_font_size=30,
        max_font_size=60,
    )

    html_new = nester.nest(
        outer_html=search_results.iloc[10].to_dict()["label"],
        inner_items=[
            search_results.iloc[3].to_dict()["label"],
            # search_results.iloc[4].to_dict()["label"],
            # Path("/mnt/AI_NAS/Data/temp/스크린샷 2025-07-04 142126.png").read_bytes(),
            images[0],
            images[1],
        ]
    )
    html_new_style = html_styler.style(
        html_new,
        shadow_prob=0.,
    )
    # html_new_style = html_new

    # image_bytes = render(html_new)
    image_bytes = renderer.render(
        html_new_style,
    )
    image = Image.open(BytesIO(image_bytes))
    print(image.size)
    image.save("/home/eric/workspace/sample.jpg")
