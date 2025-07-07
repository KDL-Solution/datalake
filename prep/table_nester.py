import random
import base64
import imghdr
from PIL import Image
from bs4 import BeautifulSoup
from typing import List, Union, Tuple
from PIL import Image, ImageDraw, ImageFont
from copy import deepcopy

from prep.utils import (
    HTMLToOTSL,
    bytes_to_pil,
    pil_to_bytes,
    HTMLProcessor,
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
        first_row = set(rows[0].find_all("td"))
        first_col = {r.find_all("td")[0] for r in rows if r.find_all("td")}
        candidate_tds = [
            td for td in outer_table.find_all("td")
            if td not in first_row and td not in first_col
        ]
        if not candidate_tds:
            raise ValueError("No eligible <td> found")

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
        return self.nest(
            outer_html=outer_html,
            inner_items=inner_items,
        )
