import math
import random
import base64
import imghdr
from bs4 import BeautifulSoup
from typing import List, Union, Tuple
from PIL import Image, ImageDraw, ImageFont
from copy import deepcopy

from prep.utils import (
    bytes_to_pil,
    pil_to_bytes,
)
from prep.html_utils.html_doctags_converter import HTMLDocTagsConverter


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


def generate_random_white_images(
    count: int = 1,
    min_area: int = (2 ** 8) ** 2,
    max_area: int = (2 ** 6) ** 2,
    min_aspect: float = 0.5,
    max_aspect: float = 2.,
    seed: int = 42,
) -> List[Image.Image]:
    """
    주어진 종횡비(aspect ratio) 범위와 면적(area) 범위, 시드, 개수를 받아
    흰색(RGB: 255,255,255) 픽셀로만 이루어진 PIL 이미지를 리스트로 생성합니다.
    """
    rng = random.Random(seed)

    images = []
    for _ in range(count):
        aspect = rng.uniform(min_aspect, max_aspect)
        area = rng.uniform(min_area, max_area)
        height = math.sqrt(area / aspect)
        width = aspect * height
        width = max(1, int(width))
        height = max(1, int(height))
        images.append(
            Image.new(
                "RGB",
                (width, height),
                (255, 255, 255),
            )
        )
    return images


class HTMLNester(object):
    def __init__(
        self,
        outer_htmls: List[str],
        inner_htmls: List[str],
        inner_images: List[bytes],
        seed: int = 42,
        image_mask_color: Tuple[int, int, int] = (0, 0, 255),
        image_size_factor: float = 0.5,
        min_num_inner_tables: int = 1,
        max_num_inner_tables: int = 3,
        min_num_inner_images: int = 0,
        max_num_inner_images: int = 6,
        min_font_size: int = 30,
        max_font_size: int = 60,
        br_tag: str = "[|BR|]",
    ):
        self.outer_htmls = outer_htmls
        self.inner_htmls = inner_htmls
        self.inner_images = inner_images
        self.image_mask_color = image_mask_color
        self.image_size_factor = image_size_factor
        self.min_font_size = min_font_size
        self.max_font_size = max_font_size
        self.min_num_inner_tables = min_num_inner_tables
        self.max_num_inner_tables = max_num_inner_tables
        self.min_num_inner_images = min_num_inner_images
        self.max_num_inner_images = max_num_inner_images
        self.br_tag = br_tag

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
        image_new = Image.new("RGB", (w, h), self.image_mask_color)
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
        # mask_images: bool = True,
    ) -> str:
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

                # if mask_images:
                # #     trg_td.append(
                # #         f"{self.br_tag}[|IMG-{mask_idx:02d}|]{self.br_tag}"
                # #     )
                # #     mask_idx += 1

                #     image_new = self._mask(
                #         image,
                #         text="",
                #     )
                #     item = pil_to_bytes(
                #         image_new,
                #     )

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
                    f"{self.br_tag}{self.br_tag}{inner_table_copy}{self.br_tag}{self.br_tag}"
                )

        html = outer_table.decode(
            formatter=None,
        )
        html = html.replace(
            self.br_tag * 4,
            self.br_tag * 2,
        )
        html = html.replace(
            self.br_tag,
            "\n",
        )

        label_html = remove_img_tags(
            html,
        )

        label_doctags = self.converter.to_doctags(
            label_html,
        )
        return {
            "html_for_rendering": html,
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
                    # mask_images=True,
                )
            except ValueError:
                continue


if __name__ == "__main__":
    html = "<table><caption>참여기업 리스트 (종합)</caption><tbody><tr><td>분야</td><td>참여기업수</td><td>비고</td></tr><tr><td>호텔업</td><td>40</td><td><br/></td></tr><tr><td>여행업</td><td>12</td><td><br/></td></tr><tr><td>휴양콘도미니엄업</td><td>10</td><td><br/></td></tr><tr><td>국제회의업</td><td>9</td><td><br/></td></tr><tr><td>카지노업</td><td>2</td><td><br/></td></tr><tr><td>융·복합 관광</td><td>18</td><td><br/></td></tr><tr><td>유원시설</td><td>1</td><td><br/></td></tr><tr><td>해외취업관</td><td>9</td><td><br/></td></tr><tr><td>미래일자리관</td><td>6</td><td><br/></td></tr><tr><td>관광벤처관</td><td>13</td><td><br/></td></tr><tr><td>계</td><td>120</td><td><br/></td></tr></tbody></table>"

    images = generate_random_white_images(
        count=1,
    )
    images = [pil_to_bytes(i) for i in images]
    html_nester = HTMLNester(
        outer_htmls=[html],
        inner_htmls=[html],
        inner_images=images,
    )

    out = html_nester.synthesize()
    # print(out["label_html"])
    # print(out["html_for_rendering"])
