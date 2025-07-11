import regex
import random
import imgkit
import textwrap
import numpy as np
from typing import Tuple
from PIL import Image, ImageOps

from prep.utils import (
    bytes_to_pil,
    pil_to_bytes,
)


class HTMLRenderer(object):
    def __init__(
        self,
        min_pad: int = 20,
        max_pad: int = 50,
        min_margin: int = 50,
        max_margin: int = 80,
        max_pixels: int = 2_048 * 2_048,
        width: int = 6,
        zoom: float = 0.5,
        seed: int = 42,
        bg_color: Tuple[int, int, int] = (255, 255, 255),
    ) -> None:
        Image.MAX_IMAGE_PIXELS = None

        self.min_pad = min_pad
        self.max_pad = max_pad
        self.min_margin = min_margin
        self.max_margin = max_margin
        self.max_pixels = max_pixels
        self.width = width
        self.zoom = zoom
        self.bg_color = bg_color

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
        try:
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
        except OSError:
            raise OSError("Too large image.")

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
            width, height = image.size
            if (width * height) > self.max_pixels:
                return

            return pil_to_bytes(
                image,
            )

        except Exception:
            # import traceback
            # traceback.print_exc()
            return
