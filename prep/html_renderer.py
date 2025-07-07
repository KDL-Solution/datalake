import imgkit
import random
import cv2
import numpy as np
from io import BytesIO
from PIL import Image


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
