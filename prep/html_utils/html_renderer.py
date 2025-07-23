import regex
import random
import numpy as np
from typing import Tuple, List
from PIL import Image, ImageOps
from playwright.sync_api import sync_playwright

from prep.utils import bytes_to_pil


class HTMLRenderer(object):
    def __init__(
        self,
        min_pad: int = 0,
        max_pad: int = 0,
        min_margin: int = 0,
        max_margin: int = 0,
        max_pixels: int = 2_048 * 2_048,
        width: int = 6,
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

    def _crop(
        self,
        image: Image.Image,
    ) -> Image.Image:
        # RGB가 아니면 변환
        if image.mode != "RGB":
            image = image.convert("RGB")

        arr = np.array(image)              # shape: (H, W, 3)
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
            return image

        left = max(non_bg_cols[0], 0)
        top = max(non_bg_rows[0], 0)
        right = min(non_bg_cols[-1] + 1, image.width)
        bottom = min(non_bg_rows[-1] + 1, image.height)
        return image.crop((left, top, right, bottom))

    def _pad_and_add_margin(
        self,
        center_image: Image.Image,
        hor_margin_image: Image.Image,
        ver_margin_image: Image.Image,
    ) -> Image.Image:
        center_image = self._crop(
            center_image,
        )
        hor_margin_image = self._crop(
            hor_margin_image,
        )
        ver_margin_image = self._crop(
            ver_margin_image,
        )

        (
            left_pad,
            top_pad,
            right_pad,
            bottom_pad,
        ) = self.rng.choices(
            range(self.min_pad, self.max_pad + 1),
            k=4,
        )
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

        top_pad_image = self._tile_or_crop_from(
            ver_margin_image,
            (W, top_margin),
            origin="bottom-left",
        )
        bottom_pad_image = self._tile_or_crop_from(
            ver_margin_image,
            (W, bottom_margin),
            origin="top-left",
        )
        left_pad_image = self._tile_or_crop_from(
            hor_margin_image,
            (left_margin, H),
            origin="top-right",
        )
        right_pad_image = self._tile_or_crop_from(
            hor_margin_image,
            (right_margin, H),
            origin="top-left",
        )

        # 5. 수직 병합
        vert = Image.new("RGB", (W, top_margin + H + bottom_margin), self.bg_color)
        vert.paste(top_pad_image, (0, 0))
        vert.paste(center_image, (0, top_margin))
        vert.paste(bottom_pad_image, (0, top_margin + H))

        # 6. 수평 병합
        final = Image.new("RGB", (left_margin + W + right_margin, vert.height), self.bg_color)
        final.paste(left_pad_image, (0, 0))
        final.paste(vert, (left_margin, 0))
        final.paste(right_pad_image, (left_margin + W, 0))
        return final, left_pad + left_margin, top_pad + top_margin

    def _render(
        self,
        html: str,
        tags: List[str] = [
            "table",
            "table table",
            "img",
        ],
    ) -> Tuple[bytes, list, float, float, list]:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
            )
            page = browser.new_page()
            page.set_content(html)

            body_size = page.evaluate(
                "() => ({ width: document.body.scrollWidth, height: document.body.scrollHeight })",
            )
            body_w = body_size["width"]
            body_h = body_size["height"]

            page.set_viewport_size(
                {
                    "width": int(body_w),
                    "height": int(body_h),
                },
            )

            inner_items = page.query_selector_all(
                ", ".join(tags),
            )
            bboxes = []
            for inner in inner_items:
                bbox = inner.bounding_box()
                xmin = bbox["x"]
                ymin = bbox["y"]
                xmax = bbox["x"] + bbox["width"]
                ymax = bbox["y"] + bbox["height"]
                bboxes.append([xmin, ymin, xmax, ymax])

            screenshot = page.screenshot(full_page=True)
            browser.close()
            return screenshot, bboxes

    def _get_margin_images(
        self,
        html: str,
        div_style: str = """<div style="text-align:justify; text-justify:inter-word;">""",
    ) -> Tuple[Image.Image, Image.Image]:
        match = self.style_pat.search(html)
        style = match.group(0) if match else ""

        html = self.style_pat.sub("", html)
        text_nodes = self.tag_pat.split(html)
        text_nodes = [i.strip() for i in text_nodes if i.strip()]
        text = " ".join(text_nodes)
        try:
            hor_bytes, _ = self._render(
                style + div_style + text + "</div>"
            )
            ver_bytes, _ = self._render(
                style + div_style + text + "</div>"
            )
            hor_image = bytes_to_pil(hor_bytes)
            ver_image = bytes_to_pil(ver_bytes)
            return hor_image, ver_image
        except OSError:
            raise OSError("Too large image.")

    def render(
        self,
        html: str,
        tags: List[str] = [
            "img",
        ],
        zoom: float = 0.3,
    ) -> Tuple:
        try:
            image_bytes, bboxes = self._render(
                html,
                tags=tags,
            )

            if image_bytes is None:
                raise RuntimeError(f"{self.engine} from_string returned None")

            center_image = bytes_to_pil(image_bytes)

            hor_image, ver_image = self._get_margin_images(
                html,
            )
            image, left, top = self._pad_and_add_margin(
                center_image=center_image,
                hor_margin_image=hor_image,
                ver_margin_image=ver_image,
            )
            for bbox in bboxes:
                bbox[0] += left
                bbox[1] += top
                bbox[2] += left
                bbox[3] += top

            # norm_bboxes = [
            #     [
            #         i[0] / image.width,
            #         i[1] / image.height,
            #         i[2] / image.width,
            #         i[3] / image.height,
            #     ]
            #     for i in bboxes
            # ]
            old_size = image.size
            new_size = (int(old_size[0] * zoom), int(old_size[1] * zoom))
            # print(old_size, new_size)
            image = image.resize(new_size, Image.LANCZOS)
            # print(image.size)
            # print(bboxes)
            bboxes = [[int(j * zoom) for j in i] for i in bboxes]
            # print(bboxes)
            # for bbox in bboxes:
            #     bbox[0] = int(bbox[0] * new_size[0] / old_size[0])
            #     bbox[1] = int(bbox[1] * new_size[1] / old_size[1])
            #     bbox[2] = int(bbox[2] * new_size[0] / old_size[0])
            #     bbox[3] = int(bbox[3] * new_size[1] / old_size[1])
            return {
                "image": image,
                # "normalized_bboxes": norm_bboxes,
                "bboxes": bboxes,
                # "hor_image": hor_image,
                # "ver_image": ver_image,
            }

        except Exception:
            import traceback
            traceback.print_exc()
            return {
                "image": None,
                # "normalized_bboxes": None,
                "bboxes": None,
            }


if __name__ == "__main__":
    html_style = """<style>table{table-layout:fixed;border-collapse:collapse;white-space:break-spaces;overflow-wrap:break-word;width:auto;}td,th{white-space:break-spaces;}table table{display:inline-table;width:auto!important;max-width:none!important;}table table td,table table th{white-space:pre!important;}td img{display:block;max-width:none;height:auto;}\n/* /mnt/AI_NAS/OCR/Font/나눔손글씨 미니 손글씨.ttf */*{font-family:\'CustomFont\', sans-serif; font-size:1.35rem;}\ntd, th {border:0.5px solid #333;}\nth {background-color:rgb(200, 175, 159); color:black; font-weight:bold;}\ntd, th {padding:0.5em; text-align:center;}\n</style><table><tbody><tr><td>표제</td><td>저자</td><td>ISBN</td><td>부가기호</td><td><b>바코드</b></td></tr><tr><td><span style=\'color:red\'>서울시</span> <b>우수건축자산</b> 제11호, 샘터사옥 <span style=\'color:blue;text-decoration:underline\'>&</span> 공공일호</td><td rowspan="2"><b>서울특별시</b> 한옥건축자산과 ; <b>하나</b></td><td><span style=\'color:red\'>979-11-6599-305-4</span></td><td><b>93540</b></td><td rowspan="2">「붙임 <b>2」</b> <b>참고(ai,</b> eps, pdf <span style=\'color:blue;text-decoration:underline\'><b>file)</b></span></td></tr><tr><td><b>서울시</b> 우수건축자산 제2호, 대선제분 <b>영등포</b> 공장</td><td>979-11-6599-304-7</td><td>93540\n\n<table><tbody><tr><td>구분</td><td>점검일자</td><td><span style=\'color:blue;text-decoration:underline\'><b>점검자</b></span></td><td>점검대상</td><td>점검내용</td><td><b>비고</b></td></tr><tr><td><b>1차</b></td><td>‘21.03.05.</td><td>****************</td><td><b>공무</b> 지하1층(기계실, <span style=\'color:blue;text-decoration:underline\'><b>보일러실)(이동식</b></span> 사다리,고압가스용기)</td><td><b>추락,</b> 화제‧폭발 위험요인</td><td></td></tr><tr><td><span style=\'color:blue;text-decoration:underline\'>2차</span></td><td><b>‘21.03.19.</b></td><td><span style=\'color:red\'><b>****************</b></span></td><td><b>공무</b> <span style=\'color:red\'><b>지하1층(기계실,</b></span> <b>보일러실)(안전모,</b> 청관제 플라스틱용기)</td><td>감전, 충돌 위험요인</td><td></td></tr></tbody></table>\n\n<table><tbody><tr><td colspan="5"><span style=\'color:blue;text-decoration:underline\'>제</span> 1 <b>정</b> <span style=\'color:blue;text-decoration:underline\'><b>수</b></span> <b>장</b></td></tr><tr><td>기종별</td><td><b>일일</b> <b>점검사항</b></td><td><b>공정별</b></td><td><b>점검결과</b></td><td>작업사항 <span style=\'color:blue;text-decoration:underline\'><b>및</b></span> 수리조치 <span style=\'color:blue;text-decoration:underline\'>내역</span></td></tr><tr><td rowspan="5">탁도계</td><td rowspan="5">: 광원램프및 Lense오염상태점검: <b>세정장치</b> 동작상태 점검: <span style=\'color:blue;text-decoration:underline\'>시료의</span> 적정유입량 점검: 측정조내(Cell) <span style=\'color:blue;text-decoration:underline\'><b>침전물,오물,</b></span> 기포발생여부 <span style=\'color:red\'>점검</span></td><td>착수정</td><td><b>양</b> 호</td><td rowspan="10">❍1정수장 착수정 수질측정기기 점검(04/09) - 쳄버, 센서부, 수조: <span style=\'color:red\'>수시</span> 세정❍1정수장 <span style=\'color:red\'>Auto</span> Jar Tester <b>점검(04/09)</b> - <b>응집제</b> <span style=\'color:blue;text-decoration:underline\'>변경:</span> <span style=\'color:blue;text-decoration:underline\'>PAC→PAHCS</span> </td></tr><tr><td>침전지</td><td>양 호</td></tr><tr><td><b>여과지별(12대)</b></td><td>양 호</td></tr><tr><td>여과 배출수</td><td>양 호</td></tr><tr><td><b>여과</b> 통합수</td><td><b>양</b> 호</td></tr><tr><td rowspan="3">pH계</td><td rowspan="3"><span style=\'color:blue;text-decoration:underline\'>:</span> 시료의 <span style=\'color:red\'>적정유입량</span> 점검: 전극내 및 <span style=\'color:red\'>홀더</span> 오염상태</td><td>착수정</td><td>양 호</td></tr><tr><td><b>응집지</b></td><td>양 호</td></tr><tr><td>침전지</td><td>양 호</td></tr><tr><td rowspan="2">알카리도계</td><td rowspan="2"><span style=\'color:red\'><b>:</b></span> 측정치와실험치 <span style=\'color:blue;text-decoration:underline\'>비교분석:</span> 전극 <span style=\'color:blue;text-decoration:underline\'>및</span> <b>측정부</b> <b>상태점검</b></td><td>착수정</td><td><span style=\'color:red\'><b>양</b></span> 호</td></tr><tr><td><span style=\'color:red\'>침전지</span></td><td>양 호</td></tr><tr><td>AUTO쟈-테스터</td><td><span style=\'color:red\'>:</span> 시료,시약,펌프,유닛 상태점검</td><td>착수정</td><td>양 <b>호</b></td><td rowspan="10">❍공정별 <span style=\'color:red\'>수질측정기</span> <b>일일점검.</b> ❍1정수장 <b>잔류염소계</b> 점검(04/09) <b>-</b> 스트레이너 및 드레인 배관 수시 세정 ❍1정수장 <span style=\'color:red\'>샘플링</span> 펌프 <b>점검(01/18∼)</b> 여과수: 샘플링 <b>펌프(3지)소음</b> 발생 <b>점검</b> * <b>양수량</b> <b>부족할</b> 때 교체</td></tr><tr><td><b>전기전도도</b></td><td><span style=\'color:red\'><b>:</b></span> 시료유입량,전극,오염상태</td><td>착수정</td><td><span style=\'color:blue;text-decoration:underline\'>양</span> 호</td></tr><tr><td>수 온</td><td>: 시료유입량,전극,전송,기타</td><td><span style=\'color:blue;text-decoration:underline\'><b>착수정</b></span></td><td>양 <b>호</b></td></tr><tr><td><b>입자계수기</b></td><td>: 시료유입량,센서,전송,기타</td><td><span style=\'color:red\'>여과지,침전지</span></td><td>양 호</td></tr><tr><td rowspan="5"><b>잔류염소계</b></td><td rowspan="5"><b>:</b> <b>시료의</b> 적정유입량 점검: <b>시약조</b> 시약량 <span style=\'color:blue;text-decoration:underline\'>점검:</span> 측정치와 <b>실험치</b> <span style=\'color:blue;text-decoration:underline\'><b>비교분석</b></span></td><td><b>착수정</b></td><td><b>양</b> 호</td></tr><tr><td> <b>착수정(Total)</b></td><td><b>양</b> <span style=\'color:red\'><b>호</b></span></td></tr><tr><td><span style=\'color:blue;text-decoration:underline\'>침전지</span></td><td><b>양</b> 호</td></tr><tr><td><b>여과지</b></td><td><b>양</b> <span style=\'color:red\'><b>호</b></span></td></tr><tr><td>정수지(A,B)</td><td><b>양</b> <span style=\'color:red\'>호</span></td></tr><tr><td>시료수펌프</td><td><b>:</b> 과전류,소음,과열,급수상태</td><td>각 <span style=\'color:blue;text-decoration:underline\'><b>공정별</b></span></td><td><b>양</b> 호</td></tr></tbody></table>\n\n</td></tr></tbody></table>"""

    renderer = HTMLRenderer(
        min_pad = 20,
        max_pad = 50,
        # min_margin = 100,
        # max_margin = 140,
        min_margin = 0,
        max_margin = 0,
    )

    render_out = renderer.render(
        html=html_style,
    )
    image = draw_texts_and_bboxes_on_red_image(
        render_out["image"],
        norm_bboxes=render_out["normalized_bboxes"][1:],
    )
    # render_out["image"].save("/home/eric/workspace/sample.jpg")
    image.save("/home/eric/workspace/sample.jpg")
