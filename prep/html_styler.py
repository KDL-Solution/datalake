import random
import regex
import re
from pathlib import Path
import latex2mathml.converter


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
            html = re.sub(
                self.pattern_text_node,
                lambda x: f">{_randomly_style(x.group(1))}<",
                html,
            )
        return "\n".join(styles) + html
