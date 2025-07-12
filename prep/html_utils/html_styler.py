import regex
import random
import latex2mathml.converter
from pathlib import Path


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
