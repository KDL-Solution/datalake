import regex


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
