import regex
from io import BytesIO, StringIO
from docling_core.types.doc.document import DocTagsDocument, DoclingDocument
from docling.backend.html_backend import HTMLDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import InputDocument
from typing import Callable, Pattern


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
        # 1) 캡션 추출
        cap_match = self.pat_cap.search(html)
        if cap_match:
            caption = cap_match.group(0)
            body = html[: cap_match.start()] + html[cap_match.end():]
        else:
            caption = ""
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
        stream = StringIO(doctags)
        table_tag = DocTagsDocument.from_doctags_and_image_pairs(stream, images=None)
        doc = DoclingDocument.load_from_doctags(table_tag)
        html = "".join(
            [i.export_to_html(doc=doc) for i in doc.tables],
        )
        return html

    def _convert(
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
        def _sub(m):
            counter[0] += 1
            ph = f"{self.repl_tag_pref}{counter[0]:01d}{self.repl_tag_suff}"
            placeholder_map[ph] = repl(m.group(0))
            return ph

        placeholder_map = {}
        counter = [0]
        # 1) 가장 안쪽 테이블부터 플레이스홀더로 치환
        while True:
            text_temp = pat.sub(
                _sub,
                text,
            )
            if text_temp == text:
                break

            text = text_temp

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
        html = html.replace("\n", self.newline_tag)
        doctags = self._convert(
            html,
            pat=self.pat_inner_table,
            repl=self._to_doctags,
        )
        return doctags.replace(self.newline_tag, "\n")

    def to_html(
        self,
        doctags: str,
    ) -> str:
        """
        HTML 문자열을 DocTags 형식으로 변환합니다.
        """
        doctags = doctags.replace("\n", self.newline_tag)
        html = self._convert(
            doctags,
            pat=self.pat_inner_otsl,
            repl=self._to_html,
        )
        return html.replace(self.newline_tag, "\n")


if __name__ == "__main__":
    converter = HTMLDocTagsConverter()
    html = """<table><tr><td colspan="1" rowspan="2">품목</td><td colspan="4" rowspan="1">반출액(합계)</td><td colspan="4" rowspan="1">비중</td></tr><tr><td>1989~1999</td><td>2000~2004</td><td>2005~2016</td><td>2016~2020</td><td>1989~1999</td><td>2000~2004</td><td>2005~2016</td><td>2016~2020</td></tr><tr><td>음식료품(HS01~24)</td><td>7,177</td><td>35,385</td><td>75,998</td><td>390</td><td>11</td><td>20</td><td>7.8</td><td>2.2</td></tr><tr><td>섬유·의류(HS41~43,50~67)</td><td>20,559</td><td>35,091</td><td>324,510</td><td>5,298</td><td>32</td><td>20</td><td>33</td><td>30</td></tr><tr><td>가구,침구, 조명 (HS94)</td><td>830</td><td>2,287</td><td>16,690</td><td>749</td><td>1.3</td><td>1.3</td><td>1.7</td><td>4.2</td></tr><tr><td>완구,운동용구 (HS95)</td><td>33</td><td>268</td><td>459</td><td>11</td><td>0.1</td><td>0.2</td><td>0.0</td><td>0.1</td></tr><tr><td>종이목재(HS44~49)</td><td>639</td><td>1,570</td><td>25,484</td><td>533</td><td>1.0</td><td>0.9</td><td>2.6</td><td>3.0</td></tr><tr><td>금속(HS72~83)</td><td>3,090</td><td>10,541</td><td>66,298</td><td>396</td><td>4.9</td><td>6.0</td><td>6.8</td><td>2.2</td></tr><tr><td>기계(HS84)</td><td>3,694</td><td>10,101</td><td>63,690</td><td>661</td><td>5.8</td><td>5.8</td><td>6.6</td><td>3.7</td></tr><tr><td>전자,기기 (HS85,90)</td><td>2,121\n\n<table><caption>참여기업 리스트 (종합)</caption><tbody><tr><td>분야</td><td>참여기업수</td><td>비고</td></tr><tr><td>호텔업</td><td>40</td><td><br></td></tr><tr><td>여행업</td><td>12</td><td><br></td></tr><tr><td>휴양콘도미니엄업</td><td>10</td><td><br></td></tr><tr><td>국제회의업</td><td>9</td><td><br></td></tr><tr><td>카지노업</td><td>2</td><td><br></td></tr><tr><td>융·복합 관광</td><td>18</td><td><br></td></tr><tr><td>유원시설</td><td>1</td><td><br></td></tr><tr><td>해외취업관</td><td>9</td><td><br></td></tr><tr><td>미래일자리관</td><td>6</td><td><br></td></tr><tr><td>관광벤처관</td><td>13</td><td><br></td></tr><tr><td>계</td><td>120</td><td><br></td></tr></tbody></table>\n\n</td><td>11,661</td><td>222,128</td><td>6,853</td><td>3.3</td><td>6.7</td><td>23</td><td>38</td></tr><tr><td>수송기계(HS86~89)</td><td>3,620</td><td>5,846</td><td>18,987</td><td>157</td><td>5.7</td><td>3.4</td><td>2.0</td><td>0.9</td></tr><tr><td>화학(HS28~40)</td><td>7,881</td><td>46,464</td><td>91,033</td><td>2,223</td><td>12</td><td>27</td><td>9.4</td><td>12</td></tr><tr><td>광산물(HS25~27)</td><td>12,034</td><td>8,810</td><td>47,042</td><td>556</td><td>19</td><td>5.1</td><td>4.9</td><td>3.1</td></tr><tr><td>건재(HS68~70)</td><td>715</td><td>1,924</td><td>5,942</td><td>18</td><td>1.1</td><td>1.1</td><td>0.6</td><td>0.1</td></tr><tr><td>귀금속(HS71)</td><td>15</td><td>215</td><td>1,224</td><td>2</td><td>0.0</td><td>0.1</td><td>0.1</td><td>0.0</td></tr><tr><td>시계(HS91)</td><td>3</td><td>19</td><td>3,827</td><td>12</td><td>0.0</td><td>0.0</td><td>0.4</td><td>0.1</td></tr><tr><td>기타(HS92~93,96~)</td><td>1,079</td><td>4,187</td><td>6,287</td><td>66</td><td>1.7</td><td>2.4</td><td>0.6</td><td>0.4</td></tr><tr><td>계</td><td>63,490</td><td>174,368</td><td>969,599</td><td>17,924</td><td>100</td><td>100</td><td>100</td><td>100</td></tr></table>"""
    doctags = converter.to_doctags(html)
    print(doctags)
