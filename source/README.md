# 📊 DeepDocs 데이터 레이크 - 원본 데이터셋 소스

> 전처리 이전의 원본 데이터셋 관리 저장소

## 📋 목차

- [원본 데이터셋 개요](#overview)
- [원본 데이터셋 카탈로그](#dataset-catalog)
- [원본 데이터셋 상세](#datasets)

---


## 📑 원본 데이터셋 개요 <a id="overview"></a>

DeepDocs 데이터 레이크는 다양한 출처의 문서 처리 원본 데이터셋을 통합 관리합니다.
원본 데이터는 NAS `/AI_NAS/datalake/source/` 에 저장되며, 전처리 이전의 수집 상태로 유지됩니다.

- **총 원본 데이터셋 수**: 12개 (2025년 5월 29일 기준)
- **주요 제공처**: aihub(5), huggingface(6), inhouse(1), opensource(1)
- **데이터 유형**: OCR, KIE, MRC, DocConv, Layout, 테이블 등

## 📚 원본 데이터셋 카탈로그 <a id="dataset-catalog"></a>

| 폴더명(영문)| 데이터셋 명| 제공처| 유형| 샘플 수| 상세정보|
|------------|-----------|-------|----|--------|--------|
| gangdong_kyunghee_hospital | 강동경희대병원 | inhouse | OCR/KIE | 3,672  | [상세](#gangdong_kyunghee_hospital) |
| tourism_food_menu_board | 관광 음식메뉴판 데이터 | aihub | OCR/KIE | 90,085 | [상세](#tourism_food_menu_board)  |
| pubtabnet_otsl | ds4sd/pubtabnet_otsl | huggingface | DocConv | 394,944 | [상세](#pubtabnet_otsl)  |
| invoice_kie | GokulRajaR/invoice-ocr-json, ikram98ai/invoice_img2json | huggingface | KIE | 5,189 | [상세](#invoice_kie)  |
| fatura2_invoices | arlind0xbb/Fatura2-invoices-original-strat1, arlind0xbb/Fatura2-invoices-original-strat2 | huggingface | KIE | 5,250 | [상세](#fatura2_invoices)  |
| vis_qa | 시각화 자료 질의응답 데이터 | aihub | VQA | 129,213 | [상세](#vis_qa)  |
| fatura2_invoices | arlind0xbb/Fatura2-invoices-original-strat1, arlind0xbb/Fatura2-invoices-original-strat2 | huggingface | KIE | 1,250 | [상세](#fatura2_invoices)  |
| synth_invoices_en | Nabin1995/invoice-dataset-layoutlmv3 | huggingface | Layout | 10,000 | [상세](#synth_invoices_en) |
| admindocs_mrc | 행정 문서 대상 기계독해 데이터 | aihub | DocConv | 50,073 | [상세](#admindocs_mrc)  |
| tech_sci_mrc | 기술과학 문서 기계독해 데이터 | aihub | DocConv | 8,148 | [상세](#tech_sci_mrc)  |
| table_qa | 표 정보 질의응답 데이터 | aihub | DocConv | 176,631 | [상세](#table_qa)  |
## 📂 원본 데이터셋 상세 <a id="datasets"></a>

<details>
<summary><b>🏛️ aihub</b></summary>

<details>
<summary id="tourism_food_menu_board"><b>tourism_food_menu_board</b></summary>

- **데이터셋명(한글)**: 관광 음식메뉴판 데이터
- **경로**: source/provider=aihub/tourism_food_menu_board
- **수집일**: 2025-05-23
- **샘플 수**: 90,085
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: JSON (bbox, text)
- **비고**: 
  - 2025-05-23 1차 수집
</details>
<details>
<summary id="vis_qa"><b>vis_qa</b></summary>

- **데이터셋명**: 시각화 자료 질의응답 데이터
- **경로**: source/provider=aihub/pubtabnet_otsl
- **수집일**: 2025-05-23
- **샘플 수**: 129,213
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: query, label
- **비고**: 
  - 표, 차트, 플로우 차트 등이 포함된 문서.
  - .png의 출처가 되는 .pdf를 알아낼 수 있으나 몇 페이지에서 온 것인지 알 수 없음.
</details>

<details>
<summary id="admindocs_mrc"><b>admindocs_mrc</b></summary>

- **데이터셋명(한글)**: 행정 문서 대상 기계독해 데이터
- **경로**: source/provider=aihub/admindocs_mrc
- **수집일**: 2025-05-29
- **샘플 수**: 50,073
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: html
- **비고**: 
  - 2025-05-29 1차 수집
</details>
<details>
<summary id="tech_sci_mrc"><b>tech_sci_mrc</b></summary>

- **데이터셋명(한글)**: 기술과학 문서 기계독해 데이터
- **경로**: source/provider=aihub/tech_sci_mrc
- **수집일**: 2025-05-29
- **샘플 수**: 8,148
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: html
- **비고**: 
  - 2025-05-29 1차 수집
</details>
<details>
<summary id="table_qa"><b>table_qa</b></summary>

- **데이터셋명(한글)**: 표 정보 질의응답 데이터
- **경로**: source/provider=aihub/table_qa
- **수집일**: 2025-05-29
- **샘플 수**: 176,631
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: html
- **비고**: 
  - 2025-05-30 1차 수집
</details>

</details>

--- 

<details>
<summary><b>🤗 huggingface</b></summary>

<details>
<summary id="pubtabnet_otsl"><b>pubtabnet_otsl</b></summary>

- **데이터셋명**: ds4sd/PubTabNet_OTSL
- **경로**: source/provider=huggingface/pubtabnet_otsl
- **수집일**: 2025-05-23
- **샘플 수**: 394,944
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: otsl, html, cell
- **비고**: 
  - 2025-05-23 1차 수집
</details>
<details>
<summary id="invoice_kie"><b>invoice_kie</b></summary>

- **데이터셋명**: GokulRajaR/invoice-ocr-json, ikram98ai/invoice_img2json
- **경로**: source/provider=huggingface/invoice_kie
- **수집일**: 2025-05-27
- **샘플 수**: 5,189
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: kie
- **비고**: 
  - 2025-05-27 1차 수집
</details>
<details>
<summary id="fatura2_invoices"><b>fatura2_invoices</b></summary>

- **데이터셋명**: arlind0xbb/Fatura2-invoices-original-strat1, arlind0xbb/Fatura2-invoices-original-strat2
- **경로**: source/provider=huggingface/fatura2_invoices
- **수집일**: 2025-05-27
- **샘플 수**: 1,250
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: kie
- **비고**: 
  - 2025-05-27 1차 수집, 중복 제거
</details>
<details>
<summary id="synth_invoices_en"><b>synth_invoices_en</b></summary>

- **데이터셋명**: Nabin1995/invoice-dataset-layoutlmv3
- **경로**: source/provider=huggingface/synth_invoices_en
- **수집일**: 2025-05-27
- **샘플 수**: 10,000
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: layout
- **비고**: 
  - 2025-05-27 1차 수집
</details>
<details>
<summary id="funsd_plus"><b>funsd_plus</b></summary>

- **데이터셋명**: funsd_plus  
- **경로**: source/provider=huggingface/funsd_plus  
- **수집일**: 2025-05-28  
- **샘플 수**: 1,139  
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: JSON (bbox, text, class 등 KIE 라벨)  
- **비고**:  
  - 2025-05-28 1차 수집  
  - 원래는 VQA 용(question-answer 쌍 기반) 구조였으나 KIE 태스크로 변환  
  - OCR 기반 KIE 데이터셋으로 사용됨  
  - FUNSD를 확장한 구조적 key-value 태깅 포함  
</details>

</details>

---

<details>
<summary><b>🏥 inhouse</b></summary>

<details>
<summary id="gangdong_kyunghee_hospital"><b>gangdong_kyunghee_hospital</b></summary>

- **데이터셋명(한글)**: 강동경희대병원 진료/처방 OCR
- **경로**: source/provider=inhouse/gangdong_kyunghee_hospital
- **수집일**: 2024-08-13
- **샘플 수**: 159,153
- **주요 폴더/파일**:
  ```
  data/
  ├─ examinations/ (images/, labels/, metadata.jsonl)
  └─ prescriptions/ (images/, labels/, metadata.jsonl)
  ```
- **라벨 포맷/주요 필드**: JSON (bbox, text, class, line_num, date)
- **비고**: 
  - 2024-08-13 1차 수집 (ocr)
  - 2025-05-26 metadata.jsonl 추가 (kie)
</details>

</details>

---

<details>
<summary><b>📄 opensource</b></summary>

<details>
<summary id="real_kie"><b>real-kie</b></summary>

- **데이터셋명**: real-kie  
- **경로**: source/provider=opensource/real-kie  
- **수집일**: 2025-05-28  
- **샘플 수**: 23,187  
- **주요 폴더/파일**:
  ```
  charities           : 8,370
  fcc_invoices        : 1,812
  nda                 : 2,574
  resource_contracts  : 33,868
  s1                  : 86,371
  s1_pages            : 13,079
  s1_trimmed          : 0
  s1_truncated        : 13,079
  ```
- **라벨 포맷/주요 필드**: JSON (label[text, start, end], ocr[token, bbox])  
- **비고**:  
  - 2025-05-28 1차 수집 완료  
  - 도메인 단위(폴더별)로 문서 유형이 나뉘어 있음  
  - 각 폴더별 `train.csv`, `val.csv`, `test.csv` 형태로 OCR + KIE 라벨 존재  
  - OCR 결과는 `ocr/*.json.gz`로 존재하며, `image_files` 열로 이미지 경로와 연결  
  - 원본 PDF는 s1_pages, s1_trimmed 등 별도 폴더에 포함됨
</details>

</details>

---