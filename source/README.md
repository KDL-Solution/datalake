# 📊 DeepDocs 데이터 레이크 - 데이터셋 소스

> 문서 처리를 위한 원본 데이터셋 관리 저장소

## 📋 목차

- [데이터셋 개요](#overview)
- [데이터셋 카탈로그](#dataset-catalog)
- [데이터셋 상세](#datasets)

---


## 📑 데이터셋 개요

DeepDocs 데이터 레이크는 다양한 출처의 문서 처리 데이터셋을 통합 관리합니다.
데이터는 NAS  `/volume1/datalake/source/` 에 저장됩니다.

- **총 데이터셋 수**: 3개 (2025년 5월 27일 기준)
- **주요 제공처**: aihub, huggingface, inhouse
- **데이터 유형**: OCR, KIE, VQA, DocConv 등

## 📚 데이터셋 카탈로그

| 폴더명(영문)| 데이터셋 명| 제공처| 유형| 샘플 수| 상세정보|
|------------|-----------|-------|----|--------|--------|
| gangdong_kyunghee_hospital | 강동경희대병원 | inhouse | OCR/KIE | 3,672  | [상세](#gangdong_kyunghee_hospital) |
| tourism_food_menu_board | 관광 음식메뉴판 데이터 | aihub | OCR/KIE | 90,085 | [상세](#tourism_food_menu_board)  |
| pubtabnet_otsl | ds4sd/pubtabnet_otsl | huggingface | DocConv | 394,944 | [상세](#pubtabnet_otsl)  |
| invoice_kie | GokulRajaR/invoice-ocr-json | huggingface | KIE | 5,189 | [상세](#invoice_kie)  |

## 📂 데이터셋 상세

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

- **데이터셋명**: GokulRajaR/invoice-ocr-json
- **경로**: source/provider=huggingface/invoice_kie
- **수집일**: 2025-05-27
- **샘플 수**: 5,189
- **주요 폴더/파일**:
- **라벨 포맷/주요 필드**: kie
- **비고**: 
  - 2025-05-27 1차 수집
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
- **샘플 수**: 3,672
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