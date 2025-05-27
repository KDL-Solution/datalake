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
- **주요 제공처**: AIHub, HuggingFace, 사내제작
- **데이터 유형**: OCR, MRC, KIE, 레이아웃 등

---

## 📚 데이터셋 카탈로그

| 폴더명(영문)               | 데이터셋 명    | 제공처  | 유형    | 샘플 수 | 상세정보                         |
| -------------------------- | -------------- | ------- | ------- | ------- | -------------------------------- |
| gangdong_kyunghee_hospital | 강동경희대병원 | inhouse | OCR/KIE | 3,672   | [상세](#gangdong_kyunghee_hospital) |

---

## 📂 데이터셋 상세

<details>
<summary id="gangdong_kyunghee_hospital"><b>gangdong_kyunghee_hospital</b></summary>

- **데이터셋명(한글)**: 강동경희대병원 진료/처방 OCR
- **제공처(Provider)**: inhouse
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
- **비고**: 2024-08-13 1차 수집, 2025-05-26 라벨 병합

</details>
