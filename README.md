# 📊 DeepDocs 데이터 레이크

> DeepDocs 프로젝트의 데이터 레이크 구조 및 파이프라인

## 🗂️ 데이터 레이크 구조

DeepDocs 데이터 레이크는 다음의 주요 구성요소로 이루어져 있습니다:

### 1. 소스 데이터 (Source)
- **위치**: `/AI_NAS/datalake/source/`
- **설명**: 전처리 이전의 원본 데이터셋을 저장합니다.
- **구조**: `source/provider={제공처}/{데이터셋명}/...`
- **문서**: [원본 데이터셋 README](./source/README.md)

### 2. 스테이징 영역 (Staging)
- **위치**: `/AI_NAS/datalake/_staging/`
- **설명**: 데이터 처리 중인 임시 저장소입니다.
- **구조**: `_staging/{task}/{provider}/{dataset}/{variant}/{partitions}/{uuid}/...`
- **문서**: [데이터 준비 README](./datalake-prep/README.md)

### 3. 카탈로그 (Catalog)
- **위치**: `/AI_NAS/datalake/catalog/`
- **설명**: 전처리가 완료된 최종 데이터를 저장합니다.
- **구조**: `catalog/{provider}/{dataset}/images/` (공통 이미지 저장소)
- **구조**: `catalog/{provider}/{dataset}/{task}/{variant}/{partitions}/data.parquet` (라벨 및 메타데이터)

## 🔄 데이터 처리 파이프라인

1. **수집 (Collect)**
   - 다양한 출처의 원본 데이터를 `source/` 경로에 저장
   - `소스 README.md`에 데이터셋 정보 문서화

2. **준비 (Prepare)**
   - 원본 데이터를 가공하여 파켓(Parquet) 형식으로 변환
   - `publish_to_staging.py`를 통해 준비된 데이터를 스테이징 영역으로 업로드

3. **검증 (Validate)**
   - 스테이징 영역의 데이터 품질 및 무결성 검증
   - 이미지 경로, 메타데이터 등의 정확성 확인

4. **커밋 (Commit)**
   - `commit_staging.py`를 통해 검증된 데이터를 카탈로그로 최종 이관
   - 중복 데이터 관리 및 버전 관리 수행

5. **활용 (Use)**
   - 카탈로그의 데이터를 학습, 추론, 분석 등에 활용
   - Athena 등의 도구를 통한 데이터 쿼리 및 분석

## 📚 유틸리티 및 도구

- **datalake-prep/**: 데이터 처리 및 파이프라인 스크립트
- **athena/**: AWS Athena 연동을 위한 유틸리티

## 📖 관련 문서

- [원본 데이터셋 관리](./source/README.md)
- [데이터 준비 및 파이프라인](./datalake-prep/README.md)
- [Athena 연동 가이드](./athena/README.md)
