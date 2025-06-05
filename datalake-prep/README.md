# DeepDocs ETL 파이프라인 사용 가이드

이 문서는 DeepDocs 프로젝트의 ETL(Extract, Transform, Load) 파이프라인을 사용하여 이미지와 데이터를 처리하고, 스테이징 영역을 거쳐 카탈로그로 옮기는 전체 과정을 설명합니다.

## 작업 흐름 개요

1. **전처리**: 원본 이미지와 데이터 가공
2. **스키마 생성**: Parquet 파일로부터 메타데이터 스키마 생성
3. **스테이징 게시**: 처리된 데이터와 이미지를 스테이징 영역으로 업로드
4. **커밋**: 스테이징 영역에서 카탈로그 영역으로 데이터 이관
5. **S3 업로드**: 카탈로그 데이터를 AWS S3로 복제

## 디렉토리 구조

```
- _staging/{provider}/{dataset}/images/            ← 모든 variant 공통
- _staging/{provider}/{dataset}/{task}/{variant}/{partitions}/{uuid}/
      ├── data.parquet
      └── _meta.json

- catalog/{provider}/{dataset}/images/            ← 집합적으로 병합됨(중복 없음)
- catalog/{provider}/{dataset}/{task}/{variant}/{partitions}/data.parquet
```

## 1. 전처리 작업

전처리 과정에서는 다음과 같은 결과물을 생성합니다:
- `images/` 폴더: 처리된 이미지 파일들
- `data.parquet`: 이미지에 연결된 메타데이터와 레이블 정보

주의사항:
- Parquet 파일에는 반드시 `image_path` 컬럼이 포함되어야 함
- `image_path`는 `images` 루트 아래의 상대 경로여야 함 (예: 'ea/0001.jpg')

## 2. 스키마 생성

전처리 완료 후 Parquet 파일에서 스키마 생성:

```bash
python make_schema_from_parquet.py --parquet /경로/data.parquet --name 스키마이름
```

실행 예:
```bash
python make_schema_from_parquet.py --parquet ./processed/data.parquet --name kie_kv_struct_v1
```

결과:
- Parquet과 동일한 이름에 `.json` 확장자로 스키마 파일 생성 (예: data.parquet → data.json)
- 스키마에는 컬럼 정보, 데이터 타입, SHA-256 무결성 값 등이 포함됨

## 3. 스테이징 게시

생성된 데이터를 스테이징 영역으로 업로드:

```bash
python publish_to_staging.py --provider [제공자] --dataset [데이터셋명] \
                          --task [태스크명] --variant [변형명] \
                          --partitions [파티션정보] --parquet [파케이파일경로] \
                          --images [이미지폴더경로]
```

실행 예:
```bash
python publish_to_staging.py --provider aihub --dataset kor_docs \
                          --task ocr --variant base \
                          --partitions "lang=ko,src=real" \
                          --parquet ./processed/data.parquet \
                          --images ./processed/images
```

파라미터 설명:
- `provider`: 데이터 출처 (aihub, huggingface, opensource, inhouse 중 하나)
- `dataset`: 데이터셋 이름
- `task`: 작업 유형 (ocr, kie, vqa, layout, document_conversion 중 하나)
- `variant`: 데이터셋 변형 (base, v2 등)
- `partitions`: 쉼표로 구분된 key=value 쌍 (예: lang=ko,src=real)
- `parquet`: 데이터 파일 경로
- `images`: (선택) 이미지 폴더 경로

실행 결과:
- 스테이징 영역에 UUID를 포함한 폴더 생성
- data.parquet, _meta.json 파일 복사
- images 폴더가 제공된 경우 이미지도 복사

## 4. 스테이징 커밋

스테이징 영역의 데이터를 카탈로그로 이관:

```bash
python commit_staging.py
```

이 스크립트는:
1. 스테이징 영역의 모든 data.parquet 파일을 검색
2. 각 파일 옆의 _meta.json 파일에서 메타데이터 읽기
3. 경로를 카탈로그 규칙에 맞게 변환하여 이관
4. 이미지 파일의 무결성 검사 및 카탈로그 영역으로 이관
5. Parquet 파일의 image_path를 카탈로그 절대경로 (/mnt/AI_NAS/datalake의 뒷부분)로 업데이트
6. date 컬럼 추가
7. 스테이징 영역의 처리된 파일 정리

## 5. S3 업로드

카탈로그에 커밋된 데이터를 AWS S3로 복제:

```bash
python s3_upload_parquet.py --parquet [카탈로그경로/data.parquet]
```

실행 예:
```bash
python s3_upload_parquet.py --parquet /mnt/AI_NAS/datalake/catalog/provider=aihub/dataset=kor_docs/task=ocr/variant=base/lang=ko/src=real/data.parquet
```

추가 옵션:
- `--nas-root`: NAS 루트 경로 (기본값: /mnt/AI_NAS/datalake)
- `--bucket`: S3 버킷 이름 (기본값: kdl-data-lake)
- `--s3-prefix`: S3 키 접두사 (선택)
- `--run_crawler`: Crawler 실행

## 전체 작업 흐름 예시

```bash
# 1. 전처리 작업 수행 (사용자 정의 스크립트)
# 결과: ./processed/images/ 디렉토리와 ./processed/data.parquet 파일

# 2. 스키마 생성
python make_schema_from_parquet.py --parquet ./processed/data.parquet --name kie_kv_struct_v1

# 3. 스테이징 게시
python publish_to_staging.py --provider aihub --dataset kor_docs \
                          --task ocr --variant base \
                          --partitions "lang=ko,src=real" \
                          --parquet ./processed/data.parquet \
                          --images ./processed/images

# 4. 스테이징 커밋
python commit_staging.py # --dry-run 을 할 경우, 시뮬레이션.

# 5. S3 업로드 (선택사항)
python s3_upload_parquet.py --parquet /mnt/AI_NAS/datalake/catalog/provider=aihub/dataset=kor_docs/task=ocr/variant=base/lang=ko/src=real/data.parquet
```

## 주의사항 및 요구사항

1. **Parquet 형식 요구사항**:
   - `image_path` 컬럼 필수
   - 이미지 경로는 images/ 폴더 하위의 상대 경로여야 함
     - ex) ea/ea1ddg181122214.jpg

2. **파티션 형식**:
   - task별 필수 파티션 키가 다름
   - ocr, kie: lang, src
   - vqa: lang, src
   - layout: lang, src, mod
   - document_conversion: lang, src

3. **허용 값**:
   - lang: ko, en, ja, multi, zxx
   - src: real, synthetic
   - mod: image, html, layout, table, chart, doctag

4. **NAS 마운트 필요**:
   - 스크립트 실행 전 /mnt/AI_NAS/datalake가 마운트되어 있어야 함

5. **환경 변수 설정 (S3 업로드용)**:
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY
   - AWS_DEFAULT_REGION
