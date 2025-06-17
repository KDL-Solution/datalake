# DataLake Management System

대용량 멀티모달 데이터(이미지, 텍스트, 메타데이터)를 효율적으로 관리하는 엔터프라이즈급 데이터 레이크 시스템입니다.

## 🚀 주요 기능

### 📊 데이터 관리
- **Raw 데이터 업로드**: 원본 데이터셋을 Provider/Dataset 구조로 조직화
- **Task 데이터 관리**: OCR, VQA, KIE, Layout 등 특정 태스크용 데이터 생성
- **스키마 검증**: 데이터 타입과 메타데이터 자동 검증
- **중복 제거**: 해시 기반 이미지 및 파일 중복 제거

### 🔄 데이터 처리
- **병렬 처리**: 멀티프로세싱을 통한 대용량 데이터 고속 처리
- **이미지 최적화**: PIL 기반 이미지 압축 및 해시 생성
- **파일 관리**: 샤딩 기반 효율적 파일 저장 시스템
- **백그라운드 작업**: FastAPI 기반 비동기 처리 서버

### 🔍 데이터 조회
- **통합 검색**: DuckDB 및 AWS Athena 지원
- **파티션 기반 조회**: Provider/Dataset/Task/Variant 계층 구조
- **JSON 검색**: OCR 결과 등 JSON 데이터 내 텍스트 검색
- **다양한 출력**: Parquet, Arrow Dataset, HuggingFace Dataset 형태

## 📁 시스템 구조

```
datalake/
├── managers/                    # 핵심 관리 모듈
│   ├── datalake_client.py      # 메인 클라이언트
│   ├── nas_processor.py        # 데이터 처리 엔진
│   ├── processing_server.py    # FastAPI 처리 서버
│   ├── data_schema.py          # 스키마 관리
│   └── logging_setup.py        # 로깅 설정
├── client/                      # 쿼리 클라이언트
│   └── src/core/
│       ├── duckdb_client.py    # DuckDB 클라이언트
│       └── athena_client.py    # AWS Athena 클라이언트
├── cli.py                       # CLI 인터페이스
└── config/schema.yaml          # 스키마 설정
```

## 🛠️ 설치

### 1. 패키지 설치
```bash
git clone <repository>
cd datalake
pip install -e .
```

### 2. 필수 디렉토리 구조 생성
```bash
mkdir -p /mnt/AI_NAS/datalake/{staging/{pending,processing,failed},catalog,assets,config,logs}
```

### 3. NAS 처리 서버 실행
```bash
python managers/processing_server.py \
    --host 0.0.0.0 \
    --port 8091 \
    --base-path /mnt/AI_NAS/datalake \
    --num-proc 16 \
    --batch-size 1000
```

## 📖 사용법

### CLI 사용법

#### 1. 초기 설정
```bash
# Provider 생성
python cli.py config provider create

# Task 생성 (OCR 예시)
python cli.py config task create
# Task 이름: ocr
# 필수 필드: lang, src
# 허용 값: lang=ko,en,ja,multi / src=real,synthetic
```

#### 2. 데이터 업로드
```bash
# Raw 데이터 업로드
python cli.py upload
# 데이터 타입: raw
# 파일 경로: /path/to/dataset
# Provider: huggingface
# Dataset: coco_2017

# Task 데이터 업로드
python cli.py upload  
# 데이터 타입: task
# Provider: huggingface
# Dataset: coco_2017 (기존)
# Task: ocr
# Variant: base_ocr
# 메타데이터: lang=ko, src=real
```

#### 3. 데이터 처리
```bash
# 처리 시작
python cli.py process start

# 처리 상태 확인
python cli.py process status <JOB_ID>

# 내 데이터 현황
python cli.py process list
```

#### 4. 데이터 다운로드
```bash
# Catalog DB 구축 (최초 1회)
python cli.py catalog rebuild

# 데이터 다운로드
python cli.py download
# 검색 방법: 1 (파티션 기반) 또는 2 (텍스트 검색)
# 다운로드 형태: 1 (Parquet), 2 (Arrow), 3 (Dataset+이미지)
```

### Python API 사용법

#### 1. 기본 사용법
```python
from managers.datalake_client import DatalakeClient

# 클라이언트 초기화
client = DatalakeClient(
    base_path="/mnt/AI_NAS/datalake",
    nas_api_url="http://localhost:8091"
)

# Raw 데이터 업로드
staging_dir, job_id = client.upload_raw_data(
    data_file="dataset.parquet",  # 또는 pandas DataFrame
    provider="huggingface",
    dataset="coco_2017",
    dataset_description="COCO 2017 dataset for object detection"
)

# Task 데이터 업로드
staging_dir, job_id = client.upload_task_data(
    data_file=processed_df,
    provider="huggingface", 
    dataset="coco_2017",
    task="ocr",
    variant="base_ocr",
    meta={"lang": "ko", "src": "real"}
)
```

#### 2. 데이터 조회
```python
from client.src.core.duckdb_client import DuckDBClient

# DuckDB 클라이언트
with DuckDBClient("/mnt/AI_NAS/datalake/catalog.duckdb") as duck:
    # 파티션 조회
    partitions = duck.retrieve_partitions("catalog")
    
    # 조건부 데이터 조회
    data = duck.retrieve_with_existing_cols(
        providers=["huggingface"],
        datasets=["coco_2017"], 
        tasks=["ocr"],
        variants=["base_ocr"]
    )
```

## 🔧 설정

### 스키마 설정 (config/schema.yaml)
```yaml
providers:
  - huggingface
  - aihub
  - inhouse

tasks:
  ocr:
    required_fields: [lang, src]
    allowed_values:
      lang: [ko, en, ja, multi]
      src: [real, synthetic]
  
  vqa:
    required_fields: [lang, src]
    allowed_values:
      lang: [ko, en, ja, multi] 
      src: [real, synthetic]
```

### 환경 변수
```bash
export BASE_PATH="/mnt/AI_NAS/datalake"
export NAS_API_URL="http://localhost:8091"
export LOG_LEVEL="INFO"
export NUM_PROC="16"
export BATCH_SIZE="1000"
```

## 📊 데이터 구조

### Catalog 구조
```
catalog/
├── provider=huggingface/
│   └── dataset=coco_2017/
│       ├── task=raw/
│       │   ├── variant=image/
│       │   │   ├── data.parquet     # 메타데이터
│       │   │   └── _metadata.json   # 업로드 정보
│       │   └── variant=mixed/
│       └── task=ocr/
│           └── variant=base_ocr/
└── provider=aihub/
    └── dataset=document_ocr/
```

### Assets 구조 (샤딩)
```
assets/
├── provider=huggingface/
│   └── dataset=coco_2017/
│       ├── ab/
│       │   ├── cd/
│       │   │   ├── abcd1234...hash.jpg
│       │   │   └── abcd5678...hash.jpg
│       │   └── ef/
│       └── gh/
```

## 🛡️ 보안 및 성능

### 성능 최적화
- **병렬 처리**: 멀티프로세싱으로 CPU 집약적 작업 분산
- **메모리 관리**: 배치 단위 처리로 메모리 사용량 제어
- **해시 캐싱**: 중복 제거를 위한 인메모리 해시 캐시
- **샤딩**: 대용량 파일을 위한 디렉토리 샤딩

### 데이터 무결성
- **해시 검증**: SHA256 기반 파일 무결성 검증
- **트랜잭션**: 처리 실패 시 자동 롤백
- **스키마 검증**: 업로드 전 메타데이터 검증
- **중복 제거**: 해시 기반 자동 중복 제거

## 🔗 통합 시스템

### AWS Athena 연동
```python
from client.src.core.athena_client import AthenaClient

athena = AthenaClient(
    database="my_catalog_db",
    s3_output="s3://my-bucket/athena-results/"
)

# JSON 내 텍스트 검색
results = athena.search_text_in_content(
    table="catalog",
    column="ocr_result", 
    search_text="invoice",
    variants="word"
)
```

### HuggingFace Datasets 연동
```python
from datasets import load_from_disk

# Dataset 형태로 다운로드된 데이터 로드
dataset = load_from_disk("./downloads/my_dataset")

# 이미지 확인
dataset[0]['image'].show()

# pandas로 변환
df = dataset.to_pandas()
```

## 🐛 TroubleShoot

### 일반적인 문제

**1. NAS API 연결 실패**
```bash
# 서버 상태 확인
curl http://localhost:8091/health

# 서버 재시작
python managers/processing_server.py --port 8091
```

**2. 메모리 부족**
```python
# 배치 크기 줄이기
client = DatalakeClient(num_proc=4, batch_size=500)
```

**3. 권한 문제**
```bash
# 디렉토리 권한 설정
chmod -R 775 /mnt/AI_NAS/datalake
chown -R $USER:$GROUP /mnt/AI_NAS/datalake
```