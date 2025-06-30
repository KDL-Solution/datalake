# DataLake Management System

> Multimodal data management with automatic processing, deduplication, and querying

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-latest-green.svg)](https://fastapi.tiangolo.com/)


## Quick Start

### Installation

```bash
git clone https://github.com/KDL-Solution/datalake.git
cd datalake
pip install -e .

# Create directories (admin only)
mkdir -p /mnt/AI_NAS/datalake/{staging/{pending,processing,failed},catalog,assets,config,logs}
```

### Start Server (Admin)

```bash
datalake-server --port 8000 --num-proc 16
```

### Basic Usage

```python
from core.datalake import DatalakeClient

# Initialize
client = DatalakeClient(
    user_id="user",
    base_path="/mnt/AI_NAS/datalake",
    server_url="http://localhost:8091"
)

# Upload data
staging_dir, job_id = client.upload_raw(
    data_file="dataset.parquet",  # or DataFrame, HF Dataset
    provider="huggingface",
    dataset="coco_2017"
)

# Process and build database
job_id = client.trigger_processing()
result = client.wait_for_job_completion(job_id)
client.build_db()

# Search and export
results = client.search(
    providers=["huggingface"],
    datasets=["coco_2017"],
    tasks=["ocr"]
)
client.export(results, "./output", format="dataset", include_images=True)
```

## CLI Usage

```bash
# Configure
datalake config provider create huggingface
datalake config task create ocr

# Upload and process
datalake upload
datalake process start

# Query and export
datalake db update
datalake export
```

## Data Flow

```
Upload → Staging → Processing → Catalog (Parquet) + Assets → Database → Query → Export
```

1. **Upload**: Raw/task data goes to staging/pending
2. **Process**: Auto-process with deduplication to catalog + assets  
3. **Database**: Build DuckDB/Athena tables from parquet
4. **Query**: Search by hierarchy or JSON content
5. **Export**: Results to Parquet/Dataset/HF format

## Configuration

### Schema Config (`config/schema.yaml`)

```yaml
providers:
  huggingface:
    description: 'Hugging Face datasets'
  aihub:
    description: 'AI Hub datasets'

tasks:
  ocr:
    description: 'OCR results'
    required_fields: ['lang', 'src']
    allowed_values:
      lang: ['ko', 'en', 'ja']
      src: ['real', 'synthetic']
```

### Server Config (`config.yaml`)

```yaml
base_path: "/mnt/AI_NAS/datalake"
server_url: "http://192.168.20.62:8091"
num_proc: 16
```

## Data Structure

```
datalake/
├── staging/           # Upload queue
│   ├── pending/
│   ├── processing/
│   └── failed/
├── catalog/           # Parquet data (Hive partitioned)
│   └── provider=huggingface/
│       └── dataset=coco_2017/
│           ├── task=raw/variant=image/
│           └── task=ocr/variant=base_ocr/
└── assets/           # File storage
```

## API Reference

### Classes
- `DatalakeClient`: Main interface
- `DuckDBClient`: Local querying
- `DatalakeProcessor`: Data processing

### Endpoints
- `POST /process`: Start processing
- `GET /status`: Server status
- `GET /jobs/{job_id}`: Job status

## Development

```bash
# Dev server
uvicorn server.app:app --reload --port 8000

# CLI alternative
python main.py <command>
```

## Examples

### Task Data Upload

```python
# Upload OCR results
staging_dir, job_id = client.upload_task(
    data_file="ocr_results.parquet",
    provider="huggingface",
    dataset="coco_2017",
    task="ocr",
    variant="base_ocr",
    meta={"lang": "ko", "src": "real"}
)
```

### Advanced Search

```python
# Search with filters
results = client.search(
    providers=["huggingface", "aihub"],
    datasets=["coco_2017"],
    tasks=["ocr"],
    filter_json={"lang": "ko"}
)
```