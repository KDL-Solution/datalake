# DataLake Management System

Multimodal data management system with automatic processing, deduplication, and querying capabilities.

## Features

- **Data Management**: Raw and task-specific data with Provider/Dataset/Task/Variant hierarchy
- **Processing**: Parallel processing with image optimization and file deduplication
- **Querying**: DuckDB and AWS Athena support with partition and JSON search
- **API Server**: FastAPI async processing server
- **CLI**: Command-line interface for data operations

## Architecture

```
datalake/
├── core/                   # Data management
├── server/                 # Processing server  
├── clients/                # Query clients (DuckDB, Athena)
├── staging/                # Processing pipeline
│   ├── pending/
│   ├── processing/
│   └── failed/
├── catalog/                # Parquet data
└── assets/                 # File storage
```

## Installation

```bash
git clone https://github.com/KDL-Solution/datalake.git
cd datalake
pip install -e .

# # Create required directories (if you are the admin)
mkdir -p /mnt/AI_NAS/datalake/{staging/{pending,processing,failed},catalog,assets,config,logs}
```

## Usage

### 1. Start Processing Server (if you are the admin)

```bash
datalake-server --port 8000 --num-proc 16
# or: python -m server.app --port 8000 --num-proc 16
```

### 2. Python API

```python
from core.datalake import DatalakeClient

# Initialize client
client = DatalakeClient(
    user_id="user",
    base_path="/mnt/AI_NAS/datalake",
    server_url="http://localhost:8091"
)

# Upload raw data
staging_dir, job_id = client.upload_raw(
    data_file="dataset.parquet",  # or pandas DataFrame, HF Dataset
    provider="huggingface",
    dataset="coco_2017"
)

# Upload task data  
staging_dir, job_id = client.upload_task(
    data_file="ocr_results.parquet", 
    provider="huggingface",
    dataset="coco_2017", 
    task="ocr",
    variant="base_ocr",
    meta={"lang": "ko", "src": "real"}
)

# Trigger processing
job_id = client.trigger_processing()
result = client.wait_for_job_completion(job_id)

# Build database
client.build_db()

# Search data
results = client.search(
    providers=["huggingface"],
    datasets=["coco_2017"],
    tasks=["ocr"]
)

# Export results
client.export(results, "./output", format="dataset", include_images=True)
```

### 3. CLI Usage

```bash
# Configure providers and tasks
datalake config provider create huggingface
datalake config task create ocr

# Upload and process data
datalake upload
datalake process start

# Build database and export
datalake db update
datalake export

# or use: python main.py <command>
```

## Configuration

### Schema Configuration (`config/schema.yaml`)

```yaml
providers:
  huggingface:
    description: 'Hugging Face datasets'
  aihub:
    description: 'AI Hub public datasets'

tasks:
  ocr:
    description: 'Optical Character Recognition'
    required_fields: ['lang', 'src']
    allowed_values:
      lang: ['ko', 'en', 'ja', 'multi']
      src: ['real', 'synthetic']
```

### Server Configuration (`config.yaml`)

```yaml
base_path: "/mnt/AI_NAS/datalake"
server_url: "http://192.168.20.62:8091"
log_level: "INFO"
num_proc: 16
```

## Data Flow

1. **Upload**: Raw/task data → staging/pending
2. **Process**: pending → processing → catalog (parquet) + assets (files)
3. **Database**: Catalog parquet files → DuckDB/Athena tables
4. **Query**: Search by partitions or JSON content
5. **Export**: Results → Parquet/Dataset/HF Dataset

## Development

### Server Development

```bash
# Dev server with reload
uvicorn server.app:app --reload --host 0.0.0.0 --port 8000
```

### Database Schema

Data is stored in Hive-partitioned Parquet format:
```
catalog/
├── provider=huggingface/
│   └── dataset=coco_2017/
│       ├── task=raw/variant=image/
│       └── task=ocr/variant=base_ocr/
```

Required columns: `hash`, `path`, task-specific metadata fields

## API

### Classes

- `DatalakeClient`: Main client
- `DuckDBClient`: Local querying  
- `DatalakeProcessor`: Data processing
- `SchemaManager`: Configuration management

### Endpoints

- `POST /process`: Start processing
- `GET /status`: Server status  
- `GET /jobs/{job_id}`: Job status
- `GET /jobs`: List jobs