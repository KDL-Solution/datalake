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
mkdir -p /mnt/AI_NAS/datalake/{staging/{pending,processing,failed},catalog,assets,collections,config,logs}
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
client.upload_raw(
    data_source="dataset.parquet",  # or DataFrame, HF Dataset
    provider="huggingface",
    dataset="coco_2017"
)

client.upload_task(
    data_source="ocr_results.parquet", # or DataFrame, HF Dataset
    provider="huggingface", 
    dataset="coco_2017",
    task="ocr",
    variant="base_ocr",
    meta={"lang": "ko", "src": "real"}
)

# Process and build database
job_id = client.trigger_processing()
result = client.wait_for_job_completion(job_id)
client.build_db()

# Search and download
results = client.search(
    providers=["huggingface"],
    datasets=["coco_2017"],
    # tasks=["ocr"],
    # variant=["*"],
)

client.download(results, "./output", format="dataset", include_images=True)

# Or get as dataset object directly
dataset = client.to_dataset(
    search_results=results,
    include_images=True,
)

# Create managed collection for training
client.import_collection(
    data_source=results,
    name="korean_ocr_train",
    version="v1.0",
    description="Korean OCR training dataset"
)
```

## CLI Usage

```bash
# Configure
datalake config provider create huggingface
datalake config task create ocr

# Upload and process
datalake upload
datalake process start

# Query and download
datalake db update
datalake download
datalake download --as-collection # Save as managed collection

# Manage collections
datalake collections list
datalake collections info
datalake collections import
datalake collections export
datalake collections delete
```

## Collections Management

Collections provide versioned storage for training datasets with automatic metadata tracking.

### Create Collections
```python
# From catalog search results
results = client.search(providers=["huggingface"], tasks=["ocr"])
client.import_collection(
    data_source=results,
    name="my_training_set",
    version="v1.0",
    description="OCR training data v1"
)

# Import external data
client.import_collection(
    data_source="external_data", # dataset, parquet path
    name="external_dataset", 
    version="v1.0",
    description="Test"
)
```

### Use Collections
```python
# List all collections
collections = client.collection_manager.list_collections()

# Load collection (get as dataset object directly)
dataset = client.load_collection("my_training_set", "v1.0")

# Export collection
client.export_collection(
    name="my_training_set", 
    version="v1.0", 
    output_path="./training_data",
    format="datasets"
)

```

## Data Flow

```
Upload → Staging → Processing → Catalog (Parquet) + Assets → Database → Query → Download/Collections
```

1. **Upload**: Raw/task data goes to staging/pending
2. **Process**: Auto-process with deduplication to catalog + assets  
3. **Database**: Build DuckDB/Athena tables from parquet
4. **Query**: Search by hierarchy or JSON content
5. **Download**: Results to Parquet/Dataset/HF format
6. **Collections**: Version-managed datasets for training

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
├── assets/           # File storage (deduplicated)
├── collections/      # Versioned training datasets
│   ├── korean_ocr_train/
│   │   ├── v1.0/
│   │   ├── v1.1/
│   │   └── latest
│   └── sentiment_data/
│       ├── v1.0/
│       └── latest
└── config/
```

## API Reference

### Classes
- `DatalakeClient`: Main interface
- `CollectionManager`: Version-managed datasets
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