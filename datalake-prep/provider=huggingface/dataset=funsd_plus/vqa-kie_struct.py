from pathlib import Path
from utils import convert_arrow_to_vqa_parquet
from datetime import datetime

BASE = Path("/mnt/AI_NAS/datalake")

STAGING_ROOT = Path("./staging")

PROVIDER = "huggingface"
DATASET = "funsd_plus"
TASK = "vqa"
VARIANT = "kie_struct"
PARTITION = "lang=en/src=real"

DATE = datetime.today()
YEAR = f"year={DATE.year}"
MONTH = f"month={DATE.month:02}"
DAY = f"day={DATE.day:02}"

STAGING_DIR = (
    STAGING_ROOT / PROVIDER / DATASET / TASK / VARIANT / PARTITION / YEAR / MONTH / DAY
)
IMAGE_ROOT = STAGING_ROOT / PROVIDER / DATASET / "images"

THIS_DIR = Path(__file__).resolve().parent
PROMPT_CONFIG_PATH = THIS_DIR / "prompt_config.yaml"


def main():
    runs = [
        {
            "arrow": BASE / "source/huggingface/funsd_plus/funsd_plus-train.arrow",
            "outfile": "funsd_plus_train.parquet",
        },
        {
            "arrow": BASE / "source/huggingface/funsd_plus/funsd_plus-test.arrow",
            "outfile": "funsd_plus_test.parquet",
        },
    ]

    for run in runs:
        convert_arrow_to_vqa_parquet(
            arrow_path=str(run["arrow"]),
            output_dir=STAGING_DIR,
            image_root=IMAGE_ROOT,
            output_file_name=run["outfile"],
            base_dir=STAGING_ROOT,
            prompt_config_path=PROMPT_CONFIG_PATH,
        )


if __name__ == "__main__":
    main()
