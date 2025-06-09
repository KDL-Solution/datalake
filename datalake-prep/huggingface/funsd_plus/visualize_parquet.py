from pathlib import Path
import pandas as pd
import json
from utils import draw_vqa_label_on_image


THIS_DIR = Path(__file__).resolve().parent
BASE = THIS_DIR.parents[4]
BASE_STAGING = BASE / "staging"

PARQUET_PATH = (
    BASE_STAGING
    / "huggingface/funsd_plus/vqa/kie_struct/lang=en/src=real/year=2025/month=05/day=21/funsd_plus_train.parquet"
)


VIS_DIR = THIS_DIR / "visualization"
VIS_DIR.mkdir(parents=True, exist_ok=True)


def main(index: int):
    df = pd.read_parquet(PARQUET_PATH)

    if index >= len(df):
        print(f"❌ 인덱스 {index}는 전체 길이 {len(df)}보다 큽니다.")
        return

    sample = df.iloc[index]
    label = json.loads(sample["label"])
    image_rel_path = sample["image_path"]
    image_path = BASE_STAGING / image_rel_path

    out_path = VIS_DIR / f"vis_{index}.jpg"
    draw_vqa_label_on_image(image_path, label, out_path)
    print(f"✅ 저장 완료: {out_path}")


if __name__ == "__main__":
    main(index=1000)
