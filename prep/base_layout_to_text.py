# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
import swifter
from pathlib import Path
import pandas as pd
from typing import List, Dict
from PIL import Image
from tqdm import tqdm
from io import BytesIO

import sys
sys.path.insert(0, "/home/eric/workspace/datalake/")
from prep.utils import DATALAKE_DIR, get_safe_image_hash_from_pil
from core.datalake import DatalakeClient
from export.recognition_exporter import RecogntionCharExporter
from export.utils import (
    save_df_as_jsonl,
    denormalize_bboxes,
    layout_category_dict,
    user_prompt_dict,
)

tqdm.pandas()


class BaseLayoutToText(object):
    def _crop(
        self,
        image_path: str,
        elements: List[Dict[str, str]],
    ):
        try:
            image = Image.open(image_path).convert("RGB")
        except OSError:
            print(f"[ERROR] Cannot open image: {image_path}")
            return [], []
        else:
            image_bytes_ls = []
            labels = []
            # queries = []
            for el in elements:
                if not el["value"]:
                    continue

                image_crop = image.crop(el["bbox"])
                buffer = BytesIO()
                image_crop.save(
                    buffer,
                    format="JPEG",
                )
                image_bytes_ls.append(
                    buffer.getvalue()
                )

                labels.append(el["value"])
            return image_bytes_ls, labels

    def convert(
        self,
        df: pd.DataFrame,
    ) -> None:
        df_copy = df.copy()

        df_copy["label"] = df_copy.apply(
            lambda x: denormalize_bboxes(
                x["label"],
                width=x["width"],
                height=x["height"],
                bbox_key="bbox",
            ),
            axis=1,
        )  # Denormalize.
        df_copy["label"] = df_copy["label"].apply(
            lambda x: json.loads(x),
        )  # String to Dict.

        results = df_copy.swifter.apply(
            lambda x: self._crop(
                image_path=x["path"],
                elements=x["label"]["elements"],
            ),
            axis=1,
        )
        df_new = pd.DataFrame(
            results.tolist(),
            columns=["image", "label"],
        )
        df_new = df_new.explode(
            ["image", "label",],
            ignore_index=True,
        )
        return df_new


def main(
    user_id: str,
):
    client = DatalakeClient(
        user_id=user_id,
    )

    search_results = client.search(
        variants=[
            "base_layout",
        ]
    )
    print(
        search_results.groupby(
            [
                "provider",
                "dataset",
            ],
        ).size()
    )

    df = client.to_pandas(
        search_results,
        absolute_paths=True,
    )
    df = df.head(10)

    converter = BaseLayoutToText()
    for dataset_name, df_subset in df.groupby("dataset"):
        # ROOT = Path(__file__).resolve().parent
        ROOT = Path("/home/eric/workspace/datalake/")
        df_new = converter.convert(
            df=df,
        )

        _ = client.upload_task(
            df_new,
            provider=df_subset["provider"].unique()[0],
            dataset=dataset_name,
            task="document_conversion",
            variant="text",
            meta={
                "lang": df_subset["lang"].unique()[0],
                "src": df_subset["src"].unique()[0],
                "mod": "paragraph",
            },
            overwrite=True,
        )

        job_id = client.trigger_processing()
        client.wait_for_job_completion(
            job_id,
        )
        client.build_db(
            force_rebuild=True,
        )
    
    # idx = 10
    # image_bytes = df_new.iloc[idx]["image_bytes"]
    # print(df_new.iloc[idx]["label"])
    # Image.open(BytesIO(image_bytes)).save("/home/eric/workspace/sample.jpg")


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )