# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
from pathlib import Path
import pandas as pd
from typing import List, Dict
from PIL import Image
from tqdm import tqdm

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


class DolphinStage2Exporter(object):
    def crop(
        self,
        image_path: str,
        elements: List[Dict[str, str]],
        images_dir: str,
        layout_category_dict: Dict[str, str],
        user_prompt_dict: Dict[str, str],
    ):
        try:
            image = Image.open(image_path).convert("RGB")
        except OSError:
            print(f"[ERROR] Cannot open image: {image_path}")
            return [], []
        else:
            new_image_paths = []
            labels = []
            queries = []
            for el in elements:
                if not el["value"]:
                    continue

                cropped = image.crop(el["bbox"])
                image_hash = get_safe_image_hash_from_pil(
                    cropped,
                )
                new_image_path = Path(images_dir) / image_hash[: 2] / f"{image_hash}.jpg"
                if not new_image_path.exists():
                    new_image_path.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )
                    cropped.save(
                        new_image_path.as_posix(),
                        format="JPEG",
                    )
                new_image_paths.append(new_image_path.as_posix())

                category = layout_category_dict[el["type"]]
                queries.append(user_prompt_dict[category])

                labels.append(el["value"])
            return new_image_paths, queries, labels

    def export(
        self,
        df: pd.DataFrame,
        jsonl_path: str,
        images_dir: str,
        layout_category_dict: Dict[str, str] = layout_category_dict,
        user_prompt_dict: Dict[str, str] = user_prompt_dict,
    ) -> None:
        df_copied = df.copy()

        df_copied["label"] = df_copied.apply(
            lambda x: denormalize_bboxes(
                x["label"],
                width=x["width"],
                height=x["height"],
                bbox_key="bbox",
            ),
            axis=1,
        )  # Denormalize.
        df_copied["label"] = df_copied["label"].apply(
            lambda x: json.loads(x),
        )  # String to Dict.

        df_copied[["image_path", "query", "label"]] = df_copied.progress_apply(
            lambda x: self.crop(
                image_path=x["image_path"],
                elements=x["label"]["elements"],
                images_dir=images_dir,
                layout_category_dict=layout_category_dict,
                user_prompt_dict=user_prompt_dict,
            ),
            axis=1,
            result_type="expand",
        )
        save_df_as_jsonl(
            df=df_copied.explode(
                [
                    "image_path",
                    "label",
                ]
            ),
            jsonl_path=jsonl_path,
        )


def main(
    user_id: str,
):
    client = DatalakeClient(
        user_id=user_id,
    )

    search_results = client.search(
        datasets=[
            "diverse_ocr_char",
            "diverse_ocr_word",
        ]
    )
    exporter = RecogntionCharExporter()

    search_results = client.search(
        variants=[
            "base_layout",
        ]
    )
    search_results.iloc[0]


    search_results
    search_results.iloc[-1]



    exporter = DolphinStage2Exporter()
    ROOT = Path(__file__).resolve().parent
    exporter.export(
        df=df,
        jsonl_path=(ROOT / "data/office_docs_stage2.jsonl").as_posix(),
        images_dir=(ROOT / "data/cropped").as_posix(),
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )