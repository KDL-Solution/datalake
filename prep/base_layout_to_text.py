# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
import swifter
import pandas as pd
from typing import List, Dict, Tuple
from PIL import Image
from tqdm import tqdm
from io import BytesIO

from core.datalake import DatalakeClient
from export.utils import denormalize_bboxes

swifter

tqdm.pandas()


class BaseLayoutToText(object):
    def _crop(
        self,
        image_path: str,
        elements: List[Dict[str, str]],
        verbose: bool = False,
    ) -> Tuple[List[bytes], List[str]]:
        # 1) Try opening the image
        try:
            image = Image.open(image_path).convert("RGB")
        except OSError as e:
            if verbose:
                print(f"[ERROR] Cannot open image {image_path!r}: {e}")
            return [], []

        image_bytes_ls: List[bytes] = []
        labels: List[str] = []

        # 2) Process each element, but isolate errors per-crop
        for el in elements:
            value = el.get("value")
            bbox  = el.get("bbox")

            if not value or not bbox:
                continue

            try:
                # 2a) crop
                crop = image.crop(bbox)
                # 2b) encode to JPEG bytes
                buf = BytesIO()
                crop.save(buf, format="JPEG")
                image_bytes_ls.append(buf.getvalue())
                labels.append(value)

            except Exception as e:
                # log and skip this element
                if verbose:
                    print(f"[ERROR] cropping/saving element {el!r} from {image_path!r}: {e}")
                continue
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
            columns=[
                "image",
                "label",
            ],
        )
        df_new = df_new.explode(
            [
                "image",
                "label",
            ],
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
        absolute_paths=False,
    )

    converter = BaseLayoutToText()
    for dataset_name, df_subset in df.groupby("dataset"):
        df_subset_new = converter.convert(
            df_subset,
        )

        _ = client.upload_task(
            df_subset_new,
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


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
