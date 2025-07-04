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


class BaseLayoutImageExtractor(object):
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
            return []


        # 2) Process each element, but isolate errors per-crop:
        image_bytes_ls: List[bytes] = []
        for el in elements:
            desc = el.get("description")
            bbox  = el.get("bbox")

            if not desc or "표" in desc or not bbox:
                continue

            try:
                # 2a) crop
                crop = image.crop(bbox)
                # 2b) encode to JPEG bytes
                buf = BytesIO()
                crop.save(buf, format="JPEG")
                image_bytes_ls.append(buf.getvalue())

            except Exception as e:
                # log and skip this element
                if verbose:
                    print(f"[ERROR] cropping/saving element {el!r} from {image_path!r}: {e}")
                continue
        return image_bytes_ls

    def extract(
        self,
        df: pd.DataFrame,
    ) -> List[bytes]:
        df_copy = df.copy()

        df_copy["label"] = df_copy.swifter.apply(
            lambda x: denormalize_bboxes(
                x["label"],
                width=x["width"],
                height=x["height"],
                bbox_key="bbox",
            ),
            axis=1,
        )  # Denormalize.
        df_copy["label"] = df_copy["label"].swifter.apply(
            lambda x: json.loads(x),
        )  # String to Dict.

        results = df_copy.swifter.apply(
            lambda x: self._crop(
                image_path=x["path"],
                elements=x["label"]["elements"],
            ),
            axis=1,
        )
        results_filterd = results[results.apply(
            lambda x: len(x) > 0,
        )]
        results_filterd_tolist = results_filterd.tolist()
        return sum(results_filterd_tolist, [])


def main(
    user_id: str,
):
    client = DatalakeClient(
        user_id=user_id,
    )

    client.build_db(
        force_rebuild=False,
    )
    search_results = client.search(
        variants=[
            "base_layout",
        ],
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
        search_results.head(500),
        # search_results,
        absolute_paths=True,
    )

    extractor = BaseLayoutImageExtractor()
    images = extractor.extract(
        df,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
