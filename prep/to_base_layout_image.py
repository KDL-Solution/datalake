# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
import swifter
import pandas as pd
from typing import List, Dict, Tuple
from PIL import Image
from tqdm import tqdm
from io import BytesIO
from copy import deepcopy

import sys
sys.path.insert(0, "/home/eric/workspace/datalake/")
# /home/eric/workspace/datalake/datalake/core/client.py
from datalake.core.client import DatalakeClient
from export.utils import denormalize_bboxes

_ = swifter

tqdm.pandas()


class BaseLayoutImageExtractor(object):
    def __init__(
        self,
        layout_cats: List[str] = [
            "figure",
            "picture",
        ],
        verbose: bool = False,
    ):
        self.layout_cats = layout_cats
        self.verbose = verbose

    def _crop(
        self,
        image_path: str,
        elements: List[Dict[str, str]],
    ) -> Tuple[List[bytes], List[str]]:
        # 1) Try opening the image
        try:
            image = Image.open(image_path).convert("RGB")
        except OSError as e:
            if self.verbose:
                print(f"[ERROR] Cannot open image {image_path!r}: {e}")
            return [], []

        # 2) Process each element, but isolate errors per-crop:
        image_bytes_ls = []
        new_elements = []
        for el in elements:
            type_ = el.get("type")
            desc = el.get("description")
            bbox  = el.get("bbox")
            if type_ not in self.layout_cats:
                continue

            # if not desc or "표" in desc or not bbox:
            if (
                not bbox
                or " 표이다" in desc
                or " 표이며 " in desc
            ):
                continue

            try:
                new_el = deepcopy(el)
                new_el["idx"] = 1
                new_el["bbox"] = [0, 0, 1., 1.]

                # 2a) crop
                crop = image.crop(bbox)
                # 2b) encode to JPEG bytes
                buf = BytesIO()
                crop.save(buf, format="JPEG")
                image_bytes_ls.append(buf.getvalue())
                new_elements.append(new_el)

            except Exception as e:
                if self.verbose:
                    print(f"[ERROR] cropping/saving element {el!r} from {image_path!r}: {e}")
                continue
        return image_bytes_ls, new_elements

    def extract(
        self,
        df: pd.DataFrame,
        indent: int = None,
    ) -> pd.DataFrame:
        df_copy = df.copy()

        df_copy["label"] = df_copy.swifter.apply(
            lambda x: denormalize_bboxes(
                x["label"],
                width=x["width"],
                height=x["height"],
                bbox_key="bbox",
            ),
            axis=1,
        )
        df_copy["label"] = df_copy["label"].swifter.apply(
            json.loads,
        )

        records = []
        for row in df_copy.itertuples(index=False):
            label = row.label
            elements = label.get("elements", [])
            image_bytes_ls, elements = self._crop(
                image_path=row.path,
                elements=elements,
            )

            for image_bytes, el in zip(
                image_bytes_ls,
                elements,
            ):
                label_copy = deepcopy(label)

                label_copy["reading_order"] = True
                label_copy["elements"] = [
                    el,
                ]

                records.append(
                    {
                        # **row._asdict() | {"label": None},  # remove parsed label
                        **row._asdict(),
                        # "element": element,
                        "image": image_bytes,
                        "label": label_copy,
                    }
                )

        new_df = pd.DataFrame(
            records,
        )
        new_df["label"] = new_df["label"].swifter.apply(
            lambda x: json.dumps(
                x,
                ensure_ascii=False,
                indent=indent,
            ),
        )
        new_df.drop(
            [
                "path",
            ],
            axis=1,
            inplace=True,
        )
        return new_df


def main(
    user_id: str,
    upload: bool = True,
):
    # user_id = "eric"
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

    search_results = search_results[~(search_results["dataset"].str.contains("doclaynet"))]
    df_base_layout = client.to_pandas(
        # search_results.head(2048),
        search_results,
        # df_base_layout,
        absolute_paths=True,
    )

    extractor = BaseLayoutImageExtractor()
    df_task = extractor.extract(
        df_base_layout,
    )
    # len(df_task)

    # temp = df_task[(df_task["label"].str.contains("표"))]
    # len(temp)
    # temp["label_str"] = temp["label"].swifter.apply(
    #     json.loads,
    # )
    # temp["temp"] = temp["label_str"].apply(
    #     lambda x: x["elements"][0]["description"],
    # )
    # for i in temp["temp"].tolist():
    #     print(i)
    # set([i.split(" ")[-1] for i in temp["temp"].tolist()])
    # for i in set([i for i in temp["temp"].tolist() if i.split(" ")[-1] == "표기)"]):
    #     print(i)

    # temp[(temp["label"].str.contains("표이며"))]

    # for idx in range(0, 5):
    #     print(df_task.iloc[idx]["label"])
    # 1

    if upload:
        for (provider, dataset), df_groupby in df_task.groupby(
            [
                "provider",
                "dataset",
            ],
        ):
            _ = client.upload_task(
                df_groupby,
                provider=provider,
                dataset=dataset,
                task="layout",
                variant="base_layout",
                meta={
                    "lang": df_groupby["lang"].unique().tolist()[0],
                    "src": df_groupby["src"].unique().tolist()[0],
                    "mod": "image",
                },
                overwrite=True,
            )

    job_id = client.trigger_processing()
    _ = client.wait_for_job_completion(
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
