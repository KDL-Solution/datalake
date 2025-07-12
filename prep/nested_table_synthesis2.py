import random
import math
from typing import Dict, Any
from PIL import Image
from datasets import Dataset
from tqdm import tqdm

from datalake.core.client import DatalakeClient
from prep.utils import pil_to_bytes
from datalake.prep.html_utils.html_processor import (
    HTMLProcessor,
    TableNester,
    HTMLStyler,
    HTMLRenderer,
    HTMLDocTagsConverter,
)

def generate_random_white_images(
    count: int = 1,
    min_area: int = 500 * 500,
    max_area: int = 100 * 100,
    min_aspect: float = 0.5,
    max_aspect: float = 2.0,
    seed: int = 42,
):
    rng = random.Random(seed)
    images = []
    for _ in range(count):
        aspect = rng.uniform(min_aspect, max_aspect)
        area = rng.uniform(min_area, max_area)
        h = math.sqrt(area / aspect)
        w = aspect * h
        images.append(
            Image.new("RGB", (max(1, int(w)), max(1, int(h))), (255, 255, 255))
        )
    return images


def _to_html_batch(
    batch,
) -> Dict[str, Any]:
    converter = HTMLDocTagsConverter()
    return {
        "html": [
            converter.to_html(
                i,
            ) for i in batch["label_doctags"]
        ],
    }


def _process_batch(
    batch,
) -> Dict[str, Any]:
    processor = HTMLProcessor()
    return {
        "html": [
            processor.remove_whitespaces(
                processor.extract_table(
                    i,
                ),
            ) for i in batch["label_doctags"]
        ],
    }


def _render_batch(
    batch,
    htmls,
    images,
    seed,
) -> Dict[str, Any]:
    nester = TableNester(
        outer_htmls=htmls,
        inner_htmls=htmls,
        inner_images=images,
        seed=seed,
    )
    styler = HTMLStyler(
        seed=seed,
    )
    renderer = HTMLRenderer(
        seed=seed,
    )
    return {
        "image": [
            processor.remove_whitespaces(
                processor.extract_table(
                    i,
                ),
            ) for i in batch["label_doctags"]
        ],
    }


def main(
    user_id: str,
    num_samples: int,
    batch_size: int = 64,
    num_proc: int = 64,
    upload: bool = True,
    seed: int = 42,
    use_table_image_otsl: bool = False,
):
    client = DatalakeClient(
        user_id=user_id,
    )
    if use_table_image_otsl:
        variants = [
            "table_image_otsl",
        ]
    else:
        variants = [
            "table_html",
            "table_image_html",
        ]
    try:
        search_results = client.search(
            variants=variants,
        )
    except FileNotFoundError:
        client.build_db(
            force_rebuild=True,
        )
        search_results = client.search(
            variants=variants,
        )
    print(search_results.groupby(["provider", "dataset"]).size())

    dataset = client.to_dataset(
        search_results,
        absolute_paths=False,
    )

    dataset = dataset.map(
        _to_html_batch,
        desc="Converting to DocTags...",
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    dataset = dataset.map(
        _process_batch,
        desc="Processing HTMLs...",
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    dataset = dataset.filter(
        lambda x: x["html"] is not None,
    )

    htmls = dataset["html"]
    images = generate_random_white_images(count=num_samples // 10)
    images = [pil_to_bytes(img) for img in images]

    # 6) 멀티프로세싱으로 합성
    with Pool(
        processes=num_proc,
        initializer=init_worker,
        initargs=(htmls, images, seed),
    ) as pool:
        results = [
            r for r in tqdm(pool.imap(_worker, range(num_samples)),
                           total=num_samples, desc="Synthesizing tables")
        ]

    # 7) 결과를 다시 Dataset 또는 DataFrame으로
    out_ds = Dataset.from_list(results)

    if upload:
        # HF Dataset → pandas DataFrame
        df_task = out_ds.to_pandas()
        langs = dataset.unique("lang")
        srcs = dataset.unique("src")
        lang = "multi" if len(langs) > 1 else langs[0]
        src = "multi" if len(srcs) > 1 else srcs[0]

        client.upload_task(
            df_task,
            provider="inhouse",
            dataset=f"nested_table_seed_{seed}_{seed + num_proc - 1}_ns_{num_samples}",
            task="document_conversion",
            variant="table_image_otsl",
            meta={"lang": lang, "src": src, "mod": "table"},
            overwrite=True,
        )
        job_id = client.trigger_processing()
        client.wait_for_job_completion(job_id)
        client.build_db(force_rebuild=True)

if __name__ == "__main__":
    import fire
    fire.Fire(main)
