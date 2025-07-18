import swifter
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, current_process
import os
import io

from PIL import Image

from datalake.core.client import DatalakeClient
from prep.utils import pil_to_bytes
from prep.html_utils import (
    HTMLProcessor,
    generate_random_white_images,
    HTMLNester,
    HTMLStyler,
    HTMLRenderer,
    HTMLDocTagsConverter,
)

_ = swifter

def init_worker(
    htmls, 
    images, 
    seed, 
    zoom,
    renderer_type
):
    worker_idx = current_process()._identity[0] - 1
    seed = seed + worker_idx

    global html_nester, styler, renderer
    html_nester = HTMLNester(
        outer_htmls=htmls,
        inner_htmls=htmls,
        inner_images=images,
        seed=seed
    )
    styler = HTMLStyler(seed=seed)
    renderer = HTMLRenderer(seed=seed, zoom=zoom, renderer_type=renderer_type)

def _worker(_):
    out = html_nester.synthesize()
    html_style = styler.style(out["html_for_rendering"])
    image_bytes, bboxes, rendered_width, rendered_height, inner_tables_html = renderer.render(html=html_style)

    elements = []
    if inner_tables_html and bboxes:
        for i, (html_val, bbox_val) in enumerate(zip(inner_tables_html, bboxes), start=1):
            elements.append({
                "idx": i,
                "type": "table",
                "value": html_val,
                "description": "",
                "bbox": bbox_val,
            })

    return {
        "page": 0,
        "reading_order": True,
        "elements": elements,
        "image": image_bytes  # filtering용, 최종 저장 전 drop
    }

def main(user_id: str, 
         num_samples: int, 
         num_proc: int = 64, 
         upload: bool = True, 
         seed: int = 42,
         use_table_image_otsl: bool = False, 
         zoom: float = 0.5, 
         renderer_type: str = 'playwright'):

    client = DatalakeClient(user_id=user_id)
    variants = ["table_image_otsl"] if use_table_image_otsl else ["table_html", "table_image_html"]

    try:
        search_results = client.search(variants=variants)
    except FileNotFoundError:
        client.build_db(force_rebuild=True)
        search_results = client.search(variants=variants)

    print(search_results.groupby(["provider", "dataset"]).size())

    
    if use_table_image_otsl:
        raise ValueError("use_table_image_otsl=True is not supported.")

    df = client.to_pandas(search_results, absolute_paths=False)
    if use_table_image_otsl:
        df = df[df["provider"] != "inhouse"]

    if use_table_image_otsl:
        df.rename(columns={"label": "label_doctags"}, inplace=True)
        converter = HTMLDocTagsConverter()
        df["html"] = df["label_doctags"].swifter.apply(converter.to_html)
    else:
        df.rename(columns={"label": "html"}, inplace=True)

    html_processor = HTMLProcessor()
    df["html"] = df["html"].swifter.apply(html_processor.extract_table)
    df["html"] = df["html"].swifter.apply(html_processor.remove_whitespaces)
    df = df[df["html"].notna()]
    df = df[~df["html"].str.contains("\\\\", regex=True)]
    htmls = df["html"].tolist()

    images = generate_random_white_images(count=num_samples // 10)
    images = [pil_to_bytes(i) for i in images]

    with Pool(processes=num_proc, initializer=init_worker, initargs=(htmls, images, seed, zoom, renderer_type)) as pool:
        results = list(tqdm(pool.imap(_worker, range(num_samples)), total=num_samples, desc="Synthesizing tables"))

    df_task = pd.DataFrame(results)
    df_task = df_task[df_task["image"].notna()].reset_index(drop=True)

    df_task = df_task.drop(columns=["image"])

    print(df_task)

    # if upload:
    #     langs = df["lang"].unique().tolist()
    #     lang = "multi" if len(langs) > 1 else langs[0]
    #     srcs = df["src"].unique().tolist()
    #     src = "multi" if len(srcs) > 1 else srcs[0]
    #     _ = client.upload_task(
    #         df_task,
    #         provider="inhouse",
    #         dataset=f"nested_table_seed_{seed}_{seed + num_proc - 1}_num_samples_{num_samples}",
    #         task="document_conversion",
    #         variant="table_image_otsl",
    #         meta={
    #             "lang": lang,
    #             "src": src,
    #             "mod": "table",
    #         },
    #         overwrite=True,
    #     )

    #     job_id = client.trigger_processing()
    #     _ = client.wait_for_job_completion(
    #         job_id,
    #         3600 * 4,
    #     )
    #     client.build_db(
    #         force_rebuild=True,
    #     )

if __name__ == "__main__":
    import fire
    fire.Fire(main)
