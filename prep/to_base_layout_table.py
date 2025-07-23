import os
import io
import swifter
import pandas as pd
from typing import List
from bs4 import BeautifulSoup
from tqdm import tqdm
from multiprocessing import Pool, current_process
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
    engine
):
    worker_idx = current_process()._identity[0] - 1
    seed = seed + worker_idx

    global html_nester, styler, renderer
    # seed = 42
    html_nester = HTMLNester(
        outer_htmls=htmls,
        inner_htmls=htmls,
        inner_images=images,
        seed=seed,
        min_num_inner_images=2,
        max_num_inner_images=2,
        # min_num_inner_images=0,
        # max_num_inner_images=0,
        min_num_inner_tables=2,
        max_num_inner_tables=2,
    )
    styler = HTMLStyler(
        seed=seed,
    )
    renderer = HTMLRenderer(
        seed=seed,
        # max_margin=100,
        # zoom=zoom,
        # engine=engine,
    )


def extract_tables_and_images(
    html: str,
) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    result = []

    def dfs(tag):
        for child in tag.children:
            if isinstance(child, str):
                continue

            if child.name == "table":
                result.append(str(child))  # 전체 <table> HTML 문자열
                dfs(child)  # 내부 순회
            elif child.name == "img":
                result.append(str(child))
                # src = child.get("src")
                # if src:
                #     result.append(src)
            else:
                dfs(child)

    dfs(soup)
    return result


def _worker(_):
    out = html_nester.synthesize(
        mask_images=False,
    )
    # [i.size for i in images]
    html_style = styler.style(
        out["html_for_rendering"],
    )
    render_out = renderer.render(
        html=html_style,
    )

    vals = extract_tables_and_images(
        out["label_html"]
    )
    elements = [
        {
            "idx": idx,
            "type": "table" if val.startswith("<table>") else "image",
            "value": val,
            "bbox": norm_bbox,
            "description": "",
        } for idx, (val, norm_bbox) in enumerate(zip(
            vals,
            render_out["normalized_bboxes"],
        ))
    ]
    label = {
        "page": 0,
        "reading_order": True,
        "elements": elements,
    }
    return {
        "image": pil_to_bytes(
            render_out["image"],
        ),
        "label": label,
    }
    # image = bytes_to_pil(image_bytes)
    

    # len(bboxes)
    # bboxes
    # inner_tables_html[0]
    # inner_tables_html[1]
    # image.size
    # image.save("/home/eric/workspace/sample.jpg")
    # rendered_width, rendered_height
    # inner_tables_html[1]
    # bboxes

    # elements = []
    # if inner_tables_html and bboxes:
    #     for i, (html_val, bbox_val) in enumerate(zip(inner_tables_html, bboxes), start=1):
    #         elements.append({
    #             "idx": i,
    #             "type": "table",
    #             "value": html_val,
    #             "description": "",
    #             "bbox": bbox_val,
    #         })

    # return {
    #     "page": 0,
    #     "reading_order": True,
    #     "elements": elements,
    #     "image": image_bytes  # filtering용, 최종 저장 전 drop
    # }


def main(
    user_id: str, 
    num_samples: int, 
    num_proc: int = 64,
    upload: bool = True, 
    seed: int = 42,
    use_table_image_otsl: bool = False, 
    zoom: float = 0.5, 
    engine: str = "playwright",
):
    # user_id="eric"
    # num_samples=32
    # num_proc: int = 4
    # use_table_image_otsl: bool = False
    if use_table_image_otsl:
        raise ValueError("use_table_image_otsl=True is not supported.")

    client = DatalakeClient(
        user_id=user_id,
    )
    variants = ["table_image_otsl"] if use_table_image_otsl else ["table_html", "table_image_html"]

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

    df = client.to_pandas(
        search_results,
        absolute_paths=False,
    )
    if use_table_image_otsl:
        df = df[df["provider"] != "inhouse"]

    if use_table_image_otsl:
        df.rename(columns={"label": "label_doctags"}, inplace=True)
        converter = HTMLDocTagsConverter()
        df["html"] = df["label_doctags"].swifter.apply(converter.to_html)
    else:
        df.rename(columns={"label": "html"}, inplace=True)

    df = df.head(1024)  #########

    html_processor = HTMLProcessor()
    df["html"] = df["html"].swifter.apply(
        html_processor.extract_table,
    )
    df["html"] = df["html"].swifter.apply(
        html_processor.remove_whitespaces,
    )
    df = df[df["html"].notna()]
    df = df[~df["html"].str.contains("\\\\", regex=True)]
    htmls = df["html"].tolist()

    images = generate_random_white_images(
        count=num_samples // 10,
        min_area=(2 ** 8) ** 2,
        max_area=(2 ** 10) ** 2,
    )
    images = [pil_to_bytes(i) for i in images]

    with Pool(
        processes=num_proc,
        initializer=init_worker,
        initargs=(htmls, images, seed, zoom, engine),
    ) as pool:
        results = list(
            tqdm(
                pool.imap(_worker, range(num_samples)),
                total=num_samples,
                desc="Synthesizing tables",
            )
        )

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
