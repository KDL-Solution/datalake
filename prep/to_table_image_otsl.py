import swifter
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, current_process
from PIL import Image

import sys
sys.path.insert(0, "/home/eric/workspace/datalake/")
from datalake.core.client import DatalakeClient
from prep.utils import pil_to_bytes
from prep.html_utils import (
    HTMLProcessor,
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
):
    worker_idx = current_process()._identity[0] - 1
    seed = seed + worker_idx

    global html_nester, styler, renderer
    # seed = 42
    styler = HTMLStyler(
        seed=seed,
    )
    html_nester = HTMLNester(
        outer_htmls=htmls,
        inner_htmls=htmls,
        inner_images=images,
        seed=seed,
        # min_num_inner_images=2,
        # max_num_inner_images=2,
        # min_num_inner_images=0,
        # max_num_inner_images=0,
        # min_num_inner_tables=2,
        # max_num_inner_tables=2,
        # min_num_inner_tables=0,
        # max_num_inner_tables=0,
    )
    renderer = HTMLRenderer(
        seed=seed,
        min_pad = 20,
        max_pad = 50,
        min_margin = 50,
        max_margin = 80,
    )


def _worker(_):
    synthesize_out = html_nester.synthesize()

    html_style = styler.style(
        synthesize_out["label_html"],
    )

    render_out = renderer.render(
        html=html_style,
    )

    label = synthesize_out["label_doctags_masked"]
    for idx, bbox in enumerate(
        render_out["bboxes"],
        start=1,
    ):
        label = label.replace(
            f"[|IMAGE-{idx:02}|]",
            f"[|IMAGE-{idx:02}|]{bbox}"
        )
    return {
        "image": pil_to_bytes(render_out["image"]),
        "label": label,
    }


def main(
    user_id: str, 
    num_samples: int, 
    num_proc: int = 64,
    upload: bool = True, 
    seed: int = 42,
    use_table_image_otsl: bool = False, 
):
    # user_id="eric"
    # num_samples=32
    # num_proc: int = 4
    # use_table_image_otsl: bool = False
    # seed=42
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
    # search_results = search_results.head(1024)  #########

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

    # images = generate_random_white_images(
    #     count=num_samples // 10,
    #     min_area=(2 ** 8) ** 2,
    #     max_area=(2 ** 10) ** 2,
    # )
    # images = [pil_to_bytes(i) for i in images]
    base_layout_image = client.search(
        variants=[
            "base_layout",
        ],
        mods=[
            "image",
        ],
    )
    base_layout_image = client.to_pandas(
        base_layout_image,
        absolute_paths=True,
    )
    image_paths = base_layout_image["path"].tolist()
    image_bytes_ls = [pil_to_bytes(Image.open(i).convert("RGB")) for i in image_paths]

    with Pool(
        processes=num_proc,
        initializer=init_worker,
        initargs=(htmls, image_bytes_ls, seed),
    ) as pool:
        results = list(
            tqdm(
                pool.imap(_worker, range(num_samples)),
                total=num_samples,
                desc="Synthesizing tables",
            )
        )

    df_task = pd.DataFrame(results)
    df_task = df_task[df_task["image"].notna()].reset_index(
        drop=True,
    )

    if upload:
        langs = df["lang"].unique().tolist()
        lang = "multi" if len(langs) > 1 else langs[0]
        srcs = df["src"].unique().tolist()
        src = "multi" if len(srcs) > 1 else srcs[0]
        _ = client.upload_task(
            df_task,
            provider="inhouse",
            dataset=f"nested_table_seed_{seed}_{seed + num_proc - 1}_num_samples_{num_samples}",
            task="document_conversion",
            variant="table_image_otsl",
            meta={
                "lang": lang,
                "src": src,
                "mod": "table",
            },
            overwrite=True,
        )

        job_id = client.trigger_processing()
        _ = client.wait_for_job_completion(
            job_id,
            # 3600 * 4,
        )
        client.build_db(
            force_rebuild=True,
        )

if __name__ == "__main__":
    import fire
    fire.Fire(main)
