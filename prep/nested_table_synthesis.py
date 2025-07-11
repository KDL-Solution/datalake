import swifter
import random
import pandas as pd
import math
from PIL import Image
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, current_process

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


def generate_random_white_images(
    count: int = 1,
    min_area: int = (2 ** 8) ** 2,
    max_area: int = (2 ** 6) ** 2,
    min_aspect: float = 0.5,
    max_aspect: float = 2.,
    seed: int = 42,
):
    """
    주어진 종횡비(aspect ratio) 범위와 면적(area) 범위, 시드, 개수를 받아
    흰색(RGB: 255,255,255) 픽셀로만 이루어진 PIL 이미지를 리스트로 생성합니다.
    """
    rng = random.Random(seed)

    images = []
    for _ in range(count):
        aspect = rng.uniform(min_aspect, max_aspect)
        area = rng.uniform(min_area, max_area)
        height = math.sqrt(area / aspect)
        width = aspect * height
        width = max(1, int(width))
        height = max(1, int(height))
        images.append(
            Image.new(
                "RGB",
                (width, height),
                (255, 255, 255),
            )
        )
    return images


def init_worker(
    htmls,
    images,
    seed,
):
    worker_idx = current_process()._identity[0] - 1
    seed = seed + worker_idx

    global html_nester, styler, renderer
    html_nester = HTMLNester(
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


# 3) 실제 워커 함수는 모듈 최상단에, 전역 변수에 의존
def _worker(_):
    out = html_nester.synthesize()
    html_style = styler.style(
        out["label_html"],
    )
    image_bytes = renderer.render(
        html=html_style,
    )
    doctags = out["label_doctags"]
    return {
        "image": image_bytes,
        "label": doctags,
    }


def main(
    user_id: str,
    num_samples: int,
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

    df = client.to_pandas(
        search_results,
        absolute_paths=False,
    )
    if use_table_image_otsl:
        df = df[(df["provider"] != "inhouse")]  # 합성 데이터 제외.

    if use_table_image_otsl:
        # DocTags -> HTML:
        df.rename(
            columns={
                "label": "label_doctags",
            },
            inplace=True,
        )
        converter = HTMLDocTagsConverter()
        df["html"] = df["label_doctags"].swifter.apply(
            converter.to_html,
        )
    else:
        df.rename(
            columns={
                "label": "html",
            },
            inplace=True,
        )

    html_processor = HTMLProcessor()
    df["html"] = df["html"].swifter.apply(
        html_processor.extract_table,
    )
    df["html"] = df["html"].swifter.apply(
        html_processor.remove_whitespaces,
    )
    df = df[(df["html"].notna())]
    df = df[(~df["html"].str.contains("\\\\", regex=True))]
    htmls = df["html"].tolist()

    images = generate_random_white_images(
        count=num_samples // 10,
    )
    images = [pil_to_bytes(i) for i in images]

    with Pool(
        processes=num_proc,
        initializer=init_worker,
        initargs=(
            htmls,
            images,
            seed,
        ),
    ) as pool:
        results = []
        for result in tqdm(
            pool.imap(
                _worker,
                range(num_samples),
            ),
            total=num_samples,
            desc="Synthesizing tables",
        ):
            results.append(result)
    df_task = pd.DataFrame(
        results,
        columns=[
            "image",
            "label",
        ],
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
        )
        client.build_db(
            force_rebuild=True,
        )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
