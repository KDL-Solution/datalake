import random
import pandas as pd
import math
from PIL import Image
from io import BytesIO
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, current_process, cpu_count

from core.datalake import DatalakeClient
from prep.table_nester import TableNester
from prep.utils import pil_to_bytes
from prep.html_styler import HTMLStyler
from prep.html_renderer import HTMLRenderer


def generate_random_white_images(
    count: int = 1,
    min_area: int = 500 * 500,
    max_area: int = 100 * 100,
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
        # while True:
        aspect = rng.uniform(min_aspect, max_aspect)
        area = rng.uniform(min_area, max_area)
        height = math.sqrt(area / aspect)
        width = aspect * height
        width = max(1, int(width))
        height = max(1, int(height))
        # actual_aspect = width / height
        # actual_area = width * height
        # if min_aspect <= actual_aspect <= max_aspect and min_area <= actual_area <= max_area:
        images.append(
            Image.new(
                "RGB",
                (width, height),
                (255, 255, 255),
            )
        )
            # break
    return images


# def init_worker(_nester, _styler, _renderer):
#     global nester, styler, renderer
#     nester = _nester
#     styler = _styler
#     renderer = _renderer
def init_worker(
    htmls,
    images,
    # seed,
    start_seed,
):
    worker_idx = current_process()._identity[0] - 1
    seed = start_seed + worker_idx

    global nester, styler, renderer
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


# 3) 실제 워커 함수는 모듈 최상단에, 전역 변수에 의존
def _worker(_):
    out = nester.synthesize()
    styled = styler.style(out["html_for_rendering"])
    img_bytes = renderer.render(styled)
    return {
        "label": out["html_for_gt"],
        "image": img_bytes,
    }


def main(
    user_id: str,
    num_samples: int,
    num_workers: int = cpu_count(),
    start_seed: int = 42,
):
    client = DatalakeClient(
        user_id=user_id
    )
    client.build_db(
        force_rebuild=False,
    )
    search_results = client.search(
        variants=[
            "table_html",
            "table_image_html",
        ],
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    # processor = HTMLProcessor()

    df = client.to_pandas(
        search_results,
        absolute_paths=False,
    )
    df = df[(df["label"].notna())]
    htmls = df["label"].tolist()
    images = generate_random_white_images(
        count=num_workers // 10,
    )
    images = [pil_to_bytes(i) for i in images]

    # nester = TableNester(
    #     outer_htmls=htmls,
    #     inner_htmls=htmls,
    #     inner_images=images,
    #     mask=True,
    # )
    # styler = HTMLStyler()
    # renderer = HTMLRenderer()

    with Pool(
        processes=num_workers,
        initializer=init_worker,
        # initargs=(nester, styler, renderer),
        initargs=(htmls, images, start_seed),
    ) as pool:
        results = pool.map(_worker, range(num_samples))

    df = pd.DataFrame(results, columns=["label", "image"])
    print(len(df))
    dup = df[df.duplicated(subset=["label"], keep=False)]
    print(dup)
    # for _ in tqdm(range(num_samples)):
    #     out = nester.synthesize()
    #     html_new_style = styler.style(
    #         out["html_for_rendering"],
    #     )

    #     html_for_gt = out["html_for_gt"]
    #     image_bytes = renderer.render(
    #         html_new_style,
    #     )

    # _ = client.upload_task(
    #     df_subset_new,
    #     provider="inhouse",
    #     dataset=dataset_name,
    #     task="document_conversion",
    #     variant="table_image_otsl",
    #     meta={
    #         "lang": df_subset["lang"].unique()[0],
    #         "src": df_subset["src"].unique()[0],
    #         "mod": "table",
    #     },
    #     overwrite=True,
    # )

    # job_id = client.trigger_processing()
    # client.wait_for_job_completion(
    #     job_id,
    # )
    # client.build_db(
    #     force_rebuild=True,
    # )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
