from typing import Dict, Any
from functools import partial

# import sys
# sys.path.insert(0, '/home/eric/workspace/datalake/')
from datalake.core.client import DatalakeClient
from prep.html_utils import (
    HTMLDocTagsConverter,
    HTMLStyler,
    HTMLRenderer,
)


def _style_batch(
    batch,
    seed,
) -> Dict[str, Any]:
    html_styler = HTMLStyler(
        seed=seed,
    )
    return {
        "label_html_style": [
            html_styler.style(
                i,
            ) for i in batch["label_html"]
        ]
    }


def _to_doctags_batch(
    batch,
) -> Dict[str, Any]:
    converter = HTMLDocTagsConverter()
    return {
        "label": [
            converter.to_doctags(
                i,
            ) for i in batch["label_html"]
        ],
    }


def _render_batch(
    batch,
    seed,
) -> Dict[str, Any]:
    renderer = HTMLRenderer(
        seed=seed,
    )
    return {
        "image": [
            renderer.render(
                html=i,
                pad_html=j,
            ) for i, j in zip(
                batch["label_html_style"],
                batch["label_html"]
            )
        ],
    }


def main(
    user_id: str,
    batch_size: int = 64,
    num_proc: int = 64,
    mod: str = "table",
    upload: bool = True,
    seed: int = 42,
) -> None:
    client = DatalakeClient(
        user_id=user_id,
    )
    try:
        search_results = client.search(
            variants=[
                "table_html",
            ],
        )
    except FileNotFoundError:
        client.build_db(
            force_rebuild=True,
        )
        search_results = client.search(
            variants=[
                "table_html",
            ],
        )
    print(search_results.groupby(["provider", "dataset"]).size())
    # admindocs_mrc
    # table_qa
    # tech_sci_mrc

    dataset = client.to_dataset(
        search_results,
        absolute_paths=False,
    )

    dataset = dataset.rename_column(
        "label",
        "label_html",
    )

    dataset = dataset.filter(
        lambda example: "\\" not in example["label_html"],
        desc="Filtering out math formula...",
    )
    dataset = dataset.select(range(num_proc))

    dataset = dataset.map(
        partial(
            _style_batch,
            seed=seed,
        ),
        desc="Adding style...",
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    dataset = dataset.map(
        _to_doctags_batch,
        desc="Converting to DocTags...",
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    dataset = dataset.filter(
        lambda x: x["label"].strip() is not None,
    )

    dataset = dataset.map(
        partial(
            _render_batch,
            seed=seed,
        ),
        desc="Rendering HTML...",
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    dataset = dataset.filter(
        lambda x: x["image"] is not None,
    )

    dataset = dataset.select_columns(
        [
            "provider",
            "dataset",
            "lang",
            "src",
            "path",
            "label",
        ]
    )

    if upload:
        for dataset_ in dataset.unique("dataset"):
            dataset_filter = dataset.filter(
                lambda x: x["dataset"] == dataset_,
            )
            _ = client.upload_task(
                dataset_filter,
                provider=dataset_filter.unique("provider")[0],
                dataset=dataset_,
                task="document_conversion",
                variant="table_image_otsl",
                meta={
                    "lang": dataset_filter.unique("lang")[0],
                    "src": dataset_filter.unique("src")[0],
                    "mod": mod,
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
        main,
    )
