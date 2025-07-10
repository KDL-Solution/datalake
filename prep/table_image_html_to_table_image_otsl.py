from datalake.core.client import DatalakeClient
from prep.html_utils import HTMLDocTagsConverter


def _to_doctags_batch(
    batch,
):
    converter = HTMLDocTagsConverter()
    return {
        "label": [converter.to_doctags(
            i,
        ) for i in batch["label_html"]]
    }


def main(
    user_id: str,
    batch_size: int = 256,
    num_proc: int = 32,
    mod: str = "table",
    upload: bool = True,
) -> None:

    client = DatalakeClient(
        user_id=user_id,
    )
    try:
        search_results = client.search(
            variants=[
                "table_image_html",
            ],
        )
    except FileNotFoundError:
        client.build_db(
            force_rebuild=True,
        )
        search_results = client.search(
            variants=[
                "table_image_html",
            ],
        )
    print(search_results.groupby(["provider", "dataset"]).size())
    # table_image_text_pair
    # finance_legal_mrc_merged_table
    # ko_document_table_visual_sft

    dataset = client.to_dataset(
        search_results,
        absolute_paths=True,
    )

    dataset = dataset.rename_column(
        "label",
        "label_html",
    )

    dataset = dataset.map(
        _to_doctags_batch,
        desc="Converting to DocTags...",
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    # 잘못된 label 제거:
    dataset = dataset.map(
        lambda x: {
            "label": None if not x["label"].strip() else x["label"],
        }
    )
    dataset = dataset.filter(
        lambda x: x["label"] is not None,
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
