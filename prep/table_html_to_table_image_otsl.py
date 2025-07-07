# import sys
# sys.path.insert(0, '/home/eric/workspace/datalake/')
from core.datalake import DatalakeClient
from prep.utils import HTMLToOTSL


def main(
    batch_size: int = 16,
    num_proc: int = 64,
    mod: str = "table",
) -> None:
    client = DatalakeClient()

    search_results = client.search(
        variants=[
            "table_html",
        ],
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    dataset = client.to_dataset(
        search_results,
        absolute_paths=False,
    )
    # dataset = dataset.filter(
    #     lambda x: x["dataset"] == "tech_sci_mrc",
    # )  # TEMP!
    # 수식이 포함된 표 제거:
    dataset = dataset.filter(
        lambda example: "\\" not in example["label"]
    )
    # dataset = dataset.shuffle()  # TEMP!
    # dataset = dataset.select(range(16))  # TEMP!

    html_styler = HTMLStyler()
    dataset = dataset.map(
        lambda x: {
            "html": [
                html_styler.style(
                    i,
                ) for i in x["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )
    dataset = dataset.map(
        lambda x: {
            "image": [
                render(
                    i,
                ) for i in x["html"]
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )

    converter = HTMLToOTSL()
    dataset = dataset.map(
        lambda x: {
            "label": [
                converter.convert(
                    i,
                ) for i in x["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
        num_proc=num_proc,
    )

    for dataset_name in dataset.unique("dataset"):
        dataset_filter = dataset.filter(
            lambda x: x["dataset"] == dataset_name,
        )
        _, _ = client.upload_task(
            data_file=dataset_filter,
            provider=dataset_filter.unique("provider")[0],
            dataset=dataset_name,
            task="document_conversion",
            variant="table_image_otsl",
            meta={
                "lang": dataset_filter.unique("lang")[0],
                "src": dataset_filter.unique("src")[0],
                "mod": mod,
            },
            overwrite=True,
        )

    job_id = client.trigger_nas_processing()
    client.wait_for_job_completion(
        job_id,
    )
    client.build_catalog_db(
        force_rebuild=True,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main,
    )
