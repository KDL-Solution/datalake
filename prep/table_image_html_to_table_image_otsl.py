# import sys
# sys.path.insert(0, '/home/eric/workspace/datalake/')
from prep.utils import HTMLToOTSL
from core.datalake import DatalakeClient


def main(
    batch_size: int = 8,
    num_proc: int = 32,
    mod: str = "table",
) -> None:
    manager = DatalakeClient()
    converter = HTMLToOTSL()

    search_results = manager.search_catalog(
        variants=[
            "table_image_html",
        ],
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    dataset = manager.to_dataset(
        search_results,
        absolute_paths=False,
    )
    # dataset = dataset.shuffle()
    # dataset = dataset.select(range(64))

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
        _, _ = manager.upload_task(
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

    job_id = manager.trigger_nas_processing()
    manager.wait_for_job_completion(
        job_id,
    )
    manager.build_catalog_db(
        force_rebuild=True,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main,
    )
