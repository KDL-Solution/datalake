# import sys
# sys.path.insert(0, '/home/eric/workspace/datalake/')
from export.utils import html_to_doctags
from managers.datalake_client import DatalakeClient


def main(
    batch_size: int = 32,
    mod: str = "table",
) -> None:
    manager = DatalakeClient()

    search_results = manager.search_catalog(
        variants=[
            "table_image_html",
        ]
    )
    print(search_results.groupby(["provider", "dataset"]).size())

    dataset = manager.to_dataset(
        search_results,
        absolute_paths=False,
    )
    dataset = dataset.map(
        lambda batch: {
            "label": [
                html_to_doctags(
                    i,
                ) for i in batch["label"]
            ],
        },
        batched=True,
        batch_size=batch_size,
    )

    df = dataset.to_pandas()
    for dataset_name, df_group in df.groupby("dataset"):
        _, _ = manager.upload_task_data(
            data_file=df_group ,
            provider=df_group["provider"].unique()[0],
            dataset=dataset_name,
            task="document_conversion",
            variant="table_image_otsl",
            meta={
                "lang": df_group["lang"].unique()[0],
                "src": df_group["src"].unique()[0],
                "mod": "table",
            },
            overwrite=True,
        )

    manager.trigger_nas_processing()
    manager.build_catalog_db(
        force_rebuild=True,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main,
    )
