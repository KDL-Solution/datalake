import regex
from datasets import Dataset

# import sys
# sys.path.insert(0, "/home/eric/workspace/datalake/")
from core.datalake import DatalakeClient
from export.utils import user_prompt_dict


class TableImageOTSLExporter:
    def __init__(
        self,
        batch_size: int = 64,
        num_procs: int = 16,
    ):
        self.batch_size = batch_size
        self.num_procs = num_procs
        self.otsl_pattern = regex.compile(
            r"<otsl>.*?</otsl>",
            regex.DOTALL,
        )
        self.keep_columns = [
            "path",
            "query",
            "label",
        ]

    def extract_otsl(
        self,
        text: str,
    ) -> str:
        if not isinstance(text, str):
            return None
        # Find the content inside <otsl>...</otsl>:
        match = regex.search(
            self.otsl_pattern,
            text,
        )
        if match:
            return match.group(0).strip()
        else:
            return None

    def export(
        self,
        dataset: Dataset,
        user_prompt: str = user_prompt_dict["table"],
    ) -> None:
        dataset = dataset.map(
            lambda x: {
                "label": [
                    self.extract_otsl(
                        i,
                    ) for i in x["label"]
                ],
            },
            batched=True,
            batch_size=self.batch_size,
            num_proc=self.num_procs,
            desc="Extracting <otsl>...</otsl>",
        )
        # label이 `None`인 샘플 제거:
        print(f"# original samples: {len(dataset):,}")
        dataset = dataset.filter(
            lambda x: x["label"] is not None,
            desc="Removing empty labels",
        )
        print(f"# samples after removing empty labels: {len(dataset):,}")

        dataset = dataset.map(
            lambda x: {
                "length": [
                    len(i) for i in x["label"]
                ]
            },
            batched=True,
            batch_size=self.batch_size,
            num_proc=self.num_procs,
            desc="Getting label lengths",
        )
        self.keep_columns.append("length")

        # 수식 등 제거:
        dataset = dataset.filter(
            lambda x: "\\" not in x["label"],
            desc="Filtering LaTeX-style labels",
        )

        # user prompt 추가:
        dataset = dataset.map(
            lambda x: {
                "query": [user_prompt] * len(x["label"]),
            },
            batched=True,
            batch_size=self.batch_size,
            num_proc=self.num_procs,
            desc="Adding user prompt",
        )

        dataset = dataset.remove_columns(
            [col for col in dataset.column_names if col not in self.keep_columns]
        )
        return dataset


def main(
    user_id: str,
    # dataset: str,
    batch_size: int = 1_024,
    num_procs: int = 16,
) -> None:
    exporter = TableImageOTSLExporter(
        batch_size=batch_size,
        num_procs=num_procs,
    )
    client = DatalakeClient(
        user_id=user_id,
    )

    search_results = client.search(
        variants=[
            "table_image_otsl",
        ],
        output_format="dataset",
    )
    # search_results = search_results.select(range(2_000))  # TEMP
    print(
        search_results.to_pandas().groupby(
            [
                "provider",
                "dataset",
            ],
        ).size()
    )

    dataset = client._prepare_dataset(
        search_results,
        absolute_paths=True,
    )
    dataset = exporter.export(
        dataset,
    )

    # _, _ = client.upload_task_data(
    #     data_file=dataset,
    #     provider="inhouse",
    #     dataset=dataset,
    #     task="document_conversion",
    #     variant="table_image_otsl",
    #     meta={
    #         "lang": "unk",
    #         "src": "unk",
    #         "mod": "table",
    #     },
    #     overwrite=True,
    # )

    # job_id = client.trigger_nas_processing()
    # client.wait_for_job_completion(
    #     job_id,
    # )
    # client.build_catalog_db(
    #     force_rebuild=True,
    # )
    dataset.to_json(
        "/home/eric/workspace/datalake/export/data/table_image_otsl.jsonl",
        lines=True,
        force_ascii=False,
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
