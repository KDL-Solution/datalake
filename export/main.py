# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
from export.base_kie_exporter import KIEStructExporter
from export.base_layout_exporter import BaseLayoutExporter


if __name__ == "__main__":
    # Run, e.g., `python -m export.main`.`
    from athena.src.core.athena_client import AthenaClient

    client = AthenaClient()

    kie_exporter = KIEStructExporter()
    layout_exporter = BaseLayoutExporter()

    # df = client.retrieve_with_existing_cols(
    #     tasks=["vqa"],
    #     variants=["kie_struct"],
    #     datasets=["funsd_plus"],
    # )
    # kie_exporter.export(
    #     df=df,
    #     datalake_dir="/mnt/AI_NAS/datalake",
    #     save_path="/home/eric/workspace/Qwen-SFT/funsd_plus.jsonl",
    # )

    # df = client.retrieve_with_existing_cols(
    #     # datasets=["real_kie"],
    #     variants=["base_kie"],
    # )
    # kie_exporter.export(
    #     df=df,
    #     datalake_dir="/mnt/AI_NAS/datalake",
    #     save_path="/home/eric/workspace/Qwen-SFT/base_kie.jsonl",
    # )

    df = client.retrieve_with_existing_cols(
        datasets=[
            "doclaynet_train",
            "doclaynet_val",
            "doclaynet_test",
        ]
    )
    layout_exporter.export(
        df=df,
        datalake_dir="/mnt/AI_NAS/datalake",
        save_path="/home/eric/workspace/Qwen-SFT/doclaynet.jsonl",
    )
