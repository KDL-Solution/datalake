# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
from export.kie_struct_exporter import KIEStructExporter
from export.base_layout_exporter import BaseLayoutExporter


if __name__ == "__main__":
    # datalake에서 실행하시오: e.g., `python -m export.main`.`
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
    #     indent=0,
    # )

    df = client.retrieve_with_existing_cols(
        # datasets=["real_kie"],
        variants=["base_kie"],
    )
    kie_exporter.export(
        df=df,
        datalake_dir="/mnt/AI_NAS/datalake",
        save_path="/home/eric/workspace/Qwen-SFT/base_kie.jsonl",
        indent=0,
    )

    # df = client.retrieve_with_existing_cols(
    #     datasets=["office_docs"],
    # )
    # layout_exporter.export(
    #     df=df,
    #     datalake_dir="/mnt/AI_NAS/datalake",
    #     save_path="/home/eric/workspace/Qwen-SFT/office_docs.jsonl",
    #     indent=0,
    # )
