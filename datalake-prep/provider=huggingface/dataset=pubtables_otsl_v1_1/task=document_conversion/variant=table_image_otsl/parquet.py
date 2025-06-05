if __name__ == "__main__":
    # datalake/datalake-prep에서 실행하시오: e.g., `python -m provider=huggingface.dataset=pubtables_otsl_v1_1.task=document_conversion.variant=table_image_otsl.parquet`.
    from datasets import load_dataset
    from pathlib import Path

    from utils import NAS_ROOT
    import sys
    sys.path.insert(
        0,
        (Path(__file__).resolve().parents[3] / "dataset=pubtabnet_otsl/task=document_conversion/variant=table_image_otsl").as_posix(),
    )
    from parquet import export_to_parquet

    script_dir = Path(__file__).resolve().parent
    data_dir = NAS_ROOT / f"source/{script_dir.parents[2].stem}/{script_dir.parents[1].stem}"

    train_dataset, val_dataset = load_dataset(
        "parquet",
        data_files={
            "train": (data_dir / "data/train-*.parquet").as_posix(),
            "val": (data_dir / "data/val-*.parquet").as_posix(),
        },
        split=[
            "train",
            "val",
        ],
    )

    script_dir = Path(__file__).resolve().parent
    images_dir = script_dir / "images"
    export_to_parquet(
        dataset=train_dataset,
        images_dir=images_dir,
        parquet_path=(script_dir / "train.parquet").as_posix(),
    )
    export_to_parquet(
        dataset=val_dataset,
        images_dir=images_dir,
        parquet_path=(script_dir / "val.parquet").as_posix(),
    )
