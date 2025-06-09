if __name__ == "__main__":
    # datalake/datalake-prep에서 실행하시오: e.g., `python -m provider=huggingface.dataset=fintabnet_otsl.task=document_conversion.variant=table_image_otsl.parquet`.
    from pathlib import Path

    import sys
    sys.path.insert(
        0,
        (Path(__file__).resolve().parents[3] / "dataset=pubtabnet_otsl/task=document_conversion/variant=table_image_otsl").as_posix(),
    )
    from parquet import main

    main()
