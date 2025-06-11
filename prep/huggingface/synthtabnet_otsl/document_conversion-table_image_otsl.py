if __name__ == "__main__":
    # datalake/datalake-prep에서 실행하시오: e.g., `python -m huggingface.synthtabnet_otsl.document_conversion-table_image_otsl`.
    from pathlib import Path

    from huggingface.table_image_otsl import main

    main(
        dataset="synthtabnet_otsl",
        save_dir=Path(__file__).resolve().parent.as_posix(),
    )
