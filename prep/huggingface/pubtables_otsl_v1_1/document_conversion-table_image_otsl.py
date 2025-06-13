if __name__ == "__main__":
    # Run, e.g., `python -m prep.huggingface.pubtables_otsl_v1_1.document_conversion-table_image_otsl`.
    from pathlib import Path

    from prep.huggingface.pubtabnet_otsl.utils import main

    main(
        dataset="pubtables_otsl_v1_1",
        save_dir=Path(__file__).resolve().parent.as_posix(),
    )
