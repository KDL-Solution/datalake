if __name__ == "__main__":
    # Run, e.g., `python -m prep.huggingface.synthtabnet_otsl.document_conversion-table_image_otsl`.
    from pathlib import Path

    from prep.huggingface.pubtabnet_otsl.utils import main

    main(
        dataset="synthtabnet_otsl",
        save_dir=Path(__file__).resolve().parent.as_posix(),
    )
