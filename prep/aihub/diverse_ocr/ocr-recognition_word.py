if __name__ == "__main__":
    # Run, e.g., `python -m prep.aihub.diverse_ocr.ocr-recognition_word`.
    import fire
    from pathlib import Path
    from functools import partial

    from prep.aihub.diverse_ocr.utils import main

    script_dir = Path(__file__).resolve().parent
    fire.Fire(
        partial(
            main,
            ocr_unit="word",
            data_dir=(script_dir / "data").as_posix(),
            parquet_path=(script_dir / "word.parquet").as_posix(),
            images_dir=(script_dir / "images_word").as_posix(),
        )
    )
