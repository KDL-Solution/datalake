if __name__ == "__main__":
    # datalake/prep에서 실행하시오: e.g., `python -m aihub.diverse_ocr.ocr-recognition_char`.
    import fire
    from pathlib import Path
    from functools import partial

    from aihub.diverse_ocr.utils import main

    script_dir = Path(__file__).resolve().parent
    fire.Fire(
        partial(
            main,
            ocr_unit="char",
            data_dir=(script_dir / "data").as_posix(),
            parquet_path=(script_dir / "char.parquet").as_posix(),
            images_dir=(script_dir / "images").as_posix(),
        )
    )
