if __name__ == "__main__":
    import fire
    from pathlib import Path
    from functools import partial

    from aihub.diverse_ocr.utils import main

    script_dir = Path(__file__).resolve().parent
    fire.Fire(
        partial(
            main,
            ocr_unit="word",
            data_dir=(script_dir / "data").as_posix(),
            parquet_dir=script_dir.as_posix(),
            images_dir=(script_dir / "images").as_posix(),
        )
    )
