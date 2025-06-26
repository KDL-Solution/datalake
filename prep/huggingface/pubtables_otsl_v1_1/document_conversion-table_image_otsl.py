if __name__ == "__main__":
    from pathlib import Path

    from prep.huggingface.pubtabnet_otsl.utils import main

    main(
        dataset_name="pubtables_otsl_v1_1",
        # save_dir=Path(__file__).resolve().parent.as_posix(),
    )
