
import regex
import swifter
import pandas as pd
import numpy as np
from datasets import Dataset
from typing import List
from transformers import AutoProcessor, PreTrainedTokenizerBase

import sys
sys.path.insert(0, "/home/eric/workspace/datalake/")
from core.datalake import DatalakeClient
from export.utils import (
    EXPORT_DATA_DIR,
    user_prompt_dict,
    save_df_as_jsonl,
)


class Splitter(object):
    def __init__(
        self,
        batch_size: int = 64,
        num_procs: int = 16,
    ):
        self.batch_size = batch_size
        self.num_procs = num_procs
        self.tags_pattern = regex.compile(
            r"<otsl>.*?</otsl>",
            regex.DOTALL,
        )

    def count_total_tags(
        self,
        text: str,
    ):
        if not isinstance(text, str):
            return 0
        return len(regex.findall(self.tags_pattern, text))


def main(
    length_quantile: float = 0.5,
):
    df = pd.read_json(
        "/home/eric/workspace/datalake/export/data/table_image_otsl-train.jsonl",
        lines=True,
    )

    df["length"] = df.swifter.apply(
        lambda x: len(x["messages"][1]["content"]) if isinstance(x["messages"][1]["content"], str) else 0,
        axis=1,
    )
    length_thresh = df["length"].quantile(
        length_quantile,
    )
    df = df[df["length"] > length_thresh]

    save_df_as_jsonl(
        df,
        jsonl_path=f"/home/eric/workspace/datalake/export/data/table_image_otsl-train-{length_quantile}=0.5.jsonl",
    )


if __name__ == "__main__":
    import fire

    fire.Fire(
        main
    )
