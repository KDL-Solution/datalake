# https://swift.readthedocs.io/en/latest/Customization/Custom-dataset.html
import json
import re
import sys
from PIL import Image
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any

sys.path.insert(0, "/home/eric/workspace/datalake/")
from datalake.export.kie_struct_exporter import KIEStructExporter
from datalake.export.base_layout_exporter import BaseLayoutExporter


if __name__ == "__main__":
    from athena.src.core.athena_client import AthenaClient

    client = AthenaClient()

    kie_exporter = KIEStructExporter()

    df = client.retrieve_with_existing_cols(
        tasks=["vqa"],
        variants=["kie_struct"],
        datasets=["funsd_plus"],
    )
    kie_exporter.export(
        df=df,
        datalake_dir="/mnt/AI_NAS/datalake",
        save_path="/home/eric/workspace/Qwen-SFT/funsd_plus.jsonl",
        indent=0,
    )

    df = client.retrieve_with_existing_cols(
        variants=["kie_struct"],
        providers=["opensource"],
    )
    kie_exporter.export(
        df=df,
        datalake_dir="/mnt/AI_NAS/datalake",
        save_path="/home/eric/workspace/Qwen-SFT/real_kie.jsonl",
        indent=0,
    )


    df = client.retrieve_with_existing_cols(
        datasets=["office_docs"],
    )
    layout_exporter = BaseLayoutExporter()
    layout_exporter.export(
        df=df,
        datalake_dir="/mnt/AI_NAS/datalake",
        save_path="/home/eric/workspace/Qwen-SFT/office_docs.jsonl",
        indent=0,
    )
    
    
#     idx = 0
#     label = df.iloc[idx].to_dict()["label"]
#     indent = 4
#     label_dict = json.loads(label)
#     label_dict
#     label_dict = remove_none_values(label_dict)
#     label_str = json.dumps(
#         label_dict,
#         indent=indent,
#         ensure_ascii=False,
#     )
#     print(label_str)
    
#     label_str = _blank_value_and_bbox(
#         label_str,
#         indent=indent,
#     )
#     print(label_str)    
    
#         image_path = df.iloc[5].to_dict()["image_path"]
#     image_path = image_path.replace("images/images", "images")
#     image = Image.open(image_path).convert("RGB")
#     image.save("/home/eric/workspace/sample.jpg")