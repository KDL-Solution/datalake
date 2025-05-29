#!/usr/bin/env python3
"""
make_schema_from_parquet.py
    usage:  python make_schema_from_parquet.py data.parquet --name kie_kv_struct_v1
결과:  같은 폴더에 data.json 생성
"""
import json, hashlib, argparse, datetime
from pathlib import Path
import pyarrow.parquet as pq


def pa_type_to_str(
    pa_type: str,
) -> str:
    """PyArrow → Glue 호환 문자열(간단 매핑)"""
    if pa_type == "string":
        return "string"
    if pa_type == "double":
        return "double"
    if pa_type == "int64":
        return "int64"
    if pa_type == "boolean":
        return "boolean"
    return "string"          # fallback

def main(
    parquet_path: Path,
    schema_name: str,
) -> None:
    schema = pq.read_schema(parquet_path)
    cols = []
    for f in schema:
        cols.append({
            "name":      f.name,
            "type":      pa_type_to_str(str(f.type)),
            "nullable":  f.nullable,
            "source":    f"name:{f.name}"      
        })

    draft = {
        "schema_name":     schema_name,
        "created":         datetime.datetime.now(
            datetime.timezone.utc,
        ).isoformat(timespec="seconds") + "Z",
        "columns":         cols,
        "version_sha256":  ""  # SHA-256 계산 후 삽입
    }

    # SHA-256 계산 후 삽입
    draft_str = json.dumps(draft, separators=(",", ":"), ensure_ascii=False)
    draft["version_sha256"] = hashlib.sha256(draft_str.encode()).hexdigest()

    #out = parquet_path.with_name(".json")
    out = parquet_path.with_suffix(".json")
    out.write_text(json.dumps(draft, indent=2, ensure_ascii=False))
    print(f"Schema saved to {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--parquet", required=True, type=Path, help="parquet 파일 경로")
    p.add_argument("--name", required=True, help="schema_name 값")
    a = p.parse_args()
    main(a.parquet, a.name)
