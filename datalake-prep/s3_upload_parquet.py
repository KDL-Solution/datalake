import boto3
from botocore.exceptions import ClientError
from pathlib import Path
import os


def upload_parquet_to_s3(
    client,
    bucket,
    local_path,
    s3_key,
):
    try:
        client.upload_file(
            Filename=str(local_path),
            Bucket=bucket,
            Key=s3_key
        )
    except ClientError as e:
        print(f"Error uploading {local_path} to s3://{bucket}/{s3_key}: {e}")
        return False
    return True


if __name__ == "__main__":
    from dotenv import load_dotenv
    import argparse
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from athena.src.core.athena_client import AthenaClient

    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--parquet",
        required=True,
        type=Path,
        help="Local parquet file path",
    )
    parser.add_argument(
        "--nas-root",
        required=False,
        type=Path,
        default="/mnt/AI_NAS/datalake",
        help="Local NAS root (for S3 key relpath)",
    )
    parser.add_argument(
        "--bucket",
        required=False,
        default="kdl-data-lake",
        help="S3 bucket name",
    )
    parser.add_argument(
        "--s3-prefix",
        required=False,
        default="",
        help="S3 key prefix (optional)",
    )
    parser.add_argument(
        "--run_crawler",
        required=False,
        action="store_true",
        help="Crawler 실행. (optional)",
    )

    args = parser.parse_args()
    parquet_fp = args.parquet
    nas_root = args.nas_root

    rel_key = parquet_fp.relative_to(nas_root)
    s3_key = os.path.join(args.s3_prefix, str(rel_key)) if args.s3_prefix else str(rel_key)
    s3_key = s3_key.replace("\\", "/")  # 윈도우 호환
    # S3 client 설정
    client = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_DEFAULT_REGION")
    )

    ok = upload_parquet_to_s3(
        client,
        args.bucket,
        parquet_fp,
        s3_key,
    )
    if ok:
        print(f"Uploaded {parquet_fp} to s3://{args.bucket}/{s3_key}")

    # S3에 업로드 후 Crawler를 실행해야 Athena에서 데이터 조회가 가능한 경우가 있음:
    if args.run_crawler:
        client = AthenaClient()
        client.run_crawler(
            crawler_name="catalog_crawler",
        )
