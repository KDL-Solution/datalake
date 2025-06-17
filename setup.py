from setuptools import setup, find_packages

setup(
    name="kdl_datalake",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "awswrangler",
        "pandas",
        "boto3",
        "duckdb",
        "dataset",
        "fastapi",
        "uvicorn",
    ],
    python_requires=">=3.8",
) 