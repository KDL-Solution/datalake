from setuptools import setup, find_packages

setup(
    name="athena-query",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "awswrangler",
        "pandas",
        "boto3"
    ],
    python_requires=">=3.7",
) 