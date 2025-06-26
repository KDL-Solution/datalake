#!/usr/bin/env python3

from setuptools import setup, find_packages
import os
from pathlib import Path
# Read README for long description
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read version from __init__.py
version = "1.0.0"

# Core dependencies
install_requires = [
    # Core data processing
    "datasets>=3.1.0",
    "pandas>=2.2.3",
    "numpy>=1.26.4",
    
    # Image processing
    "Pillow>=11.0.0",
    
    # Configuration and utilities
    "PyYAML",
    "tqdm>=4.67.0",
    
    # Database
    "duckdb>=1.3.0",
    
    # API server
    "fastapi>=0.115.4",
    "uvicorn[standard]>=0.32.0",
    "pydantic>=2.10.3",
]

# Optional dependencies for different use cases
extras_require = {
    # AWS integration
    "aws": [
        "awswrangler>=3.11.0",
        "boto3>=1.35.69",
        "botocore>=1.35.69",
    ],
}

# All optional dependencies
extras_require["all"] = list(set(sum(extras_require.values(), [])))

# Console scripts
console_scripts = [
    "datalake=main:main",
    "datalake-server=server.app:main",
]

setup(
    name="datalake_management",
    version=version,
    description="Enterprise-grade data lake management system for multimodal data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    
    # Author information
    author="DataTeam",
    url="https://github.com/KDL-Solution/datalake",
    
    # Package information
    packages=find_packages(exclude=["tests*", "docs*", "examples*"]),
    zip_safe=False,

    # Dependencies
    python_requires=">=3.8",
    install_requires=install_requires,
    extras_require=extras_require,
    
    # Console scripts
    entry_points={
        "console_scripts": console_scripts,
    },
)