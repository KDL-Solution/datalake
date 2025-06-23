__version__ = "1.0.0"

from .core.datalake import DatalakeClient
from .core.schema import SchemaManager

__all__ = [
    "DatalakeClient",
    "SchemaManager",
]