from .src.core.duckdb_client import DuckDBClient

try:
    from .src.core.athena_client import AthenaClient
    __all__ = ["DuckDBClient", "AthenaClient"]
except ImportError:
    # AWS dependencies not installed
    __all__ = ["DuckDBClient"]