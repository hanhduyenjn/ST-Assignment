"""Model-test conftest: pre-mock clickhouse_connect before it is imported.

clickhouse_connect transitively imports pyarrow, which has a binary
incompatibility in this environment (_ARRAY_API not found).  Since the
model pipeline tests mock ClickHouseWriter anyway, we stub the entire
clickhouse_connect package so the real C-extension is never loaded.
"""
import sys
import types
from unittest.mock import MagicMock

# Only install the stub once per process.
if "clickhouse_connect" not in sys.modules:
    _stub = types.ModuleType("clickhouse_connect")
    _stub.get_client = MagicMock(return_value=MagicMock())
    sys.modules["clickhouse_connect"] = _stub
