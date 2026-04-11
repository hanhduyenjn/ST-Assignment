from typing import Optional

import pyarrow as pa
from pydantic import BaseModel, ConfigDict


class InterfaceStatsPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    ts: str
    device_id: str
    interface_name: Optional[str] = None
    util_in: Optional[float] = None
    util_out: Optional[float] = None
    admin_status: Optional[int] = None
    oper_status: Optional[int] = None


class SyslogPayload(BaseModel):
    ts: str
    device_id: str
    severity: Optional[int] = None
    message: Optional[str] = None


class InventoryPayload(BaseModel):
    device_id: str
    site_id: str
    vendor: Optional[str] = None
    role: Optional[str] = None


INTERFACE_STATS_SCHEMA = pa.schema([
    pa.field("_raw_payload",    pa.string()),
    pa.field("ts",              pa.string()),
    pa.field("device_id",       pa.string()),
    pa.field("interface_name",  pa.string()),
    pa.field("util_in",         pa.float64()),
    pa.field("util_out",        pa.float64()),
    pa.field("admin_status",    pa.int32()),
    pa.field("oper_status",     pa.int32()),
    pa.field("_extra_cols",     pa.map_(pa.string(), pa.string())),
    pa.field("_ingested_at",    pa.timestamp("us", tz="UTC")),
    pa.field("_source_topic",   pa.string()),
    pa.field("_partition_date", pa.date32()),
])

SYSLOGS_SCHEMA = pa.schema([
    pa.field("_raw_payload",    pa.string()),
    pa.field("ts",              pa.string()),
    pa.field("device_id",       pa.string()),
    pa.field("severity",        pa.int32()),
    pa.field("message",         pa.string()),
    pa.field("_ingested_at",    pa.timestamp("us", tz="UTC")),
    pa.field("_source_topic",   pa.string()),
    pa.field("_partition_date", pa.date32()),
])

INVENTORY_SCHEMA = pa.schema([
    pa.field("device_id",       pa.string()),
    pa.field("site_id",         pa.string()),
    pa.field("vendor",          pa.string()),
    pa.field("role",            pa.string()),
    pa.field("_ingested_at",    pa.timestamp("us", tz="UTC")),
    pa.field("_source_topic",   pa.string()),
    pa.field("_partition_date", pa.date32()),
])
