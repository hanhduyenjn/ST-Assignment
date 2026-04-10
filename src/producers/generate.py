import json
import random
import csv
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

from src.common.config import DATA_DIR


VENDORS = ["Cisco", "Arista", "Fortinet", "Juniper", "Palo Alto"]
ROLES = ["router", "switch", "firewall", "access-point", "load-balancer"]
SITES = ["SITE-A", "SITE-B", "SITE-C", "DC1", "DC2"]
DEVICE_IDS = ["edge-r1", "core-sw1", "fw1", "edge-r2", "core-sw2", "ap1", "lb1"]
INTERFACE_NAMES = ["gi0/1", "gi0/2", "gi0/3", "eth0", "eth1", "fa0/0"]
SYSLOG_MESSAGES = [
    "Link Down",
    "Link Up",
    "Config Saved",
    "CPU High",
    "Memory Warning",
    "Interface Flap",
    "BGP Session Reset",
    "OSPF Neighbor Lost",
    "Fan Failure",
    "Temperature Alert",
]


@dataclass
class AnomalyConfig:
    """Controls how often and what kind of anomalies are injected.

    All rates are probabilities in [0.0, 1.0] applied independently per record.

    Anomaly types
    -------------
    missing_field   : randomly drop one field from the record
    extra_field     : add an unexpected field not in the schema
    wrong_type      : replace a numeric field value with a string
    out_of_range    : push a bounded numeric field outside its valid range
    null_value      : set one field to None / null
    schema_change   : rename a field key to something unexpected
    """

    rate: float = 0.0
    missing_field: bool = True
    extra_field: bool = True
    wrong_type: bool = True
    out_of_range: bool = True
    null_value: bool = True
    schema_change: bool = True

    @property
    def enabled(self) -> list:
        return [
            k
            for k in (
                "missing_field",
                "extra_field",
                "wrong_type",
                "out_of_range",
                "null_value",
                "schema_change",
            )
            if getattr(self, k)
        ]


NO_ANOMALY = AnomalyConfig(rate=0.0)
LOW_ANOMALY = AnomalyConfig(rate=0.1)
HIGH_ANOMALY = AnomalyConfig(rate=0.5)


def _inject_anomaly(
    record: dict, anomaly_type: str, numeric_fields: list[str], ts_fields: list[str]
) -> dict:
    """Mutate *record* in-place according to *anomaly_type* and return it."""
    keys = list(record.keys())

    if anomaly_type == "missing_field" and keys:
        del record[random.choice(keys)]

    elif anomaly_type == "extra_field":
        record[f"_unknown_{random.randint(0, 99)}"] = "UNEXPECTED"

    elif anomaly_type == "wrong_type" and numeric_fields:
        field = random.choice(numeric_fields)
        if field in record:
            record[field] = "NaN"

    elif anomaly_type == "out_of_range" and numeric_fields:
        field = random.choice(numeric_fields)
        if field in record:
            record[field] = random.choice([-999.9, 9999.9, -1, 999])

    elif anomaly_type == "null_value" and keys:
        record[random.choice(keys)] = None

    elif anomaly_type == "schema_change" and keys:
        old_key = random.choice(keys)
        record[f"{old_key}_renamed"] = record.pop(old_key)

    return record


def _maybe_inject(
    record: dict, cfg: AnomalyConfig, numeric_fields: list[str], ts_fields: list[str]
) -> dict:
    if cfg.rate > 0 and cfg.enabled and random.random() < cfg.rate:
        anomaly_type = random.choice(cfg.enabled)
        record = _inject_anomaly(record, anomaly_type, numeric_fields, ts_fields)
    return record


def now_ts() -> str:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp()
    end = datetime.now(timezone.utc).timestamp()
    random_ts = random.uniform(start, end)
    return datetime.fromtimestamp(random_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_device_inventory_row(anomaly: AnomalyConfig = NO_ANOMALY) -> dict:
    record = {
        "device_id": f"dev-{random.randint(100, 999)}",
        "site_id": random.choice(SITES),
        "vendor": random.choice(VENDORS),
        "role": random.choice(ROLES),
    }
    return _maybe_inject(record, anomaly, numeric_fields=[], ts_fields=[])


def generate_interface_stats_row(anomaly: AnomalyConfig = NO_ANOMALY) -> dict:
    record = {
        "ts": now_ts(),
        "device_id": random.choice(DEVICE_IDS),
        "interface_name": random.choice(INTERFACE_NAMES),
        "util_in": round(random.uniform(0.0, 100.0), 1),
        "util_out": round(random.uniform(0.0, 100.0), 1),
        "admin_status": random.choice([1, 2]),
        "oper_status": random.choice([1, 2]),
    }
    return _maybe_inject(
        record,
        anomaly,
        numeric_fields=["util_in", "util_out", "admin_status", "oper_status"],
        ts_fields=["ts"],
    )


def generate_syslog_entry(anomaly: AnomalyConfig = NO_ANOMALY) -> dict:
    record = {
        "ts": now_ts(),
        "device_id": random.choice(DEVICE_IDS),
        "severity": random.randint(1, 7),
        "message": random.choice(SYSLOG_MESSAGES),
    }
    return _maybe_inject(record, anomaly, numeric_fields=["severity"], ts_fields=["ts"])


def append_csv(filepath, row: dict):
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        writer.writerow(row)


def append_jsonl(filepath, entry: dict):
    with open(filepath, "a") as f:
        f.write(json.dumps(entry) + "\n")


def generate_data(n: int = 1, anomaly: Optional[AnomalyConfig] = None):
    """Generate *n* records and append them to each data file.

    Parameters
    ----------
    n       : number of records to generate per file
    anomaly : AnomalyConfig instance; defaults to NO_ANOMALY (clean data)
              Use LOW_ANOMALY or HIGH_ANOMALY for quick presets, or build
              a custom AnomalyConfig to enable only specific anomaly types.
    """
    if anomaly is None:
        anomaly = NO_ANOMALY

    inventory_path = DATA_DIR / "device_inventory.csv"
    interface_path = DATA_DIR / "interface_stats.csv"
    syslog_path = DATA_DIR / "syslogs.jsonl"

    for _ in range(n):
        append_csv(inventory_path, generate_device_inventory_row(anomaly))
        append_csv(interface_path, generate_interface_stats_row(anomaly))
        append_jsonl(syslog_path, generate_syslog_entry(anomaly))


if __name__ == "__main__":
    generate_data(n=10000)
    generate_data(n=2000, anomaly=HIGH_ANOMALY)
    print("Done.")
