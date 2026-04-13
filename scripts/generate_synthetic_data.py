#!/usr/bin/env python3
"""Generate synthetic data with matching device IDs across inventory, interface stats, and syslogs.

The interface stream now includes deterministic 4-minute anomaly bursts so the
streaming pipeline can exercise threshold, z-score, IQR, flatline, and critical
syslog paths without waiting for random chance.
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

try:
    import clickhouse_connect
except Exception:  # pragma: no cover - optional test-time dependency
    clickhouse_connect = None

# Configuration
DATA_DIR = Path(__file__).parent.parent / "data"
NUM_DEVICES = 20
NUM_INTERFACE_STATS_PER_DEVICE = 50
NUM_SYSLOGS_PER_DEVICE = 60
ANOMALY_SECONDS = 240  # 4 minutes of one-second samples
ANOMALY_LOOKBACK_MINUTES = 20

FLATLINE_DEVICE = "edge-r1"
FLATLINE_INTERFACE = "gi0/1"
FLATLINE_UTIL_IN = 42.0
FLATLINE_UTIL_OUT = 17.5
FLATLINE_BASELINE_MEAN = 42.0
FLATLINE_BASELINE_STD = 10.0
FLATLINE_IQR_K = 20.0

ZSCORE_DEVICE = "edge-r2"
ZSCORE_INTERFACE = "gi0/1"
ZSCORE_UTIL_IN = 62.0
ZSCORE_UTIL_OUT = 58.0
ZSCORE_BASELINE_MEAN = 55.0
ZSCORE_BASELINE_STD = 2.0
ZSCORE_IQR_K = 20.0

IQR_DEVICE = "fw1"
IQR_INTERFACE = "gi0/1"
IQR_UTIL_IN = 74.0
IQR_UTIL_OUT = 36.0
IQR_BASELINE_MEAN = 55.0
IQR_BASELINE_STD = 10.0
IQR_IQR_K = 1.5

ANOMALY_ADMIN_STATUS = 1
ANOMALY_OPER_STATUS = 1
ANOMALY_ISOLATION_SCORE_THRESHOLD = 0.65
ANOMALY_VALID_FROM = datetime(2026, 1, 1, 0, 0, 0)
ANOMALY_VALID_UNTIL = datetime(2099, 12, 31, 23, 59, 59)

TEST_PROFILES = [
    {
        "name": "flatline",
        "device_id": FLATLINE_DEVICE,
        "interface_name": FLATLINE_INTERFACE,
        "util_in": FLATLINE_UTIL_IN,
        "util_out": FLATLINE_UTIL_OUT,
        "baseline_mean": FLATLINE_BASELINE_MEAN,
        "baseline_std": FLATLINE_BASELINE_STD,
        "iqr_k": FLATLINE_IQR_K,
        "severity": 1,
        "message": "Temperature Alert",
    },
    {
        "name": "zscore",
        "device_id": ZSCORE_DEVICE,
        "interface_name": ZSCORE_INTERFACE,
        "util_in": ZSCORE_UTIL_IN,
        "util_out": ZSCORE_UTIL_OUT,
        "baseline_mean": ZSCORE_BASELINE_MEAN,
        "baseline_std": ZSCORE_BASELINE_STD,
        "iqr_k": ZSCORE_IQR_K,
        "severity": 2,
        "message": "CPU High",
    },
    {
        "name": "iqr",
        "device_id": IQR_DEVICE,
        "interface_name": IQR_INTERFACE,
        "util_in": IQR_UTIL_IN,
        "util_out": IQR_UTIL_OUT,
        "baseline_mean": IQR_BASELINE_MEAN,
        "baseline_std": IQR_BASELINE_STD,
        "iqr_k": IQR_IQR_K,
        "severity": 2,
        "message": "Memory Warning",
    },
]

TEST_PROFILE_DEVICE_IDS = {profile["device_id"] for profile in TEST_PROFILES}

# Device naming patterns
DEVICE_NAMES = [
    "core-sw1", "core-sw2", "edge-r1", "edge-r2", "fw1", "fw2",
    "ap1", "ap2", "ap3", "lb1", "lb2",
    "router-a", "router-b", "switch-prod", "switch-dr",
    "fw-dmz", "vpn-gateway", "dns-primary", "ntp-server", "syslog-server"
]

SITES = ["DC1", "DC2", "SITE-A", "SITE-B", "SITE-C"]
VENDORS = ["Cisco", "Juniper", "Palo Alto", "Fortinet", "Arista", "Dell", "HP"]
ROLES = ["router", "switch", "firewall", "load-balancer", "ap", "gateway"]
INTERFACES = ["gi0/1", "gi0/2", "eth0", "eth1", "fa0/0", "fa0/1"]
SEVERITIES = [2, 3, 4, 5, 6]  # WARN, INFO, etc.
LOG_MESSAGES = [
    "Interface Flap", "Link Up", "Link Down", "BGP Session Reset",
    "CPU High", "Memory High", "Temperature Warning", "Configuration Changed",
    "Authentication Failed", "Session Timeout", "Packet Loss Detected",
    "Route Flap", "STP Recalculation", "VLAN Mismatch", "DupIP Detected"
]


def generate_inventory():
    """Generate device_inventory.csv with matching device IDs."""
    devices = DEVICE_NAMES[:NUM_DEVICES]

    with open(DATA_DIR / "device_inventory.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["device_id", "site_id", "vendor", "role"])
        writer.writeheader()

        for device_id in devices:
            writer.writerow({
                "device_id": device_id,
                "site_id": random.choice(SITES),
                "vendor": random.choice(VENDORS),
                "role": random.choice(ROLES),
            })

    print(f"✓ Generated device_inventory.csv ({NUM_DEVICES} devices)")
    return devices


def generate_interface_stats(devices):
    """Generate interface_stats.csv with matching device IDs."""
    now = datetime.utcnow().replace(microsecond=0)
    base_time = now - timedelta(days=7)
    anomaly_start = now - timedelta(minutes=ANOMALY_LOOKBACK_MINUTES)

    with open(DATA_DIR / "interface_stats.csv", "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ts", "device_id", "interface_name", "util_in", "util_out", "admin_status", "oper_status"]
        )
        writer.writeheader()

        row_count = 0
        # Keep random background traffic away from deterministic test devices.
        # This prevents baseline refresh jobs from drifting test baselines and
        # mixing z-score and IQR violations onto the same device.
        random_devices = [d for d in devices if d not in TEST_PROFILE_DEVICE_IDS]
        for device_id in random_devices:
            for i in range(NUM_INTERFACE_STATS_PER_DEVICE):
                ts = base_time + timedelta(
                    days=random.randint(0, 100),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )

                writer.writerow({
                    "ts": ts.isoformat() + "Z",
                    "device_id": device_id,
                    "interface_name": random.choice(INTERFACES),
                    "util_in": round(random.uniform(0, 100), 1),
                    "util_out": round(random.uniform(0, 100), 1),
                    "admin_status": random.choice([1, 2]),
                    "oper_status": random.choice([1, 2]),
                })
                row_count += 1

        for profile in TEST_PROFILES:
            for offset in range(ANOMALY_SECONDS):
                ts = anomaly_start + timedelta(seconds=offset)
                writer.writerow({
                    "ts": ts.isoformat() + "Z",
                    "device_id": profile["device_id"],
                    "interface_name": profile["interface_name"],
                    "util_in": profile["util_in"],
                    "util_out": profile["util_out"],
                    "admin_status": ANOMALY_ADMIN_STATUS,
                    "oper_status": ANOMALY_OPER_STATUS,
                })
                row_count += 1

    print(
        f"✓ Generated interface_stats.csv ({row_count} records, including "
        f"{ANOMALY_SECONDS * len(TEST_PROFILES)} deterministic anomaly samples)"
    )


def generate_syslogs(devices):
    """Generate syslogs.jsonl with matching device IDs."""
    now = datetime.utcnow().replace(microsecond=0)
    base_time = now - timedelta(days=7)
    anomaly_start = now - timedelta(minutes=ANOMALY_LOOKBACK_MINUTES)

    with open(DATA_DIR / "syslogs.jsonl", "w") as f:
        row_count = 0
        # Mirror interface-stats behavior so test devices only emit controlled
        # deterministic anomaly windows.
        random_devices = [d for d in devices if d not in TEST_PROFILE_DEVICE_IDS]
        for device_id in random_devices:
            for i in range(NUM_SYSLOGS_PER_DEVICE):
                ts = base_time + timedelta(
                    days=random.randint(0, 100),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )

                record = {
                    "ts": ts.isoformat() + "Z",
                    "device_id": device_id,
                    "severity": random.choice(SEVERITIES),
                    "message": random.choice(LOG_MESSAGES),
                }
                f.write(json.dumps(record) + "\n")
                row_count += 1

        for profile in TEST_PROFILES:
            for offset in range(ANOMALY_SECONDS):
                ts = anomaly_start + timedelta(seconds=offset)
                record = {
                    "ts": ts.isoformat() + "Z",
                    "device_id": profile["device_id"],
                    "severity": profile["severity"],
                    "message": profile["message"],
                }
                f.write(json.dumps(record) + "\n")
                row_count += 1

    print(
        f"✓ Generated syslogs.jsonl ({row_count} records, including "
        f"{ANOMALY_SECONDS * len(TEST_PROFILES)} deterministic critical syslog samples)"
    )


def seed_clickhouse_baseline():
    """Best-effort seed matching baseline rows for the deterministic anomaly bursts."""
    if clickhouse_connect is None:
        print("! clickhouse_connect unavailable; skipping ClickHouse baseline seeding")
        return

    anomaly_start = datetime.utcnow().replace(microsecond=0) - timedelta(minutes=ANOMALY_LOOKBACK_MINUTES)
    hour_of_day = anomaly_start.hour
    # Streaming pipeline computes day_of_week as (spark_dayofweek + 5) % 7 where
    # Spark's dayofweek() is 1=Sun..7=Sat.  Python's weekday() is 0=Mon..6=Sun,
    # which maps identically: Mon→0, Tue→1, ..., Sun→6.
    day_of_week = anomaly_start.weekday()

    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "network_health"),
    )

    # Always upsert: ReplacingMergeTree(valid_from) keeps the row with the latest
    # valid_from, so using utcnow() here guarantees these test baselines win over
    # any stale rows written by the baseline_refresh job.
    rows: list[dict[str, object]] = []
    for profile in TEST_PROFILES:
        rows.append({
            "device_id": profile["device_id"],
            "interface_name": profile["interface_name"],
            "hour_of_day": hour_of_day,
            "day_of_week": day_of_week,
            "baseline_mean": profile["baseline_mean"],
            "baseline_std": profile["baseline_std"],
            "iqr_k": profile["iqr_k"],
            "isolation_score_threshold": ANOMALY_ISOLATION_SCORE_THRESHOLD,
            "distilled_rules": "{}",
            "valid_from": datetime.utcnow(),
            "valid_until": ANOMALY_VALID_UNTIL,
        })

    if not rows:
        return

    client.insert(
        table="device_baseline_params",
        data=[tuple(row.values()) for row in rows],
        column_names=list(rows[0].keys()),
    )
    print(f"✓ Seeded ClickHouse baseline params for {len(rows)} anomaly test devices")


if __name__ == "__main__":
    print("Generating synthetic data with matching device IDs...")
    devices = generate_inventory()
    generate_interface_stats(devices)
    generate_syslogs(devices)
    try:
        seed_clickhouse_baseline()
    except Exception as exc:  # pragma: no cover - best effort seeding only
        print(f"! ClickHouse baseline seeding skipped: {exc}")
    print("\n✓ All data files generated successfully!")
