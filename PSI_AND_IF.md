================================================================================
DEMONSTRATION: How Isolation Forest Baselines Flow Through Streaming Pipeline
================================================================================

### Step 1: Baseline Parameters in ClickHouse (device_baseline_params table) ###

{"device_id":"ap1","interface_name":"eth0","hour_of_day":1,"day_of_week":0,"baseline_mean":42.7,"baseline_std":5.0976035e-7,"iqr_k":1.5,"isolation_score_threshold":0.65}
{"device_id":"ap1","interface_name":"eth0","hour_of_day":3,"day_of_week":3,"baseline_mean":0,"baseline_std":0,"iqr_k":1.5,"isolation_score_threshold":0.65}
{"device_id":"ap1","interface_name":"eth0","hour_of_day":4,"day_of_week":0,"baseline_mean":17.45,"baseline_std":17.453056,"iqr_k":1.5,"isolation_score_threshold":0.65}
{"device_id":"ap1","interface_name":"eth0","hour_of_day":6,"day_of_week":0,"baseline_mean":14.7,"baseline_std":6.698452e-7,"iqr_k":1.5,"isolation_score_threshold":0.65}
{"device_id":"ap1","interface_name":"eth0","hour_of_day":7,"day_of_week":2,"baseline_mean":75.2,"baseline_std":0,"iqr_k":1.5,"isolation_score_threshold":0.65}


### Step 2: Sample Calculation (baseline matching + anomaly scoring) ###

Given a record arrives:
  device_id: ap1
  interface_name: eth0
  hour_of_day: 2 (2:00 AM)
  day_of_week: 0 (Monday)
  effective_util_in: 42.50%

ClickHouse baseline for ap1/eth0/hour2/day0:
  baseline_mean: 42.70%
  baseline_std: 0.0000005%
  iqr_k: 1.5

Streaming._apply_ingest_scores() calculation:

  1. LEFT JOIN on (device_id, interface_name, hour_of_day, day_of_week)
     ✓ Record matches baseline parameters

  2. Calculate z-score:
     z_score = |42.50 - 42.70| / (0.0000005 + 1e-8)
             = 0.20 / 0.00000051
             = 392,157
     → HIGH_Z_SCORE flag SET (z > 2.0) ✓✓✓

  3. Calculate IQR score:
     iqr_score = |42.50 - 42.70| / (1.5 × 0.0000005 + 1e-8)
               = 0.20 / 0.00000085
               = 235,294
     → IQR_OUTLIER flag SET (iqr > 1.0) ✓✓✓

  4. Final ingest_flags:
     [THRESHOLD_SATURATED?, HIGH_Z_SCORE, IQR_OUTLIER, ...]

     Note: THRESHOLD_SATURATED only fires if util > 80%, not triggered here.


### Step 3: Baseline Cache Refresh Cycle ###

Timeline in streaming.py:

[T=0s]   streaming.py starts
         → _load_baseline_params(ch) called
         → Fetches all 42,408 rows from device_baseline_params table
         → Stored in driver-side _baseline_params list
         → Log message: "streaming.baseline: loaded 42408 device-baseline rows"

[T=1s-1800s]  Interface records arrive continuously
              → Per micro-batch: _ensure_baseline_fresh(ch) called
              → Check: time.monotonic() - _baseline_loaded_at
              → Age < 1800s: skip reload (cache still fresh)
              → Baseline params applied to ALL records via _get_baseline_df()

[T=1801s]     Baseline TTL expired
              → _ensure_baseline_fresh() detects age >= BASELINE_TTL_SECONDS
              → Reloads all baselines from ClickHouse
              → Log message: "streaming.baseline: TTL expired (age=1801s), refreshing from ClickHouse"
              → New isolation_forest rules from weekly training picked up

[T=3600s+]    Repeats every 30 minutes...

### Step 4: What Happens to Records WITHOUT Baseline Match ###

If a NEW device/interface/hour/day_of_week combination arrives (e.g., new device):

  LEFT JOIN result: baseline_mean IS NULL, baseline_std IS NULL

  Spark expressions use COALESCE:
    ingest_z_score   = COALESCE(z_score, 0.0) = 0.0
    ingest_iqr_score = COALESCE(iqr_score, 0.0) = 0.0

  Result: Only THRESHOLD_SATURATED can fire
          HIGH_Z_SCORE and IQR_OUTLIER cannot fire

  → Next week's isolation_forest_train.py includes new device in training
  → New baselines written to ClickHouse
  → Within 30 minutes, streaming picks up new baselines via TTL refresh
  → Future records for this device get full anomaly scoring


### Step 5: Complete Data Flow Diagram ###

isolation_forest_train.py (weekly batch job)
    ↓
    Queries: SELECT device_id, interface_name, avg(effective_util_in), ...
             FROM silver_interface_stats_src
             GROUP BY device_id, interface_name, window_start
    ↓
    Trains: IsolationForestDetector(contamination=0.05)
    ↓
    Distills: decoder.distill_rules(features) → lightweight decision tree rules
    ↓
    Syncs: writer.upsert_baseline_params([
             {"device_id": "ap1", "interface_name": "eth0",
              "hour_of_day": 0..23, "day_of_week": 0..6,
              "baseline_mean": 42.70, "baseline_std": 0.0000005,
              "iqr_k": 1.5, "isolation_score_threshold": 0.65,
              "distilled_rules": '{"source":"isolation_forest",...}'}
           ])
    ↓
ClickHouse device_baseline_params table (persists across streaming restarts)
    ↓
streaming.py (continuous Spark job)
    ↓
    Startup: _load_baseline_params(ch) → _baseline_params cache (in-memory)
    ↓
    Per micro-batch: _apply_ingest_scores()
        ↓
        _get_baseline_df() → snapshot cache as Spark DataFrame
        ↓
        LEFT JOIN incoming records with baseline cache
        on: device_id, interface_name, hour_of_day, day_of_week
        ↓
        For matched: z_score = |util - baseline_mean| / (baseline_std + ε)
                     iqr_score = |util - baseline_mean| / (iqr_k × baseline_std + ε)
        ↓
        Set flags: HIGH_Z_SCORE (z > 2.0), IQR_OUTLIER (iqr > 1.0), THRESHOLD_SATURATED (util > 80)
        ↓
Iceberg silver.interface_stats table (with ingest_z_score, ingest_iqr_score, ingest_flags)
    ↓
    Consumed by: ClickHouse gold tables, dashboards, alerting


================================================================================
Summary: Isolation Forest baselines enable statistical anomaly detection in
the streaming pipeline without requiring model inference at ingest time.
================================================================================


================================================================================
PSI DRIFT DETECTOR vs ISOLATION FOREST: WHEN TO USE WHICH
================================================================================

## Quick Answer
**Use both.** They are complementary, not competitive:
- **Isolation Forest**: Initial baseline calculation (weekly training)
- **PSI Drift Detector**: Detects when IF baselines become stale (weekly validation)


## Side-by-Side Comparison

| Aspect | Isolation Forest | PSI Drift Detector |
|--------|------------------|-------------------|
| **Purpose** | Detect statistical outliers in normal conditions | Detect when baselines are STALE (distribution shift) |
| **Lookback Window** | 30 days of historical data | 7 days current vs 23 days previous |
| **What It Measures** | Anomalies per device/interface/hour/day | Population Stability Index (PSI) |
| **Output** | baseline_mean, baseline_std for each hour/day | baseline_mean, baseline_std if PSI > 0.25 |
| **Updates Baseline** | Always (weekly schedule) | Only if PSI > 0.25 (significant drift) |
| **Source in distilled_rules** | `"source":"isolation_forest"` | `"source":"psi_drift_detector","psi":0.42` |
| **Streaming Uses** | z-score, IQR scoring for real-time anomalies | Same (whichever baseline is latest) |
| **Failure Mode** | Baselines become anchored to outdated patterns | Would miss baseline staleness without PSI |

## Purpose & How They Work

### **Isolation Forest** (`isolation_forest_train.py`)
Trains a machine learning model on 30 days of clean data to learn "normal" behavior:

```
Input:  30 days of network utilization data (no known anomalies)
        ↓
Model:  IsolationForestDetector(contamination=0.05)
        ↓
Output: Per device/interface/hour/day:
        - baseline_mean: average utilization for that time slot
        - baseline_std: standard deviation (spread around mean)
        - iqr_k: 1.5 (scaling factor for IQR bounds)
        ↓
Written to: device_baseline_params with source="isolation_forest"
```

**Example:**
```
Device: ap1, Interface: eth0, Hour: 2 (2:00-3:00 AM), Day: 0 (Monday)
baseline_mean: 42.70%  ← Typical utilization at this time
baseline_std: 0.0000005%  ← Very consistent (low variance)
```

**Streaming uses these to flag:**
- `HIGH_Z_SCORE`: |util - mean| / (std + ε) > 2.0 (2σ deviation)
- `IQR_OUTLIER`: |util - mean| / (1.5×std + ε) > 1.0 (beyond 1.5×IQR fence)

### **PSI Drift Detector** (`psi_drift_detect.py`)
Validates that IF baselines are still accurate by detecting distribution shifts:

```
Baseline Window (23 days ago → 7 days ago):
  ↓
  Histogram of utilization values → baseline distribution
  
Current Window (last 7 days):
  ↓
  Histogram of utilization values → current distribution
  
Comparison: PSI = Σ (current% - baseline%) × ln(current% / baseline%)
  
If PSI > 0.25:  ✓ SIGNIFICANT DRIFT DETECTED
  → Network behavior changed (capacity upgrade, new traffic, etc.)
  → Update baseline_mean and baseline_std to current window
  → Write to device_baseline_params with source="psi_drift_detector"
  
If PSI ≤ 0.25:  ✓ BASELINE STILL VALID
  → IF baselines are still accurate
  → No update needed
```

**Example:**
```
Device: ap1, Interface: eth0, Hour: 2, Day: 0

Before (IF):      baseline_mean = 42.70%
                  baseline_std = 0.0000005%

PSI Analysis: Last 7 days avg utilization = 85.3% (was using old 42.70%)
              PSI = 0.48 > 0.25 ✓ DRIFT DETECTED

After (PSI):      baseline_mean = 85.30%  ← Updated to current behavior
                  baseline_std = 8.2%      ← Updated to current spread
                  distilled_rules: '{"source":"psi_drift_detector","psi":0.48}'
```

## Weekly Pipeline Flow

```
Monday Week 1:
  ↓
  isolation_forest_train.py runs (weekly job)
  ├─ Trains on 30 days of clean data
  ├─ Creates baseline_mean and baseline_std for each device/interface/hour/day
  └─ Writes 42,408 baseline entries to device_baseline_params
     source="isolation_forest"
  
  ↓ (after IF completes)
  
  psi_drift_detect.py runs (weekly job)
  ├─ Compares: last 7 days vs previous 23 days
  ├─ Calculates: PSI for each device/interface/hour/day combo
  └─ For each group where PSI > 0.25:
     └─ Overwrites baseline_mean and baseline_std to current window
        source="psi_drift_detector"
  
  ↓ (both jobs complete)
  
  streaming.py picks up latest baselines (via 30-min TTL refresh)
  ├─ If IF ran → uses IF baselines
  ├─ If PSI ran after IF → uses PSI-updated baselines (wherever drift detected)
  └─ Applies anomaly scoring to ALL records using latest baselines
     → HIGH_Z_SCORE, IQR_OUTLIER flags based on current baseline
```

## When Would Baselines Drift?

**Scenario 1: Network Capacity Upgrade**
```
Before: Device utilization ranges 30-50% at peak hours
  ↓
Upgrade: Add 2x more bandwidth
  ↓
After: Device utilization ranges 15-25% at peak hours
  ↓
IF baseline still anchored to 30-50% range
  ↓
PSI detects: Distribution completely changed (PSI >> 0.25)
  ↓
Updates baseline to 15-25% range
  ↓
Streaming: Stops flagging normal 15-25% as anomalies
```

**Scenario 2: Traffic Pattern Change**
```
Before: Peak traffic at 9-5 business hours
  ↓
After: Company adds 24/7 customer service center
  ↓
After-hours traffic increases 10x
  ↓
PSI detects: Significant shift in after-hours distribution
  ↓
Updates: baseline_std increases for off-peak hours
  ↓
Streaming: Correctly allows higher variance in after-hours data
```

## When to Use Which

### **Use Isolation Forest alone** if:
- Network is stable (no major changes)
- Fast initial deployment (don't need drift detection)
- Simple threshold-based anomalies sufficient

### **Use PSI Drift alone** if:
- You already have good baselines from elsewhere
- You mainly want to detect distribution shifts
- Don't need statistical outlier detection

### **Use both** (RECOMMENDED) if:
- Want reliable baseline initialization (IF)
- Want automatic adaptation to network changes (PSI)
- Need robust statistical anomaly detection
- Operating in production where networks evolve

## Key Insight: Complementary, Not Competitive

Think of them as **quality assurance layers**:

```
Isolation Forest Layer:  "Here's what normal looks like (based on 30-day history)"
                          ↓
Streaming Layer:         "Apply z-score and IQR scoring"
                          ↓
PSI Drift Layer:         "Wait, did normal change? If yes, adjust baselines"
                          ↓
Next Streaming Cycle:    "Use updated baselines for next batch"
```

Without PSI: Baselines become stale after network changes → more false positives/negatives
Without IF: No statistical baseline to start with → manual setup or drift-based init (slow)

With both: Self-healing anomaly detection that adapts to real network evolution ✓