# Anomaly Detection — 5 Complementary Approaches

This document explains all 5 anomaly detection methods implemented in the pipeline.

---

## Quick Overview

| # | Name | Timing | Layer | Code | Use Case |
|---|---|---|---|---|---|
| 1️⃣ | **Z-Score** | Real-time | Silver (ingest) | `silver_transforms.py:155` | Baseline deviation (3σ) |
| 2️⃣ | **IQR** | Real-time | Silver (ingest) | `silver_transforms.py:156` | Interquartile outliers |
| 3️⃣ | **Flatline** | Real-time | Silver (streaming) | `flatline_detector.py` | Zero variance over 4h |
| 4️⃣ | **Isolation Forest** | Batch (nightly) | Gold (model) | `isolation_forest.py` | Multivariate anomalies |
| 5️⃣ | **PSI Drift** | Batch (weekly) | Baseline params | `psi_drift_detector.py` | Distribution shift detection |

---

## Detailed Breakdown

### 1️⃣ Z-Score Detection (Ingest-Time, Real-Time)

**What it does:**
- Calculates how many standard deviations away from baseline mean a value is
- Flags if Z > 3.0 (3 standard deviations = 99.7% confidence)

**Code:**
```python
# src/pipeline/silver_transforms.py:155
z_score = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("baseline_std") + eps)
```

**Formula:**
```
Z = |value - mean| / std
Anomaly if Z > 3.0
```

**When triggered:**
- Every micro-batch (~seconds)
- During enrichment stage (Stage 2)

**Columns in `silver.interface_stats`:**
- `ingest_z_score` — the raw Z-score value
- `ingest_flags` — contains `Z_OUTLIER` if triggered

**Example query:**
```sql
SELECT device_id, effective_util_in, ingest_z_score, ingest_flags
FROM rest.silver.interface_stats
WHERE ingest_z_score > 3.0
LIMIT 10;
```

**Pros:**
- Instant detection (microseconds latency)
- Stateless (requires only mean/std baseline)
- Interpretable (standard deviations from mean)

**Cons:**
- Assumes normal distribution
- Sensitive to baseline accuracy

---

### 2️⃣ IQR Detection (Ingest-Time, Real-Time)

**What it does:**
- Detects outliers using Interquartile Range (robust to skewed data)
- Flags if IQR_score > 1.0

**Code:**
```python
# src/pipeline/silver_transforms.py:156
iqr_score = F.abs(F.col("effective_util_in") - F.col("baseline_mean")) / (F.col("iqr_k") * F.col("baseline_std") + eps)
# iqr_k = 1.5 by default (can be tuned by PSI drift detector)
```

**Formula:**
```
IQR_score = |value - median| / (iqr_k × IQR)
Anomaly if IQR_score > 1.0
```

**When triggered:**
- Every micro-batch (~seconds)
- During enrichment stage

**Columns in `silver.interface_stats`:**
- `ingest_iqr_score` — the raw IQR-score value
- `ingest_flags` — contains `IQR_OUTLIER` if triggered

**Example query:**
```sql
SELECT device_id, effective_util_in, ingest_iqr_score, ingest_flags
FROM rest.silver.interface_stats
WHERE ingest_iqr_score > 1.0
LIMIT 10;
```

**Pros:**
- Robust to skewed distributions
- Handles outliers better than Z-score
- Tunable (iqr_k updated by PSI detector)

**Cons:**
- Requires IQR baseline
- Less sensitive to extreme values

---

### 3️⃣ Flatline Detection (Streaming, Real-Time)

**What it does:**
- Detects when a metric is completely flat (no variation)
- Sliding 4-hour window, calculates variance
- Flags if variance == 0.0

**Code:**
```python
# src/models/flatline_detector.py
def detect_from_dataframe(self, df: DataFrame, window_duration: str = "4 hours") -> DataFrame:
    agg = (
        df.filter(F.col("oper_status") == 1)  # Only UP devices
        .groupBy("device_id", "interface_name", F.window("device_ts", window_duration))
        .agg(F.variance("effective_util_in"))
        .filter(F.col("variance") == 0.0)     # Flatline detection
    )
```

**Formula:**
```
variance(util_in, over 4-hour window) == 0.0
Anomaly if true AND oper_status == UP
```

**When triggered:**
- Every streaming micro-batch
- During Stage 3b (Anomaly Detection)

**Columns in `silver.anomaly_flags`:**
- `anomaly_type` = `FLATLINE`
- `anomaly_subtype` = `LOW_VARIANCE`
- `variance` = 0.0
- `window_start`, `window_end` = 4-hour window boundaries

**Example query:**
```sql
SELECT device_id, interface_name, window_start, window_end, variance
FROM rest.silver.anomaly_flags
WHERE anomaly_type = 'FLATLINE'
ORDER BY detected_at DESC
LIMIT 10;
```

**Why it matters:**
- Flat interface utilization = likely misconfigured or offline
- Real interfaces should have natural variation

**Pros:**
- Simple, interpretable (zero variance is binary)
- Detects config/hardware issues

**Cons:**
- Won't catch slow drift
- Noisy in low-traffic periods

---

### 4️⃣ Isolation Forest (Batch, Nightly)

**What it does:**
- Unsupervised machine learning anomaly detector
- Builds a forest of random decision trees
- Points that isolate quickly = anomalies
- Trained weekly, detects nightly

**Code:**
```python
# src/models/isolation_forest.py
class IsolationForestDetector(AnomalyDetector):
    def train(self, features: pd.DataFrame) -> None:
        x = self.scaler.fit_transform(features.values)
        self.model.fit(IsolationForest(n_estimators=100, contamination=0.05))

    def detect(self, records: list[dict]) -> list[AnomalyResult]:
        # Scores last 24h against trained model
        x = self.scaler.transform(numeric_features.values)
        labels = self.model.predict(x)  # -1 = anomaly
```

**Training:**
- Weekly batch job (Airflow DAG: `weekly_batch_dag`)
- Trains on 30 days of "normal" silver data
- Learns multivariate patterns

**Detection:**
- Nightly batch job (Airflow DAG: `nightly_batch_dag` step 4)
- Depends on model artifact from weekly train
- Scores last 24 hours

**Features:**
10-dimensional feature vectors per device+interface:
- Mean utilization (last 24h)
- Std deviation
- Max/min values
- Packet loss (if available)
- Log frequency patterns
- Etc.

**Columns in `gold.anomaly_flags`:**
- `anomaly_type` = `MODEL`
- `anomaly_subtype` = `MULTIVARIATE_ANOMALY`, `FLAPPING`, `RAPID_DRIFT`, etc.
- `score` = isolation score (lower = more anomalous)

**Example query:**
```sql
SELECT device_id, interface_name, anomaly_subtype, score
FROM gold_anomaly_flags
WHERE anomaly_type = 'MODEL'
ORDER BY detected_at DESC
LIMIT 10;
```

**Pros:**
- Catches multivariate patterns
- No assumptions about distribution
- Automatic feature interactions
- Works on unlabeled data

**Cons:**
- Batch only (24h latency)
- Requires training data
- Black-box (hard to explain)

**Distillation:**
- For streaming, model is distilled into DecisionTreeClassifier (max_depth=4)
- Rules can be converted to streaming SQL expressions

---

### 5️⃣ PSI Drift Detection (Weekly Batch)

**What it does:**
- Population Stability Index: measures distribution shift
- Compares last week vs 30-day baseline
- If PSI > 0.25 → significant drift detected
- Automatically adjusts IQR_k for next week

**Code:**
```python
# src/models/psi_drift_detector.py
class PSIDriftDetector:
    def compute_psi(self, baseline: pd.Series, current: pd.Series) -> DriftResult:
        # Discretize into deciles, compare histograms
        psi = sum((current_ratio - baseline_ratio) * log(current_ratio / baseline_ratio))
        # PSI > 0.25 = significant drift
```

**Formula:**
```
PSI = Σ (current_pct - baseline_pct) × ln(current_pct / baseline_pct)
Drift detected if PSI > 0.25
```

**Timing:**
- Weekly batch job (Airflow DAG: `weekly_batch_dag`)
- Runs after model training
- Depends on 30 days of complete silver data

**What it updates:**
- `device_baseline_params.iqr_k` — adjusted for changed distribution
- Next week's ingest-time Z-score/IQR baselines auto-adapt

**Why it matters:**
- Detects when device behavior fundamentally changes
- E.g., network upgrade, traffic pattern shift
- Prevents stale baselines from false alerts

**Example query:**
```sql
SELECT device_id, interface_name, psi_score
FROM device_baseline_params
WHERE psi_score > 0.25;
```

**Pros:**
- Detects slow distribution shifts
- Automatically adapts baselines
- Statistical rigor (industry standard)

**Cons:**
- Batch only (weekly latency)
- Needs 30 days of history
- Less sensitive to point anomalies

---

## How They Work Together

```
Real-Time Stream (microseconds latency):
  ├─ Z-Score: catches 3σ deviations
  ├─ IQR: catches robust outliers
  └─ Flatline: catches zero-variance periods

Night Batch (every 24h):
  └─ Isolation Forest: catches multivariate anomalies

Weekly Batch:
  └─ PSI: detects distribution drift → updates IQR_k

Result: All anomalies written to gold.anomaly_flags
        + Automatically tuned baselines for next period
```

---

## Configuration & Tuning

### Z-Score Threshold
```python
# src/pipeline/silver_transforms.py:163
.withColumn("_z", F.col("ingest_z_score") > F.lit(3.0))
```
- Change `3.0` to `2.0` for more sensitivity (95% confidence)
- Change to `4.0` for less sensitivity (99.99% confidence)

### IQR Threshold & K Value
```python
# src/pipeline/silver_transforms.py:164
.withColumn("_iqr", F.col("ingest_iqr_score") > F.lit(1.0))
```
- Change `1.0` to `0.5` for more sensitivity
- Change to `2.0` for less sensitivity
- `iqr_k` starts at 1.5, tuned weekly by PSI detector

### Flatline Window
```python
# src/models/flatline_detector.py:45
window_duration: str = "4 hours"
```
- Change to `"2 hours"` for faster detection
- Change to `"6 hours"` for more stable baseline

### Isolation Forest
```python
# src/models/isolation_forest.py:20
IsolationForest(n_estimators=100, contamination=0.05)
```
- `contamination=0.05` → expects 5% anomalies
- Lower for fewer alerts, higher for more sensitivity

### PSI Drift Threshold
```python
# src/models/psi_drift_detector.py:18
threshold: float = 0.25
```
- Change to `0.15` for more drift detection
- Change to `0.35` for less sensitivity

---

## Monitoring All Approaches

### Prometheus Metrics to Watch
```
# Total anomalies per approach
anomaly_count{approach="z_score"}
anomaly_count{approach="iqr"}
anomaly_count{approach="flatline"}
anomaly_count{approach="isolation_forest"}
anomaly_count{approach="psi_drift"}

# Baseline health
baseline_mean{device_id, interface_name}
baseline_std{device_id, interface_name}
iqr_k{device_id, interface_name}   # Should change weekly if PSI drift detected

# Pipeline latency
ingest_latency_seconds
batch_job_duration_seconds{job="isolation_forest_train"}
batch_job_duration_seconds{job="isolation_forest_detect"}
batch_job_duration_seconds{job="psi_drift_detector"}
```

### Query Examples

**All anomalies this week:**
```sql
SELECT anomaly_type, anomaly_subtype, COUNT(*) as count
FROM gold_anomaly_flags
WHERE detected_at > NOW() - INTERVAL 7 DAY
GROUP BY anomaly_type, anomaly_subtype;
```

**Devices with multiple anomaly types:**
```sql
SELECT device_id,
  COUNT(DISTINCT anomaly_type) as anomaly_method_count
FROM gold_anomaly_flags
WHERE detected_at > NOW() - INTERVAL 24 HOUR
GROUP BY device_id
HAVING anomaly_method_count > 2;
```

**Compare ingest-time (Z+IQR) vs batch (Isolation Forest):**
```sql
SELECT
  CASE WHEN ingest_anomaly THEN 'Z/IQR' ELSE '-' END as ingest,
  CASE WHEN EXISTS(SELECT 1 FROM gold_anomaly_flags f
                   WHERE f.device_id = s.device_id
                   AND f.anomaly_type = 'MODEL')
       THEN 'IF' ELSE '-' END as batch_model
FROM rest.silver.interface_stats s
WHERE ingest_anomaly OR EXISTS(...)
LIMIT 20;
```

---

## Summary Matrix

| Aspect | Z-Score | IQR | Flatline | Isolation Forest | PSI Drift |
|---|---|---|---|---|---|
| **Latency** | ~1s | ~1s | ~1s | 24h | 7 days |
| **Complexity** | Simple | Simple | Simple | Complex | Moderate |
| **Interpretability** | High (σ) | Medium (quartiles) | Very High (0 var) | Low (black-box) | High (dist shift) |
| **False Positive Rate** | Medium | Low | Very Low | Medium | Very Low |
| **Training Required** | Historical baseline | Historical baseline | None | Yes (weekly) | Yes (30d history) |
| **Tunable** | 3.0 threshold | 1.0 threshold + iqr_k | 4h window | contamination | PSI threshold |
| **Best For** | Sudden spikes | Robust outliers | Config issues | Multivariate patterns | Long-term drift |
