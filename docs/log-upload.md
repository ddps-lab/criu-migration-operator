# Log Upload for Experiment Data Collection

## Overview

When `logUpload: true` is set in StorageConfig, the agent automatically uploads all raw CRIU logs and statistics to S3 after each dump and restore operation. This enables automated experiment data collection without manual log retrieval.

## Enabling

```yaml
spec:
  storage:
    logUpload: true
```

The agent receives this as `LOG_UPLOAD=true` environment variable.

## Uploaded Files

### After Pre-dump
- `criu.log` — CRIU verbose dump log
- `hot-vmas.json` — hot VMA data for prefetch seeding
- `hot_vma_metadata.json` — detailed VMA classification data

### After Final Dump
- `criu.log` — CRIU verbose dump log
- `stats-dump` — CRIU dump statistics (binary format)
- `hot-vmas.json`, `hot_vma_metadata.json`

### After Restore + Lazy-pages Completion
- `restore.log` — CRIU restore log
- `lazy-pages.log` — lazy-pages daemon log (contains per-fault data)
- `stats-restore` — CRIU restore statistics
- `lazy-pages-metrics.json` — parsed per-fault metrics (JSON)

## S3 Key Layout

Files are uploaded under `<s3-prefix>/logs/`:

```
checkpoints/
  my-app/0/worker1/<dump-id>/
    core-*.img, pages-*.img, ...  (checkpoint data)
    hot-vmas.json                 (agent metadata)
    logs/                         (raw experiment data)
      criu.log
      restore.log
      lazy-pages.log
      stats-dump
      stats-restore
      lazy-pages-metrics.json
```

## lazy-pages-metrics.json Format

```json
{
  "TotalFaults": 42,
  "S3Faults": 38,
  "CacheFaults": 4,
  "StallAvg": 2.35,
  "StallP50": 1.8,
  "StallMax": 15.2,
  "S3StallAvg": 2.6,
  "S3StallMax": 15.2,
  "CacheStallAvg": 0.3,
  "CacheStallMax": 0.5,
  "PagesPerFaultAvg": 12.5,
  "PagesPerFaultMax": 128,
  "TotalPagesTransferred": 526,
  "TotalPagesExpected": 526,
  "CacheHitRate": 9.52,
  "PrefetchCompleted": 18,
  "DaemonDurationS": 3.45
}
```

All stall times are in milliseconds. `DaemonDurationS` is the total lazy-pages daemon runtime in seconds.

## Collecting Data for Analysis

### Download all logs for an app
```bash
aws s3 sync s3://checkpoints/my-app/ ./experiment-data/ \
  --include "*/logs/*" --exclude "*"
```

### Parse metrics from multiple runs
```python
import json, glob

metrics = []
for f in glob.glob("experiment-data/**/lazy-pages-metrics.json", recursive=True):
    with open(f) as fh:
        metrics.append(json.load(fh))

avg_stall = sum(m["StallAvg"] for m in metrics) / len(metrics)
print(f"Average stall: {avg_stall:.2f}ms across {len(metrics)} runs")
```
