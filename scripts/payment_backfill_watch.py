#!/usr/bin/env python3
"""Monitor the payment_requests -> payin_v1_0_0 backfill on the upgraded box.
Tracks rows_processed (and computes rate/ETA), payin doc count, and task sizes."""
import json, time, urllib.request
from datetime import datetime, timezone

M = "http://10.7.0.44:7700"; KEY = "aSampleMasterKey"; IDX = "payin_v1_0_0"
S = "https://sequin.staging.xenithpay.com"
T = "g33rj6si3IJZWf4H0O5xavyDL65JHKYAfRTnAI4A9m5bmstl_Yw4M_sm-MhuaN8s"
SINK = "payment_centrals_payment_requests-meilisearch-sink-poc"

def get(url, token):
    r = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(r, timeout=15) as resp:
        return json.loads(resp.read())

def backfill():
    b = get(f"{S}/api/sinks/{SINK}/backfills", T)["data"][0]
    return b["state"], b.get("rows_processed_count") or 0, b.get("rows_initial_count") or 0

def doc_count():
    return get(f"{M}/indexes/{IDX}/stats", KEY).get("numberOfDocuments")

def task_sizes():
    d = get(f"{M}/tasks?limit=12&types=documentAdditionOrUpdate&indexUids={IDX}", KEY)
    return [(t.get("details") or {}).get("receivedDocuments") for t in d.get("results", [])]

st0, p0, init = backfill()
t0 = time.time()
print(f"baseline: state={st0} processed={p0}/{init} doc_count={doc_count()}", flush=True)
print("elapsed | state | processed(+delta) | rate r/s | ETA | payin_docs | max_task", flush=True)

DURATION = 300
while time.time() - t0 < DURATION:
    el = time.time() - t0
    try:
        st, p, init = backfill()
        dc = doc_count()
        sizes = [s for s in task_sizes() if s]
        rate = (p - p0) / el if el > 0 else 0
        remaining = (init - p)
        eta = (remaining / rate / 3600) if rate > 0 else float("inf")
        print(f"{int(el):4d}s | {st} | {p}(+{p-p0}) | {rate:6.1f} | {eta:5.1f}h | {dc} | {max(sizes) if sizes else 0}", flush=True)
        if st in ("completed", "cancelled", "failed"):
            print(f"backfill reached terminal state: {st}", flush=True)
            break
    except Exception as e:
        print(f"{int(el):4d}s | error: {e}", flush=True)
    time.sleep(20)

st, p, init = backfill()
print(f"\nSUMMARY: state={st} processed {p0} -> {p} (+{p-p0}) over {int(time.time()-t0)}s; "
      f"avg {(p-p0)/(time.time()-t0):.1f} rows/s", flush=True)
