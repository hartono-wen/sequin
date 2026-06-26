#!/usr/bin/env python3
"""Watch a Meilisearch backfill live: task fill sizes + index doc-count progress.
Distinguishes 5ms-timeout-under-fill (tasks stay ~40 even with full backfill buffer)
from feed-limit (tasks grow toward batch_size), and redelivery-churn (doc count flat)
from real progress (doc count climbs)."""
import json, time, urllib.request, re
from datetime import datetime, timezone

M = "http://10.7.0.44:7700"; KEY = "aSampleMasterKey"; SRC = "payout_v1_0_0"
S = "https://sequin.staging.xenithpay.com/api"
T = "g33rj6si3IJZWf4H0O5xavyDL65JHKYAfRTnAI4A9m5bmstl_Yw4M_sm-MhuaN8s"

def get(url, token):
    r = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    with urllib.request.urlopen(r, timeout=15) as resp:
        return json.loads(resp.read())

def doc_count():
    return get(f"{M}/indexes/{SRC}/stats", KEY).get("numberOfDocuments")

def recent_tasks():
    d = get(f"{M}/tasks?limit=20&types=documentAdditionOrUpdate&indexUids={SRC}", KEY)
    out = []
    for t in d.get("results", []):
        out.append((t["uid"], (t.get("details") or {}).get("receivedDocuments")))
    return out  # newest first

def backfill_state():
    try:
        d = get(f"{S}/sinks/payment_centrals_payout_requests-meilisearch-sink-poc/backfills", T)
        bs = d.get("data", [])
        if bs:
            b = bs[0]
            return f'{b["state"]}({b.get("rows_processed_count")}/{b.get("rows_initial_count")})'
    except Exception as e:
        return f"err:{e}"
    return "none"

base_dc = doc_count()
base_uid = recent_tasks()[0][0] if recent_tasks() else 0
t0 = time.time()
print(f"baseline: doc_count={base_dc}  latest_task_uid={base_uid}", flush=True)
print("elapsed | backfill_state | doc_count(delta) | new_task_sizes(uid>base) | max_new_task", flush=True)

max_new = 0
DURATION = 300
while time.time() - t0 < DURATION:
    el = int(time.time() - t0)
    dc = doc_count()
    tasks = recent_tasks()
    new_sizes = [sz for (uid, sz) in tasks if uid > base_uid and sz is not None]
    if new_sizes:
        max_new = max(max_new, max(new_sizes))
    bf = backfill_state()
    print(f"{el:4d}s  | {bf:28s} | {dc} ({dc-base_dc:+d}) | {new_sizes[:10]} | max_new={max_new}", flush=True)
    time.sleep(5)

print(f"\nSUMMARY: doc_count {base_dc} -> {doc_count()} (delta {doc_count()-base_dc:+d}); "
      f"largest new task seen = {max_new} docs (batch_size configured = 1000)", flush=True)
