#!/usr/bin/env python3
"""Amortization test: does Meilisearch per-commit cost amortize over bigger batches?
Clones payout_v1_0_0 settings into a throwaway index, populates a realistic base,
then measures single-commit duration at increasing batch sizes. Deletes temp index at end.
Reads each batch's own `duration` (processing-only), so concurrent load on OTHER indexes
does not contaminate the numbers (Meilisearch processes the serial queue one task at a time).
"""
import json, time, urllib.request, urllib.error, re, sys

BASE = "http://10.7.0.44:7700"
KEY = "aSampleMasterKey"
SRC = "payout_v1_0_0"
TMP = "payout_amortest"
HDRS = {"Authorization": "Bearer " + KEY}

def req(method, path, body=None, ctype="application/json", timeout=180):
    data = None
    h = dict(HDRS)
    if body is not None:
        data = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode()
        h["Content-Type"] = ctype
    r = urllib.request.Request(BASE + path, data=data, headers=h, method=method)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}

def dur_s(s):
    if not s: return None
    m = re.match(r"PT(?:(\d+)M)?([\d.]+)S", s)
    return float(m.group(1) or 0) * 60 + float(m.group(2)) if m else None

def wait_task(uid, timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        t = req("GET", f"/tasks/{uid}")
        st = t.get("status")
        if st in ("succeeded", "failed", "canceled"):
            return st, dur_s(t.get("duration")), t
        time.sleep(0.4)
    return "timeout", None, {}

def import_batch(docs, label):
    ndjson = "\n".join(json.dumps(d) for d in docs).encode()
    mb = len(ndjson) / 1e6
    t0 = time.time()
    res = req("POST", f"/indexes/{TMP}/documents", body=ndjson,
              ctype="application/x-ndjson", timeout=600)
    uid = res["taskUid"]
    st, d, _ = wait_task(uid)
    wall = time.time() - t0
    n = len(docs)
    print(f"  {label:>10}: n={n:6d} payload={mb:6.1f}MB  status={st:9s} "
          f"commit_duration={d if d is None else round(d,2):>6}s  "
          f"docs/sec={'-' if not d else round(n/d):>6}  wall={wall:5.1f}s", flush=True)
    return d

def main():
    print(f"[1] clone settings from {SRC}", flush=True)
    settings = req("GET", f"/indexes/{SRC}/settings")
    # drop keys that can't be set back / may error
    for k in ("embedders",):
        settings.pop(k, None)

    print(f"[2] (re)create temp index {TMP}", flush=True)
    try:
        st, _, _ = wait_task(req("DELETE", f"/indexes/{TMP}")["taskUid"])
    except urllib.error.HTTPError:
        pass
    wait_task(req("POST", "/indexes", body={"uid": TMP, "primaryKey": "id"})["taskUid"])

    print(f"[3] apply cloned settings (while empty = fast)", flush=True)
    st, d, t = wait_task(req("PATCH", f"/indexes/{TMP}/settings", body=settings)["taskUid"])
    print(f"    settings task: {st} ({d}s)", flush=True)

    print(f"[4] fetch 50k real docs from {SRC} (fields=*)", flush=True)
    docs = []
    off = 0
    while len(docs) < 50000:
        page = req("GET", f"/indexes/{SRC}/documents?limit=5000&offset={off}&fields=*")
        r = page.get("results", [])
        if not r: break
        docs.extend(r)
        off += 5000
        print(f"    fetched {len(docs)}", flush=True)
    docs = docs[:50000]
    print(f"    total fetched: {len(docs)}", flush=True)

    def prefixed(src, pfx, n):
        out = []
        for i in range(n):
            d = dict(src[i % len(src)])
            d["id"] = f"{pfx}{i}"
            out.append(d)
        return out

    print(f"[5] populate base index to ~100k (2 x 50k large commits)", flush=True)
    import_batch(prefixed(docs, "base0_", 50000), "base 50k")
    import_batch(prefixed(docs, "base1_", 50000), "base 50k")
    stats = req("GET", f"/indexes/{TMP}/stats")
    print(f"    temp index now has {stats.get('numberOfDocuments')} docs", flush=True)

    print(f"[6] MEASURE single-commit duration vs batch size (index already ~100k):", flush=True)
    print(f"    (compare to observed real backfill: ~40-189 docs/commit @ ~2.9s = ~14-65 docs/sec)", flush=True)
    for size in (100, 1000, 10000, 50000):
        import_batch(prefixed(docs, f"m{size}_", size), f"measure {size}")

    print("[done] cleaning up: deleting temp index", flush=True)

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            req("DELETE", f"/indexes/{TMP}")
            print(f"deleted temp index {TMP}", flush=True)
        except Exception as e:
            print(f"WARNING: failed to delete temp index {TMP}: {e}", flush=True)
