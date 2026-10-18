"""Stage 3 (Spark Structured Streaming): taxi.clean -> MinHash-LSH per 60-min window -> taxi.lsh

At each window close: destination set S(P) for every pickup zone with >= min_trips trips,
MinHash signatures (datasketch, library allowed), banding + candidate pairs (ours),
exact Jaccard on candidates (keep >= threshold), exact all-pairs baseline for evaluation.
Default setting = lsh_bands x lsh_rows_per_band from config.yaml; extra settings from
stage3.extra_lsh_configs are evaluated on the same signatures.

  python stage3_lsh/stage3.py --fresh --idle-exit 20
"""
import argparse
import itertools
import json
import os
import pickle
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import lag_and_offsets, read_topic, reset_state, spark_session  # noqa: E402

import numpy as np  # noqa: E402
import yaml  # noqa: E402
from confluent_kafka import Producer  # noqa: E402
from datasketch import MinHash  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

SCHEMA = "event_id string, event_time string, pickup_zone long, dropoff_zone long, producer_ts_ms long"


def iso(ts):
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def jaccard(a, b):
    return len(a & b) / len(a | b)


class Stage3State:
    """Pickled after every batch."""
    def __init__(self, cfg):
        s3 = cfg["stage3"]
        self.W, self.lateness_s = int(cfg["window_minutes"]) * 60, int(cfg["allowed_lateness_min"]) * 60
        self.min_trips, self.seed = int(s3["min_trips"]), int(s3["minhash_seed"])
        self.H, self.thr = int(cfg["lsh_num_hashes"]), float(cfg["jaccard_threshold"])
        default = (int(cfg["lsh_bands"]), int(cfg["lsh_rows_per_band"]))
        self.configs = [default] + [tuple(map(int, c)) for c in s3.get("extra_lsh_configs", [])
                                    if tuple(map(int, c)) != default]
        for b, r in self.configs:
            assert b * r == self.H, f"bands x rows must equal lsh_num_hashes={self.H}, got {b}x{r}"
        self.windows = {}                        # window_start -> {zone: [trip_count, set(dropoffs)]}
        self.max_event_ts = self.watermark = None
        self.events = self.discarded = self.windows_done = 0
        self.last_batch_id = -1
        self.results = []

    def process(self, pdf):
        W = self.W
        e = pdf["event_time"].values.astype("datetime64[s]").astype(np.int64)
        ws = e - e % W
        closed = (ws + W <= self.watermark) if self.watermark is not None else np.zeros(len(e), bool)
        self.events += len(pdf)
        self.discarded += int(closed.sum())
        keep = pdf.loc[~closed, ["pickup_zone", "dropoff_zone"]].assign(ws=ws[~closed])
        for (w, z), g in keep.groupby(["ws", "pickup_zone"]):
            rec = self.windows.setdefault(int(w), {}).setdefault(int(z), [0, set()])
            rec[0] += len(g)
            rec[1].update(g["dropoff_zone"].astype(int).tolist())
        self.max_event_ts = int(e.max()) if self.max_event_ts is None else max(self.max_event_ts, int(e.max()))
        self.watermark = self.max_event_ts - self.lateness_s
        return [self.evaluate(s, self.windows.pop(s), "watermark")
                for s in sorted(self.windows) if s + W <= self.watermark]

    def evaluate(self, ws, zones, reason):
        act = {z: d for z, (n, d) in zones.items() if n >= self.min_trips}
        ids = sorted(act)
        n = len(ids)
        possible = n * (n - 1) // 2

        # ground truth: exact Jaccard on ALL pairs (evaluation only)
        t = time.perf_counter()
        sims = {(a, b): jaccard(act[a], act[b]) for a, b in itertools.combinations(ids, 2)}
        truth = {p for p, s in sims.items() if s >= self.thr}
        exact_ms = (time.perf_counter() - t) * 1000

        # MinHash signatures (library)
        t = time.perf_counter()
        mh = MinHash.bulk([[str(d).encode() for d in act[z]] for z in ids], num_perm=self.H, seed=self.seed)
        sig = np.array([m.hashvalues for m in mh], dtype=np.uint64).reshape(n, self.H)
        sig_ms = (time.perf_counter() - t) * 1000

        out = []
        for ci, (b, r) in enumerate(self.configs):
            # banding: zones with an identical band slice share a bucket -> candidate pair
            t = time.perf_counter()
            cands = set()
            for band in range(b):
                buckets = defaultdict(list)
                for i, row in enumerate(sig[:, band * r:(band + 1) * r]):
                    buckets[row.tobytes()].append(ids[i])
                for members in buckets.values():
                    cands.update(itertools.combinations(members, 2))
            cand_ms = (time.perf_counter() - t) * 1000

            # verification: exact Jaccard only for candidates
            t = time.perf_counter()
            jac = {p: jaccard(act[p[0]], act[p[1]]) for p in cands}
            found = {p for p, s in jac.items() if s >= self.thr}
            verify_ms = (time.perf_counter() - t) * 1000

            res = {"bands": b, "rows": r, "threshold_approx": round((1 / b) ** (1 / r), 3),
                   "candidates": len(cands), "true_pairs": len(truth), "found_pairs": len(found),
                   "recall": round(len(found) / len(truth), 4) if truth else None,
                   "candidate_precision": round(len(found) / len(cands), 4) if cands else None,
                   "candidate_reduction": round(1 - len(cands) / possible, 4) if possible else None,
                   "signature_ms": round(sig_ms, 2), "candidate_ms": round(cand_ms, 2), "verify_ms": round(verify_ms, 2),
                   "lsh_latency_ms": round(sig_ms + cand_ms + verify_ms, 2), "exact_latency_ms": round(exact_ms, 2)}
            if ci == 0:                          # default setting: every candidate + every missed true pair
                res["pairs"] = [{"zone_a": p[0], "zone_b": p[1], "exact_jaccard": round(s, 3),
                                 "similar": s >= self.thr} for p, s in sorted(jac.items(), key=lambda kv: -kv[1])]
                res["missed_true_pairs"] = [{"zone_a": p[0], "zone_b": p[1], "exact_jaccard": round(sims[p], 3)}
                                            for p in sorted(truth - found, key=lambda p: -sims[p])]
            out.append(res)

        self.windows_done += 1
        head = out[0]
        self.results.append({"window": iso(ws), "configs": [{k: v for k, v in c.items() if "pairs" not in k}
                                                             for c in out]})
        return {"type": "lsh_window", "window_start": iso(ws), "window_end": iso(ws + self.W), "closed_by": reason,
                "active_zones": n, "possible_pairs": possible, "true_pairs": len(truth),
                "lsh": {k: head[k] for k in ("candidates", "true_pairs", "recall", "candidate_reduction")},
                "configs": out}


def summarize(state, run_id, lag):
    per = defaultdict(list)
    for r in state.results:
        for c in r["configs"]:
            per[f"{c['bands']}x{c['rows']}"].append(c)

    def mean(v, k):
        xs = [c[k] for c in v if c[k] is not None]
        return round(float(np.mean(xs)), 4) if xs else None

    keys = ("candidates", "true_pairs", "recall", "candidate_precision", "candidate_reduction",
            "lsh_latency_ms", "exact_latency_ms")
    return {"run_id": run_id, "events_processed": state.events, "discarded_late": state.discarded,
            "windows_done": state.windows_done, "consumer_lag": lag,
            "configs": {name: {"windows": len(v), **{f"mean_{k}": mean(v, k) for k in keys}} for name, v in per.items()}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--slots", type=int)
    ap.add_argument("--fresh", action="store_true")
    ap.add_argument("--idle-exit", type=int, default=0)
    ap.add_argument("--lateness-min", type=int)
    ap.add_argument("--no-snapshot", action="store_true")
    ap.add_argument("--run-id")
    a = ap.parse_args()

    cfg = yaml.safe_load(open(a.config))
    s3, T = cfg["stage3"], cfg["kafka"]["topics"]
    slots = a.slots or 2
    run_id = a.run_id or cfg["run_id"]
    os.makedirs(cfg["results_dir"], exist_ok=True)
    stats_path = os.path.join(cfg["results_dir"], "stage3_stats.json")
    state_file = s3["state_file"]
    os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)
    if a.fresh:
        reset_state(s3["checkpoint_dir"], state_file)

    if not a.no_snapshot and os.path.exists(state_file):
        state = pickle.load(open(state_file, "rb"))
        print(f"[stage3] RESTORED state: events={state.events}, last_batch_id={state.last_batch_id}", flush=True)
    else:
        state = Stage3State(cfg)
        print("[stage3] starting with EMPTY state", flush=True)
    if a.lateness_min is not None:
        state.lateness_s = a.lateness_min * 60
    print(f"[stage3] window={state.W // 60} min, lateness={state.lateness_s // 60} min, "
          f"LSH settings={state.configs} (first = default)", flush=True)

    producer = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "linger.ms": 5})
    lock = threading.Lock()
    rt = {"lag": None, "last_data": None, "max_pts": None}

    def publish(msgs):
        for m in msgs:
            m.update(run_id=run_id, report_ts_ms=int(time.time() * 1000), last_producer_ts_ms=rt["max_pts"])
            producer.produce(T["lsh"], key=run_id.encode(), value=json.dumps(m, separators=(",", ":")).encode())
        producer.flush()

    spark = spark_session("stage3-lsh", slots)
    parsed = read_topic(spark, cfg, T["clean"], s3["max_offsets_per_trigger"], SCHEMA).filter(F.col("event_id").isNotNull())

    def process_batch(df, batch_id):
        with lock:
            if batch_id <= state.last_batch_id:
                print(f"[stage3] batch {batch_id} already applied (replay) -> skipped", flush=True)
                return
            t0 = time.time()
            pdf = df.toPandas()
            if pdf.empty:
                return
            pdf = pdf.sort_values(["producer_ts_ms", "event_id"], kind="stable")
            rt["max_pts"] = max(rt["max_pts"] or 0, int(pdf["producer_ts_ms"].max()))
            msgs = state.process(pdf)
            publish(msgs)
            rt["last_data"] = time.time()
            state.last_batch_id = batch_id
            if not a.no_snapshot:
                tmp = state_file + ".tmp"
                pickle.dump(state, open(tmp, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
                os.replace(tmp, state_file)
            print(f"[stage3] batch {batch_id}: n={len(pdf)} total={state.events} windows_done="
                  f"{state.windows_done} open={len(state.windows)} ({time.time() - t0:.2f}s)", flush=True)

    query = (parsed.writeStream.foreachBatch(process_batch).option("checkpointLocation", s3["checkpoint_dir"])
             .trigger(processingTime=f"{int(s3['trigger_seconds'])} seconds").start())
    print(f"[stage3] running with local[{slots}], reading {T['clean']} ...", flush=True)

    end_of_stream = False
    try:
        while query.isActive:
            query.awaitTermination(2)
            lag, _ = lag_and_offsets(query.lastProgress)
            if lag is not None:
                rt["lag"] = lag
            if a.idle_exit and rt["last_data"] and time.time() - rt["last_data"] > a.idle_exit:
                print("[stage3] idle -> end of stream", flush=True)
                end_of_stream = True
                break
    except KeyboardInterrupt:
        print("[stage3] Ctrl+C: stopping WITHOUT flushing windows")

    query.stop()
    with lock:
        if end_of_stream:
            msgs = [state.evaluate(s, state.windows.pop(s), "end_of_stream") for s in sorted(state.windows)]
            publish(msgs)
            print(f"[stage3] flushed {len(msgs)} open windows", flush=True)
        final = summarize(state, run_id, rt["lag"] or 0)
    json.dump(final, open(stats_path, "w"), indent=2)
    print(json.dumps(final, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()