"""Stage 3 (Spark Structured Streaming): taxi.clean -> MinHash-LSH per 60-min window -> taxi.lsh

Library: datasketch.MinHash. Banding, candidate generation, verification and the exact
all-pairs baseline are implemented here.

Usage:
  python stage3_lsh/stage3.py --fresh --idle-exit 20
"""
import argparse, glob, itertools, json, os, pickle, shutil, sys, threading, time
from collections import defaultdict, deque
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yaml
from confluent_kafka import Producer
from datasketch import MinHash
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

LOCK = threading.Lock()
EPOCH, ONE_SEC = pd.Timestamp("1970-01-01"), pd.Timedelta(seconds=1)
SCHEMA = ("event_id string, event_time string, pickup_zone long, dropoff_zone long, "
          "producer_ts_ms long")
COLS = ["event_id", "event_time", "pickup_zone", "dropoff_zone", "producer_ts_ms"]


def iso(ts):
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def jaccard(a, b):
    return len(a & b) / len(a | b)


class Stage3State:
    """Pickled after every batch."""
    def __init__(self, cfg):
        s3 = cfg["stage3"]
        self.W = int(cfg["window_minutes"]) * 60
        self.lateness_s = int(s3["allowed_lateness_min"]) * 60
        self.min_trips, self.seed = int(s3["min_trips"]), int(s3["minhash_seed"])
        self.H, self.thr = int(cfg["lsh_num_hashes"]), float(cfg["jaccard_threshold"])
        self.configs = [tuple(map(int, c)) for c in s3["lsh_configs"]]
        for b, r in self.configs:
            assert b * r == self.H, f"bands*rows must be {self.H}, got {b}x{r}"
        self.windows = {}                       # window_start -> {zone: [trip_count, set(destinations)]}
        self.max_event_ts = self.watermark = None
        self.events = self.discarded = self.windows_done = 0
        self.last_batch_id = -1
        self.results = []                       # compact per-window results for the final summary

    def process(self, pdf):
        W = self.W
        e = ((pd.to_datetime(pdf["event_time"]) - EPOCH) // ONE_SEC).astype("int64").values
        ws = e - e % W
        closed = (ws + W <= self.watermark) if self.watermark is not None else np.zeros(len(e), bool)
        self.events += len(pdf)
        self.discarded += int(closed.sum())

        keep = pdf.loc[~closed, ["pickup_zone", "dropoff_zone"]].assign(ws=ws[~closed])
        for (w, z), g in keep.groupby(["ws", "pickup_zone"]):
            rec = self.windows.setdefault(int(w), {}).setdefault(int(z), [0, set()])
            rec[0] += len(g)
            rec[1].update(g["dropoff_zone"].astype(int).tolist())

        prev = np.iinfo(np.int64).min if self.max_event_ts is None else self.max_event_ts
        self.max_event_ts = int(max(prev, e.max()))
        self.watermark = self.max_event_ts - self.lateness_s
        return [self.evaluate(s, self.windows.pop(s), "watermark")
                for s in sorted(self.windows) if s + W <= self.watermark]

    def evaluate(self, ws, zones, reason):
        act = {z: d for z, (n, d) in zones.items() if n >= self.min_trips}
        ids = sorted(act)
        n = len(ids)
        possible = n * (n - 1) // 2

        # ---- ground truth: exact all-pairs Jaccard (evaluation baseline) ----
        t = time.perf_counter()
        truth = {(a, b) for a, b in itertools.combinations(ids, 2) if jaccard(act[a], act[b]) >= self.thr}
        exact_ms = (time.perf_counter() - t) * 1000

        # ---- MinHash signatures (datasketch) ----
        t = time.perf_counter()
        sig = np.empty((n, self.H), dtype=np.uint64)
        for i, z in enumerate(ids):
            m = MinHash(num_perm=self.H, seed=self.seed)
            m.update_batch([str(d).encode() for d in act[z]])
            sig[i] = m.hashvalues
        sig_ms = (time.perf_counter() - t) * 1000

        out = []
        for b, r in self.configs:
            # ---- banding: identical band slice -> same bucket -> candidate pair ----
            t = time.perf_counter()
            cands = set()
            for band in range(b):
                block = sig[:, band * r:(band + 1) * r]
                buckets = defaultdict(list)
                for i in range(n):
                    buckets[block[i].tobytes()].append(i)
                for members in buckets.values():
                    if len(members) > 1:
                        cands.update((ids[x], ids[y]) for x, y in itertools.combinations(members, 2))
            cand_ms = (time.perf_counter() - t) * 1000

            # ---- verify candidates with exact Jaccard ----
            t = time.perf_counter()
            found = {p for p in cands if jaccard(act[p[0]], act[p[1]]) >= self.thr}
            verify_ms = (time.perf_counter() - t) * 1000

            tp = len(found & truth)
            out.append({
                "bands": b, "rows": r, "threshold_approx": round((1 / b) ** (1 / r), 3),
                "candidates": len(cands), "true_pairs": len(truth), "found_pairs": len(found),
                "recall": round(tp / len(truth), 4) if truth else None,
                "candidate_precision": round(len(cands & truth) / len(cands), 4) if cands else None,
                "candidate_reduction": round(1 - len(cands) / possible, 4) if possible else None,
                "signature_ms": round(sig_ms, 2), "candidate_ms": round(cand_ms, 2),
                "verify_ms": round(verify_ms, 2),
                "lsh_latency_ms": round(sig_ms + cand_ms + verify_ms, 2),
                "exact_latency_ms": round(exact_ms, 2)})

        self.windows_done += 1
        head = out[0]                           # default configuration (first in the list)
        msg = {"type": "lsh_window", "window_start": iso(ws), "window_end": iso(ws + self.W),
               "closed_by": reason, "active_zones": n, "possible_pairs": possible,
               "true_pairs": len(truth),
               "lsh": {k: head[k] for k in ("candidates", "true_pairs", "recall", "candidate_reduction")},
               "configs": out}
        self.results.append({"window": msg["window_start"], "configs": out})
        return msg


def spark_jars():
    jars = sorted(glob.glob(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jars")) + "/*.jar"))
    if not jars:
        sys.exit("ERROR: no jars in ./jars")
    return ",".join(jars)


def lag_from_progress(p):
    try:
        src = p["sources"][0]
        end, latest = src.get("endOffset") or {}, src.get("latestOffset") or {}
        return sum(int(o) - int(end.get(t, {}).get(pt, 0)) for t, parts in latest.items() for pt, o in parts.items())
    except Exception:
        return None


def summarize(state, rt):
    cfgs = {}
    for r in state.results:
        for c in r["configs"]:
            cfgs.setdefault(f"{c['bands']}x{c['rows']}", []).append(c)

    def mean(xs):
        xs = [x for x in xs if x is not None]
        return round(float(np.mean(xs)), 4) if xs else None

    per = {k: {"windows": len(v),
               "mean_candidates": mean([c["candidates"] for c in v]),
               "mean_true_pairs": mean([c["true_pairs"] for c in v]),
               "mean_recall": mean([c["recall"] for c in v]),
               "mean_candidate_precision": mean([c["candidate_precision"] for c in v]),
               "mean_candidate_reduction": mean([c["candidate_reduction"] for c in v]),
               "mean_lsh_latency_ms": mean([c["lsh_latency_ms"] for c in v]),
               "mean_exact_latency_ms": mean([c["exact_latency_ms"] for c in v])}
           for k, v in cfgs.items()}
    p50, p95 = np.percentile(rt["lat"], [50, 95]) if rt["lat"] else (0, 0)
    return {"run_id": rt["run_id"], "events_processed": state.events, "discarded_late": state.discarded,
            "windows_done": state.windows_done, "consumer_lag": rt["lag"],
            "latency_p50_ms": int(p50), "latency_p95_ms": int(p95), "configs": per}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--slots", type=int)
    ap.add_argument("--fresh", action="store_true")
    ap.add_argument("--idle-exit", type=int, default=0)
    ap.add_argument("--lateness-min", type=int)
    ap.add_argument("--no-snapshot", action="store_true")
    ap.add_argument("--run-id")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    s3, topics, bootstrap = cfg["stage3"], cfg["kafka"]["topics"], cfg["kafka"]["bootstrap_servers"]
    slots = args.slots or cfg["spark"]["local_slots"]
    os.makedirs(cfg["results_dir"], exist_ok=True)
    stats_path = os.path.join(cfg["results_dir"], "stage3_stats.json")
    ckpt, state_file = s3["checkpoint_dir"], s3["state_file"]
    os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)

    if args.fresh:
        shutil.rmtree(ckpt, ignore_errors=True)
        if os.path.exists(state_file):
            os.remove(state_file)

    state = None
    if not args.no_snapshot and os.path.exists(state_file):
        state = pickle.load(open(state_file, "rb"))
        print(f"[stage3] RESTORED state: events={state.events}, last_batch_id={state.last_batch_id}")
    if state is None:
        state = Stage3State(cfg)
        print("[stage3] starting with EMPTY state")
    if args.lateness_min is not None:
        state.lateness_s = args.lateness_min * 60
    print(f"[stage3] window={state.W // 60} min, lateness={state.lateness_s // 60} min, "
          f"configs={state.configs}")

    rt = {"run_id": args.run_id or cfg["run_id"], "lat": deque(maxlen=200000), "lag": None, "last_data": None}
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5})

    def publish(msgs):
        for m in msgs:
            m["run_id"], m["report_ts_ms"] = rt["run_id"], int(time.time() * 1000)
            m["last_producer_ts_ms"] = rt.get("max_pts")
            producer.produce(topics["lsh"], key=rt["run_id"].encode(),
                             value=json.dumps(m, separators=(",", ":")).encode())
            producer.poll(0)
        producer.flush()

    spark = (SparkSession.builder.appName("stage3-lsh").master(f"local[{slots}]")
             .config("spark.jars", spark_jars())
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    parsed = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap).option("subscribe", topics["clean"])
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", int(s3["max_offsets_per_trigger"]))
              .option("failOnDataLoss", "false").load()
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
              .filter(F.col("event_id").isNotNull()))

    def process_batch(df, batch_id):
        with LOCK:
            if batch_id <= state.last_batch_id:
                print(f"[stage3] batch {batch_id} already applied (replay) -> skipped")
                return
            t0 = time.time()
            pdf = df.select(*COLS).toPandas()
            if pdf.empty:
                return
            pdf = pdf.sort_values(["producer_ts_ms", "event_id"], kind="stable")
            rt["lat"].extend((int(time.time() * 1000) - pdf["producer_ts_ms"]).tolist())
            rt["max_pts"] = max(rt.get("max_pts") or 0, int(pdf["producer_ts_ms"].max()))
            msgs = state.process(pdf)
            publish(msgs)
            rt["last_data"] = time.time()
            state.last_batch_id = batch_id
            if not args.no_snapshot:
                tmp = state_file + ".tmp"
                pickle.dump(state, open(tmp, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
                os.replace(tmp, state_file)
            print(f"[stage3] batch {batch_id}: n={len(pdf)} total={state.events} "
                  f"windows_done={state.windows_done} open={len(state.windows)} ({time.time() - t0:.2f}s)")

    query = (parsed.writeStream.foreachBatch(process_batch).option("checkpointLocation", ckpt)
             .trigger(processingTime=f"{int(s3['trigger_seconds'])} seconds").start())
    print(f"[stage3] running with local[{slots}], reading {topics['clean']} ...")

    end_of_stream = False
    try:
        while query.isActive:
            query.awaitTermination(2)
            lag = lag_from_progress(query.lastProgress)
            if lag is not None:
                rt["lag"] = lag
            if args.idle_exit and rt["last_data"] and time.time() - rt["last_data"] > args.idle_exit:
                print("[stage3] idle -> end of stream")
                end_of_stream = True
                break
    except KeyboardInterrupt:
        print("[stage3] Ctrl+C: stopping WITHOUT flushing windows")

    query.stop()
    if end_of_stream:
        with LOCK:
            rt["lag"] = 0 if rt["lag"] is None else rt["lag"]
            msgs = [state.evaluate(s, state.windows.pop(s), "end_of_stream") for s in sorted(state.windows)]
            publish(msgs)
        print(f"[stage3] flushed {len(msgs)} open windows")
    with LOCK:
        final = summarize(state, rt)
        json.dump(final, open(stats_path, "w"), indent=2)
    print(json.dumps(final, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()