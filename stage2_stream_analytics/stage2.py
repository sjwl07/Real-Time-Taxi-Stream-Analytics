"""Stage 2 (Spark Structured Streaming): taxi.clean -> streaming algorithms -> taxi.metrics

Spark reads/parses taxi.clean; the sketches live in one stateful object updated inside
foreachBatch (they need one global ordered stream). State is snapshotted after every batch.

Algorithms: reservoir sampling (k=100/1000/10000), event vs entity sampling per window,
HyperLogLog (own implementation) per window, DGIM (own implementation), Bloom metrics.

Usage:
  python stage2_stream_analytics/stage2.py --fresh --idle-exit 20
  python stage2_stream_analytics/stage2.py --lateness-min 1 --fresh --idle-exit 20
"""
import argparse
import glob
import hashlib
import json
import math
import os
import pickle
import random
import shutil
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yaml
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType)

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

EPOCH = pd.Timestamp("1970-01-01")
ONE_SEC = pd.Timedelta(seconds=1)
LOCK = threading.Lock()

CLEAN_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("dropoff_time", StringType()),
    StructField("pickup_zone", LongType()),
    StructField("dropoff_zone", LongType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("passenger_count", LongType()),
    StructField("payment_type", LongType()),
    StructField("producer_ts_ms", LongType()),
    StructField("is_resend", BooleanType()),
    StructField("is_delayed", BooleanType()),
    StructField("route_key", StringType()),
    StructField("trip_duration_min", DoubleType()),
    StructField("slow_trip_bit", LongType()),
    StructField("bloom_seen_before", BooleanType()),
    StructField("stage1_ts_ms", LongType()),
])

COLS = ["event_id", "pickup_zone", "dropoff_zone", "fare_amount", "trip_distance",
        "slow_trip_bit", "route_key", "bloom_seen_before", "is_resend", "is_delayed",
        "producer_ts_ms"]


# ==========================================================================
# helpers
# ==========================================================================
def h64(s):
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), "big")


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def pct(est, exact):
    return round(abs(est - exact) / exact * 100, 3) if exact else 0.0


def bloom_params(expected_items, fp_target):
    m = int(math.ceil(-expected_items * math.log(fp_target) / (math.log(2) ** 2)))
    k = max(1, int(round(m / expected_items * math.log(2))))
    return m, k


# ==========================================================================
# HyperLogLog (own implementation, 64-bit hash)
# ==========================================================================
class HyperLogLog:
    def __init__(self, p):
        self.p = p
        self.m = 1 << p
        self.reg = bytearray(self.m)

    def add(self, item):
        h = h64(item)
        idx = h >> (64 - self.p)
        w = h & ((1 << (64 - self.p)) - 1)
        rank = (64 - self.p) - w.bit_length() + 1
        if rank > self.reg[idx]:
            self.reg[idx] = rank

    def estimate(self):
        m = self.m
        alpha = 0.7213 / (1 + 1.079 / m)
        e = alpha * m * m / sum(2.0 ** -r for r in self.reg)
        zeros = self.reg.count(0)
        if e <= 2.5 * m and zeros:
            e = m * math.log(m / zeros)             # small-range correction
        return e


# ==========================================================================
# DGIM (own implementation). buckets: list of (end_timestamp, size), newest first
# ==========================================================================
class DGIM:
    def __init__(self, n):
        self.N = n
        self.t = 0
        self.buckets = []

    def add(self, bit):
        self.t += 1
        b = self.buckets
        while b and b[-1][0] <= self.t - self.N:        # expire buckets outside the window
            b.pop()
        if bit != 1:
            return
        b.insert(0, (self.t, 1))
        i, size = 0, 1
        while True:                                     # keep at most 2 buckets per size
            j = i
            while j < len(b) and b[j][1] == size:
                j += 1
            if j - i > 2:
                b[j - 2] = (b[j - 2][0], size * 2)      # merge the two OLDEST of this size
                del b[j - 1]
                i, size = j - 2, size * 2
            else:
                break

    def query(self, k):
        k = min(k, self.N)
        lo = self.t - k
        total, last = 0, 0
        for ts, size in self.buckets:
            if ts > lo:
                total += size
                last = size
            else:
                break
        if total == 0:
            return 0.0
        return total - last + (last if last == 1 else last / 2.0)


# ==========================================================================
# Reservoir sampling (Algorithm R)
# ==========================================================================
class Reservoir:
    def __init__(self, k, seed):
        self.k, self.n = k, 0
        self.rng = random.Random(seed)
        self.fare, self.dist = [], []

    def add(self, fare, dist):
        self.n += 1
        if len(self.fare) < self.k:
            self.fare.append(fare)
            self.dist.append(dist)
        else:
            j = self.rng.randrange(self.n)
            if j < self.k:
                self.fare[j] = fare
                self.dist[j] = dist


# ==========================================================================
# Per-window samplers (same storage budget B events)
# ==========================================================================
class EventSampler:
    """Uniform reservoir of B (pickup, dropoff) events."""
    def __init__(self, budget, seed):
        self.B, self.n = budget, 0
        self.rng = random.Random(seed)
        self.items = []

    def add(self, pu, do):
        self.n += 1
        if len(self.items) < self.B:
            self.items.append((pu, do))
        else:
            j = self.rng.randrange(self.n)
            if j < self.B:
                self.items[j] = (pu, do)

    def estimate(self, dmin):
        d = defaultdict(set)
        for pu, do in self.items:
            d[pu].add(do)
        if not d:
            return None
        diverse = sum(1 for s in d.values() if len(s) >= dmin)
        return {"estimate": diverse / len(d), "active_zones_in_sample": len(d),
                "events_stored": len(self.items)}


_U = {}


def zone_u(zone):
    u = _U.get(zone)
    if u is None:
        u = h64(f"zone:{zone}") / 2.0 ** 64
        _U[zone] = u
    return u


class EntitySampler:
    """Deterministic hash sample of pickup zones; keeps ALL events of selected zones.
    Threshold halves whenever the stored events exceed the budget B."""
    def __init__(self, budget):
        self.B, self.thr, self.stored = budget, 1.0, 0
        self.zones = {}

    def add(self, pu, do):
        if zone_u(pu) >= self.thr:
            return
        self.zones.setdefault(pu, []).append(do)
        self.stored += 1
        while self.stored > self.B and self.thr > 1e-6:
            self.thr /= 2
            for z in [z for z in self.zones if zone_u(z) >= self.thr]:
                self.stored -= len(self.zones.pop(z))

    def estimate(self, dmin):
        if not self.zones:
            return None
        diverse = sum(1 for v in self.zones.values() if len(set(v)) >= dmin)
        return {"estimate": diverse / len(self.zones), "zones_sampled": len(self.zones),
                "events_stored": self.stored, "threshold": self.thr}


class WindowState:
    def __init__(self, start, p, budget, seed):
        self.start, self.n = start, 0
        self.routes = set()                 # exact set: ground truth only
        self.hll = HyperLogLog(p)
        self.dest = defaultdict(set)        # exact zone -> destinations: ground truth only
        self.ev = EventSampler(budget, seed + start)
        self.ent = EntitySampler(budget)

    def add(self, route, pu, do):
        self.n += 1
        self.routes.add(route)
        self.hll.add(route)
        self.dest[pu].add(do)
        self.ev.add(pu, do)
        self.ent.add(pu, do)


# ==========================================================================
# Whole Stage 2 state (pickled after every batch)
# ==========================================================================
class Stage2State:
    def __init__(self, cfg):
        s2 = cfg["stage2"]
        self.W = int(cfg["window_minutes"]) * 60
        self.lateness_s = int(s2["allowed_lateness_min"]) * 60
        self.p = int(cfg["hll_precision"])
        self.budget = int(s2["sample_budget_events"])
        self.dmin = int(s2["diverse_min_destinations"])
        self.seed = int(s2["seed"])
        self.dgim_N = int(cfg["dgim_max_window"])
        self.dgim_queries = [min(int(k), self.dgim_N) for k in s2["dgim_queries"]]

        self.events = 0
        self.sum_fare = 0.0
        self.sum_dist = 0.0
        self.reservoirs = {int(k): Reservoir(int(k), self.seed + int(k))
                           for k in s2["reservoir_sizes"]}
        self.dgim = DGIM(self.dgim_N)
        self.exact_bits = deque(maxlen=self.dgim_N)

        buckets = int(cfg["stage1"]["state_buckets"])
        per = int(math.ceil(cfg["bloom_expected_items"] / buckets))
        m, k = bloom_params(per, cfg["bloom_false_positive_target"])
        self.bloom_cfg = {"bit_array_size": m * buckets, "num_hashes": k,
                          "state_buckets": buckets,
                          "expected_items": cfg["bloom_expected_items"],
                          "fp_target": cfg["bloom_false_positive_target"]}
        self.bloom = {"tp": 0, "fp": 0, "tn": 0, "fn": 0, "fp_examples": []}

        self.windows = {}
        self.windows_closed = 0
        self.max_event_ts = None
        self.watermark = None
        self.late = {"out_of_order": 0, "included": 0, "discarded": 0, "examples": []}
        self.last_batch_id = -1

    # ---------------------------------------------------------------
    def process(self, pdf):
        """pdf is sorted in producer order. Returns messages for windows that closed."""
        W = self.W
        ets = ((pd.to_datetime(pdf["event_time"]) - EPOCH) // ONE_SEC).astype("int64").tolist()
        cols = [pdf[c].tolist() for c in COLS]
        wm = self.watermark                      # watermark from the PREVIOUS batch (Spark semantics)
        run_max = self.max_event_ts
        res = list(self.reservoirs.values())
        bl, late = self.bloom, self.late

        for i, (eid, pu, do, fare, dist, slow, route, seen, resend, delayed, pts) in enumerate(zip(*cols)):
            pu, do, slow = int(pu), int(do), int(slow)
            fare, dist = float(fare), float(dist)
            seen, resend = bool(seen), bool(resend)
            e = ets[i]

            self.events += 1
            self.sum_fare += fare
            self.sum_dist += dist
            for r in res:
                r.add(fare, dist)
            self.dgim.add(slow)
            self.exact_bits.append(slow)

            if seen and resend:
                bl["tp"] += 1
            elif seen:
                bl["fp"] += 1
                if len(bl["fp_examples"]) < 20:
                    bl["fp_examples"].append(eid)
            elif resend:
                bl["fn"] += 1
            else:
                bl["tn"] += 1

            if run_max is None or e >= run_max:
                run_max, behind = e, 0
            else:
                behind = run_max - e
            ws = e - e % W
            closed = wm is not None and ws + W <= wm
            if behind > 0:
                late["out_of_order"] += 1
            if closed:
                late["discarded"] += 1
            else:
                w = self.windows.get(ws)
                if w is None:
                    w = self.windows[ws] = WindowState(ws, self.p, self.budget, self.seed)
                w.add(route, pu, do)
                if behind > 0:
                    late["included"] += 1
            if behind >= 60 and len(late["examples"]) < 50:
                late["examples"].append({
                    "event_id": eid, "event_time": iso(e), "is_delayed": bool(delayed),
                    "stream_max_event_time": iso(run_max), "behind_min": round(behind / 60, 1),
                    "window_start": iso(ws),
                    "decision": "discarded (window already closed)" if closed
                                else "included (window still open)"})

        self.max_event_ts = run_max
        if run_max is not None:
            self.watermark = run_max - self.lateness_s
        out = []
        if self.watermark is not None:
            for ws in sorted(self.windows):
                if ws + W <= self.watermark:
                    out.append(self.finalize(self.windows.pop(ws), "watermark"))
        return out

    # ---------------------------------------------------------------
    def finalize(self, w, reason):
        self.windows_closed += 1
        exact = len(w.routes)
        est = w.hll.estimate()
        exact_bytes = sys.getsizeof(w.routes) + sum(sys.getsizeof(r) for r in w.routes)
        hll = {"exact_routes": exact, "estimate": round(est, 1), "error_pct": pct(est, exact),
               "precision": self.p, "registers": w.hll.m, "sketch_bytes": w.hll.m,
               "exact_set_bytes": exact_bytes}

        active = len(w.dest)
        diverse = sum(1 for s in w.dest.values() if len(s) >= self.dmin)
        exact_frac = diverse / active if active else None
        div = {"min_destinations": self.dmin, "active_zones": active, "diverse_zones": diverse,
               "exact_fraction": None if exact_frac is None else round(exact_frac, 4),
               "budget_events": self.budget, "window_events": w.n}
        for name, sampler in (("event_sampling", w.ev), ("entity_sampling", w.ent)):
            r = sampler.estimate(self.dmin)
            if r is None or exact_frac is None:
                div[name] = None
            else:
                r["estimate"] = round(r["estimate"], 4)
                r["abs_error"] = round(abs(r["estimate"] - exact_frac), 4)
                if "threshold" in r:
                    r["threshold"] = round(r["threshold"], 6)
                div[name] = r
        return {"type": "window", "window_start": iso(w.start), "window_end": iso(w.start + self.W),
                "closed_by": reason, "watermark": iso(self.watermark) if self.watermark else None,
                "allowed_lateness_min": self.lateness_s // 60, "hll": hll, "diversity": div}

    # ---------------------------------------------------------------
    def snapshot(self, rt):
        n = self.events
        now_ms = int(time.time() * 1000)
        bl = self.bloom
        new_ids, dup_ids = bl["fp"] + bl["tn"], bl["tp"] + bl["fn"]
        bloom = dict(self.bloom_cfg)
        bloom.update({"inserted_ids": new_ids, "tp": bl["tp"], "fp": bl["fp"], "tn": bl["tn"],
                      "fn": bl["fn"],
                      "fp_rate": round(bl["fp"] / new_ids, 6) if new_ids else 0.0,
                      "fn_rate": round(bl["fn"] / dup_ids, 6) if dup_ids else 0.0,
                      "fp_examples": bl["fp_examples"][:3]})

        ef = self.sum_fare / n if n else 0.0
        ed = self.sum_dist / n if n else 0.0
        res = []
        for k, r in self.reservoirs.items():
            sf = sum(r.fare) / len(r.fare) if r.fare else 0.0
            sd = sum(r.dist) / len(r.dist) if r.dist else 0.0
            res.append({"k": k, "sample_size": len(r.fare),
                        "exact_mean_fare": round(ef, 4), "sample_mean_fare": round(sf, 4),
                        "error_pct_fare": pct(sf, ef),
                        "exact_mean_distance": round(ed, 4), "sample_mean_distance": round(sd, 4),
                        "error_pct_distance": pct(sd, ed)})
        top = res[-1] if res else {}
        sampling = {"k": top.get("k"), "exact_mean_fare": top.get("exact_mean_fare"),
                    "sample_mean_fare": top.get("sample_mean_fare"),
                    "error_pct": top.get("error_pct_fare"), "reservoirs": res}

        bits = list(self.exact_bits)
        queries = []
        for k in self.dgim_queries:
            exact = int(sum(bits[-k:]))
            est = self.dgim.query(k)
            queries.append({"k": k, "exact_ones": exact, "estimate": round(est, 1),
                            "abs_error": round(abs(est - exact), 1), "error_pct": pct(est, exact)})
        main_q = queries[len(queries) // 2] if queries else {}
        dgim = dict(main_q)
        dgim.update({"window": self.dgim_N, "buckets": len(self.dgim.buckets), "queries": queries})

        lat = np.percentile(list(rt.latencies), [50, 95]) if rt.latencies else [0, 0]
        wall = (rt.last_end - rt.first_ts) if rt.first_ts and rt.last_end else 0
        span = ((rt.max_pts - rt.min_pts) / 1000.0) if rt.min_pts is not None else 0
        return {
            "run_id": rt.run_id, "report_ts_ms": now_ms,
            "last_producer_ts_ms": rt.max_pts,
            "events_processed": n,
            "producer_eps": round(rt.events / span, 1) if span > 0 else 0.0,
            "stage2_eps": round(rt.events / wall, 1) if wall > 0 else 0.0,
            "stage2_busy_eps": round(rt.events / rt.busy_s, 1) if rt.busy_s > 0 else 0.0,
            "consumer_lag": rt.lag,
            "latency_p50_ms": int(lat[0]), "latency_p95_ms": int(lat[1]),
            "bloom": bloom, "sampling": sampling, "dgim": dgim,
            "late_events": {k: self.late[k] for k in ("out_of_order", "included", "discarded")},
            "windows_open": len(self.windows), "windows_closed": self.windows_closed,
        }


class Runtime:
    """Per-process counters (NOT pickled)."""
    def __init__(self, run_id):
        self.run_id = run_id
        self.latencies = deque(maxlen=200000)
        self.events = 0
        self.first_ts = None
        self.last_end = None
        self.busy_s = 0.0
        self.lag = None
        self.min_pts = None
        self.max_pts = None
        self.last_report = 0.0
        self.last_data = None


# ==========================================================================
def save_state(state, path):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def spark_jars():
    jar_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jars")
    jars = sorted(glob.glob(os.path.abspath(jar_dir) + "/*.jar"))
    if not jars:
        sys.exit("ERROR: no jars in ./jars - download them first")
    return ",".join(jars)


def lag_from_progress(p):
    try:
        src = p["sources"][0]
        end, latest = src.get("endOffset") or {}, src.get("latestOffset") or {}
        lag = 0
        for topic, parts in latest.items():
            for part, off in parts.items():
                lag += int(off) - int(end.get(topic, {}).get(part, 0))
        return lag
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--slots", type=int, default=None)
    ap.add_argument("--fresh", action="store_true", help="delete Spark checkpoint + state snapshot")
    ap.add_argument("--idle-exit", type=int, default=0)
    ap.add_argument("--lateness-min", type=int, default=None)
    ap.add_argument("--no-snapshot", action="store_true", help="do not persist/restore state")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    s2 = cfg["stage2"]
    topics = cfg["kafka"]["topics"]
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    slots = args.slots or cfg["spark"]["local_slots"]
    out_dir = cfg["results_dir"]
    os.makedirs(out_dir, exist_ok=True)
    stats_path = os.path.join(out_dir, "stage2_stats.json")
    ckpt, state_file = s2["checkpoint_dir"], s2["state_file"]
    os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)

    if args.fresh:
        shutil.rmtree(ckpt, ignore_errors=True)
        if os.path.exists(state_file):
            os.remove(state_file)

    state = None
    if not args.no_snapshot and os.path.exists(state_file):
        with open(state_file, "rb") as f:
            state = pickle.load(f)
        print(f"[stage2] RESTORED state: events={state.events}, last_batch_id={state.last_batch_id}, "
              f"open windows={len(state.windows)}")
    if state is None:
        state = Stage2State(cfg)
        print("[stage2] starting with EMPTY state")
    if args.lateness_min is not None:
        state.lateness_s = args.lateness_min * 60
    print(f"[stage2] window={state.W//60} min, allowed lateness={state.lateness_s//60} min")

    rt = Runtime(args.run_id or cfg["run_id"])
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5})

    def publish(msgs):
        for m in msgs:
            producer.produce(topics["metrics"], key=rt.run_id.encode(),
                             value=json.dumps(m, separators=(",", ":")).encode())
            producer.poll(0)
        producer.flush()

    spark = (SparkSession.builder.appName("stage2-analytics")
             .master(f"local[{slots}]")
             .config("spark.jars", spark_jars())
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topics["clean"])
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", int(s2["max_offsets_per_trigger"]))
           .option("failOnDataLoss", "false")
           .load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), CLEAN_SCHEMA).alias("e"))
              .select("e.*").filter(F.col("event_id").isNotNull()))

    def process_batch(df, batch_id):
        with LOCK:
            if batch_id <= state.last_batch_id:
                print(f"[stage2] batch {batch_id} already applied (replay after restart) -> skipped")
                return
            t0 = time.time()
            pdf = df.select(*COLS, "event_time").toPandas()
            if pdf.empty:
                return
            pdf = pdf.sort_values(["producer_ts_ms", "event_id"], kind="stable")
            now_ms = int(time.time() * 1000)

            if rt.first_ts is None:
                rt.first_ts = t0
            lat = (now_ms - pdf["producer_ts_ms"]).tolist()
            rt.latencies.extend(lat)
            rt.events += len(pdf)
            pmin, pmax = int(pdf["producer_ts_ms"].min()), int(pdf["producer_ts_ms"].max())
            rt.min_pts = pmin if rt.min_pts is None else min(rt.min_pts, pmin)
            rt.max_pts = pmax if rt.max_pts is None else max(rt.max_pts, pmax)

            windows = state.process(pdf)
            end = time.time()
            rt.busy_s += end - t0
            rt.last_end = end
            rt.last_data = end

            snap = state.snapshot(rt)
            msgs = [dict(w, **{k: v for k, v in snap.items() if k not in w}) for w in windows]
            if end - rt.last_report >= float(s2["report_interval_s"]):
                msgs.append(dict({"type": "progress"}, **snap))
                rt.last_report = end
            publish(msgs)

            state.last_batch_id = batch_id
            if not args.no_snapshot:
                save_state(state, state_file)
            print(f"[stage2] batch {batch_id}: n={len(pdf)} total={state.events} "
                  f"windows_closed={state.windows_closed} open={len(state.windows)} "
                  f"({end - t0:.2f}s)")

    query = (parsed.writeStream.foreachBatch(process_batch)
             .option("checkpointLocation", ckpt)
             .trigger(processingTime=f"{int(s2['trigger_seconds'])} seconds")
             .start())
    print(f"[stage2] running with local[{slots}], reading {topics['clean']} ...")

    def write_stats():
        with LOCK:
            snap = state.snapshot(rt)
            snap["late_examples"] = state.late["examples"][:10]
            with open(stats_path, "w") as f:
                json.dump(snap, f, indent=2)
        return snap

    end_of_stream = False
    last_write = time.time()
    try:
        while query.isActive:
            query.awaitTermination(2)
            lag = lag_from_progress(query.lastProgress)
            if lag is not None:
                rt.lag = lag
            if time.time() - last_write > 5:
                write_stats()
                last_write = time.time()
            if args.idle_exit and rt.last_data and time.time() - rt.last_data > args.idle_exit:
                print("[stage2] idle -> end of stream")
                end_of_stream = True
                break
    except KeyboardInterrupt:
        print("[stage2] Ctrl+C: stopping WITHOUT flushing windows (state stays in the snapshot)")

    query.stop()
    if end_of_stream:                       # flush every still-open window + final progress report
        with LOCK:
            windows = [state.finalize(state.windows.pop(ws), "end_of_stream")
                       for ws in sorted(state.windows)]
            rt.lag = 0 if rt.lag is None else rt.lag
            snap = state.snapshot(rt)
            msgs = [dict(w, **{k: v for k, v in snap.items() if k not in w}) for w in windows]
            msgs.append(dict({"type": "progress", "final": True}, **snap))
            publish(msgs)
        print(f"[stage2] flushed {len(windows)} open windows")
    final = write_stats()
    print(json.dumps(final, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()