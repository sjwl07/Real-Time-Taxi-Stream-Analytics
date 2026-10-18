"""Stage 2 (Spark Structured Streaming): taxi.clean -> streaming algorithms -> taxi.metrics

Libraries: datasketch (HyperLogLog), numpy/pandas (stats, baselines). DGIM, reservoir
sampling and entity sampling are implemented here (no standard library for them).

Usage:
  python stage2_stream_analytics/stage2.py --fresh --idle-exit 20
  python stage2_stream_analytics/stage2.py --lateness-min 1 --fresh --idle-exit 20
"""
import argparse, glob, hashlib, json, math, os, pickle, random, shutil, sys, threading, time
from collections import deque
from datetime import datetime, timezone
from functools import lru_cache

import numpy as np
import pandas as pd
import yaml
from confluent_kafka import Producer
from datasketch import HyperLogLog
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

LOCK = threading.Lock()
EPOCH, ONE_SEC = pd.Timestamp("1970-01-01"), pd.Timedelta(seconds=1)
SCHEMA = ("event_id string, event_time string, dropoff_time string, pickup_zone long, "
          "dropoff_zone long, trip_distance double, fare_amount double, passenger_count long, "
          "payment_type long, producer_ts_ms long, is_resend boolean, is_delayed boolean, "
          "route_key string, trip_duration_min double, slow_trip_bit long, "
          "bloom_seen_before boolean, stage1_ts_ms long")
COLS = ["event_id", "event_time", "pickup_zone", "dropoff_zone", "fare_amount", "trip_distance",
        "slow_trip_bit", "bloom_seen_before", "is_resend", "is_delayed", "producer_ts_ms"]


def iso(ts):
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def pct(est, exact):
    return round(abs(est - exact) / exact * 100, 3) if exact else 0.0


@lru_cache(maxsize=None)
def zone_u(zone):                      # deterministic hash of a zone -> uniform [0,1)
    return int.from_bytes(hashlib.blake2b(f"zone:{zone}".encode(), digest_size=8).digest(), "big") / 2.0 ** 64


def diverse_fraction(pairs, dmin):
    """(fraction of zones with >= dmin distinct destinations, #zones, #diverse) for (pu,do) pairs."""
    if not len(pairs):
        return None, 0, 0
    d = pd.DataFrame(list(pairs), columns=["pu", "do"]).groupby("pu")["do"].nunique()
    return float((d >= dmin).mean()), len(d), int((d >= dmin).sum())


# ==========================================================================
# Algorithms implemented here
# ==========================================================================
class Reservoir:
    """Reservoir sampling, Algorithm R. Stores arbitrary items."""
    def __init__(self, k, seed):
        self.k, self.n, self.rng, self.items = k, 0, random.Random(seed), []

    def add(self, x):
        self.n += 1
        if len(self.items) < self.k:
            self.items.append(x)
        else:
            j = self.rng.randrange(self.n)
            if j < self.k:
                self.items[j] = x


class EntitySampler:
    """Hash sample of pickup zones; keeps ALL events of selected zones.
    The threshold halves whenever the stored events exceed the budget."""
    def __init__(self, budget):
        self.B, self.thr, self.items = budget, 1.0, []

    def add(self, pu, do):
        if zone_u(pu) < self.thr:
            self.items.append((pu, do))
            while len(self.items) > self.B and self.thr > 1e-6:
                self.thr /= 2
                self.items = [x for x in self.items if zone_u(x[0]) < self.thr]


class DGIM:
    """DGIM for counting 1s in the last N bits. buckets = [(end_timestamp, size)], newest first."""
    def __init__(self, n):
        self.N, self.t, self.buckets = n, 0, []

    def add(self, bit):
        self.t += 1
        b = self.buckets
        while b and b[-1][0] <= self.t - self.N:        # drop expired buckets
            b.pop()
        if bit != 1:
            return
        b.insert(0, (self.t, 1))
        i, size = 0, 1
        while True:                                     # at most 2 buckets per size
            j = i
            while j < len(b) and b[j][1] == size:
                j += 1
            if j - i <= 2:
                break
            b[j - 2] = (b[j - 2][0], size * 2)          # merge the two OLDEST of this size
            del b[j - 1]
            i, size = j - 2, size * 2

    def query(self, k):
        lo, total, last = self.t - min(k, self.N), 0, 0
        for ts, size in self.buckets:
            if ts <= lo:
                break
            total, last = total + size, size
        return total - last + (1 if last == 1 else last / 2.0) if total else 0.0


# ==========================================================================
class Window:
    def __init__(self, start, p, budget, seed):
        self.start, self.n, self.pairs = start, 0, set()   # pairs = exact baseline (route == pair)
        self.hll = HyperLogLog(p=p)
        self.ev = Reservoir(budget, seed + start)
        self.ent = EntitySampler(budget)

    def add(self, pus, dos):
        for pu, do in zip(pus, dos):
            self.n += 1
            self.pairs.add((pu, do))
            self.hll.update(f"{pu}-{do}".encode())
            self.ev.add((pu, do))
            self.ent.add(pu, do)


class Stage2State:
    """Everything that is pickled after each batch."""
    def __init__(self, cfg):
        s2 = cfg["stage2"]
        self.W = int(cfg["window_minutes"]) * 60
        self.lateness_s = int(s2["allowed_lateness_min"]) * 60
        self.p, self.budget, self.seed = int(cfg["hll_precision"]), int(s2["sample_budget_events"]), int(s2["seed"])
        self.dmin = int(s2["diverse_min_destinations"])
        N = int(cfg["dgim_max_window"])
        self.dgim_queries = [min(int(k), N) for k in s2["dgim_queries"]]
        self.dgim, self.bits = DGIM(N), deque(maxlen=N)
        self.res = {int(k): Reservoir(int(k), self.seed + int(k)) for k in s2["reservoir_sizes"]}

        nb = int(cfg["stage1"]["state_buckets"])
        per = math.ceil(cfg["bloom_expected_items"] / nb)
        fp = cfg["bloom_false_positive_target"]
        m = math.ceil(-per * math.log(fp) / math.log(2) ** 2)
        self.bloom_cfg = {"bit_array_size": m * nb, "num_hashes": max(1, round(m / per * math.log(2))),
                          "state_buckets": nb, "expected_items": cfg["bloom_expected_items"], "fp_target": fp}
        self.bl = {"tp": 0, "fp": 0, "tn": 0, "fn": 0, "fp_examples": []}

        self.events, self.sum_fare, self.sum_dist = 0, 0.0, 0.0
        self.windows, self.windows_closed = {}, 0
        self.max_event_ts, self.watermark = None, None
        self.late = {"out_of_order": 0, "included": 0, "discarded": 0, "examples": []}
        self.last_batch_id = -1

    # ---------------------------------------------------------------
    def process(self, pdf):
        """pdf is in producer order. Returns the messages of windows that closed."""
        W = self.W
        e = ((pd.to_datetime(pdf["event_time"]) - EPOCH) // ONE_SEC).astype("int64").values
        seen, rs = pdf["bloom_seen_before"].values.astype(bool), pdf["is_resend"].values.astype(bool)

        # Bloom confusion matrix (vectorised)
        self.bl["tp"] += int((seen & rs).sum()); self.bl["fp"] += int((seen & ~rs).sum())
        self.bl["fn"] += int((~seen & rs).sum()); self.bl["tn"] += int((~seen & ~rs).sum())
        for eid in pdf["event_id"].values[seen & ~rs][:20]:
            if len(self.bl["fp_examples"]) < 20:
                self.bl["fp_examples"].append(eid)

        # means, reservoirs, DGIM
        fare, dist, slow = pdf["fare_amount"].values, pdf["trip_distance"].values, pdf["slow_trip_bit"].values
        self.events += len(pdf); self.sum_fare += fare.sum(); self.sum_dist += dist.sum()
        for f, d, s in zip(fare.tolist(), dist.tolist(), slow.tolist()):
            for r in self.res.values():
                r.add((f, d))
            self.dgim.add(s)
        self.bits.extend(slow.tolist())

        # event time: watermark is taken from the PREVIOUS batch (Spark semantics)
        prev = np.iinfo(np.int64).min if self.max_event_ts is None else self.max_event_ts
        run_max = np.maximum.accumulate(np.maximum(e, prev))
        behind = run_max - e
        ws = e - e % W
        closed = (ws + W <= self.watermark) if self.watermark is not None else np.zeros(len(e), bool)
        ooo = behind > 0
        self.late["out_of_order"] += int(ooo.sum()); self.late["discarded"] += int(closed.sum())
        self.late["included"] += int((ooo & ~closed).sum())
        delayed = pdf["is_delayed"].values.astype(bool)
        for i in np.where((behind >= 60) & (delayed | closed))[0][:max(0, 50 - len(self.late["examples"]))]:
            self.late["examples"].append({
                "event_id": pdf["event_id"].iloc[i], "event_time": iso(e[i]),
                "is_delayed": bool(pdf["is_delayed"].iloc[i]), "stream_max_event_time": iso(run_max[i]),
                "behind_min": round(behind[i] / 60, 1), "window_start": iso(ws[i]),
                "decision": "discarded (window already closed)" if closed[i]
                            else "included (window still open)"})

        keep = pdf.loc[~closed, ["pickup_zone", "dropoff_zone"]].assign(ws=ws[~closed])
        for w_start, g in keep.groupby("ws", sort=True):
            w = self.windows.setdefault(int(w_start), Window(int(w_start), self.p, self.budget, self.seed))
            w.add(g["pickup_zone"].tolist(), g["dropoff_zone"].tolist())

        self.max_event_ts = int(run_max[-1])
        self.watermark = self.max_event_ts - self.lateness_s
        return [self.finalize(self.windows.pop(s), "watermark")
                for s in sorted(self.windows) if s + W <= self.watermark]

    # ---------------------------------------------------------------
    def finalize(self, w, reason):
        self.windows_closed += 1
        exact, est = len(w.pairs), w.hll.count()
        hll = {"exact_routes": exact, "estimate": round(est, 1), "error_pct": pct(est, exact),
               "precision": self.p, "registers": w.hll.m, "sketch_bytes": int(w.hll.reg.nbytes),
               "exact_set_bytes": sys.getsizeof(w.pairs) + sum(sys.getsizeof(x) for x in w.pairs)}

        exact_frac, active, diverse = diverse_fraction(w.pairs, self.dmin)
        div = {"min_destinations": self.dmin, "active_zones": active, "diverse_zones": diverse,
               "exact_fraction": None if exact_frac is None else round(exact_frac, 4),
               "budget_events": self.budget, "window_events": w.n}
        for name, items, extra in (("event_sampling", w.ev.items, {}),
                                   ("entity_sampling", w.ent.items, {"threshold": round(w.ent.thr, 6)})):
            frac, zones, _ = diverse_fraction(items, self.dmin)
            div[name] = None if frac is None or exact_frac is None else {
                "estimate": round(frac, 4), "abs_error": round(abs(frac - exact_frac), 4),
                "zones_in_sample": zones, "events_stored": len(items), **extra}
        return {"type": "window", "window_start": iso(w.start), "window_end": iso(w.start + self.W),
                "closed_by": reason, "watermark": iso(self.watermark) if self.watermark else None,
                "allowed_lateness_min": self.lateness_s // 60, "hll": hll, "diversity": div}

    # ---------------------------------------------------------------
    def snapshot(self, rt):
        n, bl = self.events, self.bl
        new_ids, dup_ids = bl["fp"] + bl["tn"], bl["tp"] + bl["fn"]
        bloom = {**self.bloom_cfg, "inserted_ids": new_ids, **{k: bl[k] for k in ("tp", "fp", "tn", "fn")},
                 "fp_rate": round(bl["fp"] / new_ids, 6) if new_ids else 0.0,
                 "fn_rate": round(bl["fn"] / dup_ids, 6) if dup_ids else 0.0,
                 "fp_examples": bl["fp_examples"][:3]}

        ef, ed = self.sum_fare / max(n, 1), self.sum_dist / max(n, 1)
        res = []
        for k, r in self.res.items():
            a = np.array(r.items) if r.items else np.zeros((1, 2))
            sf, sd = a[:, 0].mean(), a[:, 1].mean()
            res.append({"k": k, "sample_size": len(r.items),
                        "exact_mean_fare": round(ef, 4), "sample_mean_fare": round(sf, 4),
                        "error_pct_fare": pct(sf, ef),
                        "exact_mean_distance": round(ed, 4), "sample_mean_distance": round(sd, 4),
                        "error_pct_distance": pct(sd, ed)})
        top = res[-1] if res else {}
        sampling = {"k": top.get("k"), "exact_mean_fare": top.get("exact_mean_fare"),
                    "sample_mean_fare": top.get("sample_mean_fare"),
                    "error_pct": top.get("error_pct_fare"), "reservoirs": res}

        bits = np.fromiter(self.bits, dtype=np.int64)
        queries = []
        for k in self.dgim_queries:
            exact, est = int(bits[-k:].sum()), self.dgim.query(k)
            queries.append({"k": k, "exact_ones": exact, "estimate": round(est, 1),
                            "abs_error": round(abs(est - exact), 1), "error_pct": pct(est, exact)})
        dgim = {**(queries[len(queries) // 2] if queries else {}),
                "window": self.dgim.N, "buckets": len(self.dgim.buckets), "queries": queries}

        p50, p95 = np.percentile(rt.latencies, [50, 95]) if rt.latencies else (0, 0)
        wall = (rt.last_end - rt.first_ts) if rt.first_ts and rt.last_end else 0
        span = (rt.max_pts - rt.min_pts) / 1000.0 if rt.min_pts is not None else 0
        return {"run_id": rt.run_id, "report_ts_ms": int(time.time() * 1000),
                "last_producer_ts_ms": rt.max_pts, "events_processed": n,
                "producer_eps": round(rt.events / span, 1) if span > 0 else 0.0,
                "stage2_eps": round(rt.events / wall, 1) if wall > 0 else 0.0,
                "stage2_busy_eps": round(rt.events / rt.busy_s, 1) if rt.busy_s > 0 else 0.0,
                "consumer_lag": rt.lag, "latency_p50_ms": int(p50), "latency_p95_ms": int(p95),
                "bloom": bloom, "sampling": sampling, "dgim": dgim,
                "late_events": {k: self.late[k] for k in ("out_of_order", "included", "discarded")},
                "windows_open": len(self.windows), "windows_closed": self.windows_closed}


class Runtime:
    """Per-process counters (not pickled)."""
    def __init__(self, run_id):
        self.run_id, self.latencies = run_id, deque(maxlen=200000)
        self.events, self.busy_s, self.lag = 0, 0.0, None
        self.first_ts = self.last_end = self.last_data = self.min_pts = self.max_pts = None
        self.last_report = 0.0


# ==========================================================================
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--slots", type=int)
    ap.add_argument("--fresh", action="store_true", help="delete Spark checkpoint + state snapshot")
    ap.add_argument("--idle-exit", type=int, default=0)
    ap.add_argument("--lateness-min", type=int)
    ap.add_argument("--no-snapshot", action="store_true", help="do not persist/restore state")
    ap.add_argument("--run-id")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    s2, topics, bootstrap = cfg["stage2"], cfg["kafka"]["topics"], cfg["kafka"]["bootstrap_servers"]
    slots = args.slots or cfg["spark"]["local_slots"]
    os.makedirs(cfg["results_dir"], exist_ok=True)
    stats_path = os.path.join(cfg["results_dir"], "stage2_stats.json")
    ckpt, state_file = s2["checkpoint_dir"], s2["state_file"]
    os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)

    if args.fresh:
        shutil.rmtree(ckpt, ignore_errors=True)
        if os.path.exists(state_file):
            os.remove(state_file)

    state = None
    if not args.no_snapshot and os.path.exists(state_file):
        state = pickle.load(open(state_file, "rb"))
        print(f"[stage2] RESTORED state: events={state.events}, last_batch_id={state.last_batch_id}, "
              f"open windows={len(state.windows)}")
    if state is None:
        state = Stage2State(cfg)
        print("[stage2] starting with EMPTY state")
    if args.lateness_min is not None:
        state.lateness_s = args.lateness_min * 60
    print(f"[stage2] window={state.W // 60} min, allowed lateness={state.lateness_s // 60} min")

    rt = Runtime(args.run_id or cfg["run_id"])
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5})

    def publish(msgs):
        for m in msgs:
            producer.produce(topics["metrics"], key=rt.run_id.encode(),
                             value=json.dumps(m, separators=(",", ":")).encode())
            producer.poll(0)
        producer.flush()

    def with_snapshot(windows, snap):
        return [{**w, **{k: v for k, v in snap.items() if k not in w}} for w in windows]

    spark = (SparkSession.builder.appName("stage2-analytics").master(f"local[{slots}]")
             .config("spark.jars", spark_jars())
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    parsed = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap).option("subscribe", topics["clean"])
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", int(s2["max_offsets_per_trigger"]))
              .option("failOnDataLoss", "false").load()
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
              .filter(F.col("event_id").isNotNull()))

    def process_batch(df, batch_id):
        with LOCK:
            if batch_id <= state.last_batch_id:
                print(f"[stage2] batch {batch_id} already applied (replay after restart) -> skipped")
                return
            t0 = time.time()
            pdf = df.select(*COLS).toPandas()
            if pdf.empty:
                return
            pdf = pdf.sort_values(["producer_ts_ms", "event_id"], kind="stable")
            rt.first_ts = rt.first_ts or t0
            rt.latencies.extend((int(time.time() * 1000) - pdf["producer_ts_ms"]).tolist())
            rt.events += len(pdf)
            lo, hi = int(pdf["producer_ts_ms"].min()), int(pdf["producer_ts_ms"].max())
            rt.min_pts = lo if rt.min_pts is None else min(rt.min_pts, lo)
            rt.max_pts = hi if rt.max_pts is None else max(rt.max_pts, hi)

            windows = state.process(pdf)
            end = time.time()
            rt.busy_s += end - t0
            rt.last_end = rt.last_data = end

            snap = state.snapshot(rt)
            msgs = with_snapshot(windows, snap)
            if end - rt.last_report >= float(s2["report_interval_s"]):
                msgs.append({"type": "progress", **snap})
                rt.last_report = end
            publish(msgs)

            state.last_batch_id = batch_id
            if not args.no_snapshot:
                tmp = state_file + ".tmp"
                pickle.dump(state, open(tmp, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
                os.replace(tmp, state_file)
            print(f"[stage2] batch {batch_id}: n={len(pdf)} total={state.events} "
                  f"windows_closed={state.windows_closed} open={len(state.windows)} ({end - t0:.2f}s)")

    query = (parsed.writeStream.foreachBatch(process_batch).option("checkpointLocation", ckpt)
             .trigger(processingTime=f"{int(s2['trigger_seconds'])} seconds").start())
    print(f"[stage2] running with local[{slots}], reading {topics['clean']} ...")

    def write_stats():
        with LOCK:
            snap = state.snapshot(rt)
            snap["late_examples"] = state.late["examples"][:10]
            json.dump(snap, open(stats_path, "w"), indent=2)
        return snap

    end_of_stream, last_write = False, time.time()
    try:
        while query.isActive:
            query.awaitTermination(2)
            lag = lag_from_progress(query.lastProgress)
            if lag is not None:
                rt.lag = lag
            if time.time() - last_write > 1:
                write_stats()
                last_write = time.time()
            if args.idle_exit and rt.last_data and time.time() - rt.last_data > args.idle_exit:
                print("[stage2] idle -> end of stream")
                end_of_stream = True
                break
    except KeyboardInterrupt:
        print("[stage2] Ctrl+C: stopping WITHOUT flushing windows (state stays in the snapshot)")

    query.stop()
    if end_of_stream:                         # flush open windows + final progress report
        with LOCK:
            windows = [state.finalize(state.windows.pop(s), "end_of_stream") for s in sorted(state.windows)]
            rt.lag = 0 if rt.lag is None else rt.lag
            snap = state.snapshot(rt)
            publish(with_snapshot(windows, snap) + [{"type": "progress", "final": True, **snap}])
        print(f"[stage2] flushed {len(windows)} open windows")
    print(json.dumps(write_stats(), indent=2))
    spark.stop()


if __name__ == "__main__":
    main()