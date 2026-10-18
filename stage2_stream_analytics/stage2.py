"""Stage 2 (Spark Structured Streaming): taxi.clean -> streaming algorithms -> taxi.metrics

Libraries: datasketch (HyperLogLog), numpy/pandas. Implemented here: DGIM (required),
reservoir sampling, entity sampling. The sketch state is one object updated in foreachBatch
(DGIM and the reservoirs need one global order) and pickled after every batch together with
the Spark batch_id, so a restart neither loses nor double counts state.
Latency = time the report is delivered to taxi.metrics - producer_ts_ms (per event).

  python stage2_stream_analytics/stage2.py --fresh --idle-exit 20
  python stage2_stream_analytics/stage2.py --lateness-min 30 --fresh --idle-exit 20
"""
import argparse
import hashlib
import json
import math
import os
import pickle
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from functools import lru_cache
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import lag_and_offsets, read_topic, reset_state, spark_session  # noqa: E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402
from confluent_kafka import Producer  # noqa: E402
from datasketch import HyperLogLog  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

SCHEMA = ("event_id string, event_time string, pickup_zone long, dropoff_zone long, fare_amount double, "
          "trip_distance double, slow_trip_bit long, bloom_seen_before boolean, is_resend boolean, "
          "is_delayed boolean, producer_ts_ms long")


def iso(ts):
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def pct(est, exact):
    return round(abs(est - exact) / exact * 100, 3) if exact else 0.0


@lru_cache(maxsize=None)
def zone_u(zone):                        # deterministic hash of a zone -> uniform [0, 1)
    d = hashlib.blake2b(f"zone:{int(zone)}".encode(), digest_size=8).digest()
    return int.from_bytes(d, "big") / 2.0 ** 64


def zone_us(zones):
    return np.fromiter((zone_u(z) for z in zones), float, len(zones))


def diverse_fraction(rows, dmin):
    """rows = [[pickup, dropoff], ...] -> (fraction of zones with >= dmin destinations, #zones, #diverse)."""
    if rows is None or not len(rows):
        return None, 0, 0
    d = pd.DataFrame(rows, columns=["pu", "do"]).groupby("pu")["do"].nunique()
    return float((d >= dmin).mean()), len(d), int((d >= dmin).sum())


# ==========================================================================
# Algorithms implemented by us
# ==========================================================================
class Reservoir:
    """Reservoir sampling with random priorities: every event gets a random key and the reservoir
    keeps the k events with the smallest keys seen so far -> uniform sample of size k, one pass."""
    def __init__(self, k, seed):
        self.k, self.rng, self.keys, self.rows = k, np.random.default_rng(seed), np.empty(0), None

    def add(self, rows):
        keys = np.concatenate([self.keys, self.rng.random(len(rows))])
        rows = rows if self.rows is None else np.vstack([self.rows, rows])
        if len(keys) > self.k:
            keep = np.argpartition(keys, self.k)[:self.k]
            keys, rows = keys[keep], rows[keep]
        self.keys, self.rows = keys, rows


class EntitySampler:
    """Deterministic hash sample of pickup zones: a zone is kept if hash(zone) < thr, with ALL its
    events. thr halves whenever the stored events exceed the budget, so kept zones stay complete."""
    def __init__(self, budget):
        self.B, self.thr, self.rows = budget, 1.0, np.empty((0, 2), dtype=np.int64)

    def add(self, rows):
        self.rows = np.vstack([self.rows, rows[zone_us(rows[:, 0]) < self.thr]])
        while len(self.rows) > self.B and self.thr > 1e-6:
            self.thr /= 2
            self.rows = self.rows[zone_us(self.rows[:, 0]) < self.thr]


class DGIM:
    """DGIM: number of 1s among the last N bits. buckets = [(end_timestamp, size)], newest first."""
    def __init__(self, n):
        self.N, self.t, self.buckets = n, 0, []

    def add(self, bit):
        self.t += 1
        b = self.buckets
        while b and b[-1][0] <= self.t - self.N:        # drop buckets that left the window
            b.pop()
        if bit != 1:
            return
        b.insert(0, (self.t, 1))
        i, size = 0, 1
        while True:                                     # keep at most 2 buckets of each size
            j = i
            while j < len(b) and b[j][1] == size:
                j += 1
            if j - i <= 2:
                break
            b[j - 2] = (b[j - 2][0], size * 2)          # merge the two OLDEST; keep the newer end time
            del b[j - 1]
            i, size = j - 2, size * 2

    def query(self, k):
        lo, total, last = self.t - min(k, self.N), 0, 0
        for ts, size in self.buckets:
            if ts <= lo:
                break
            total, last = total + size, size
        return total - last + (1 if last == 1 else last / 2.0) if total else 0.0   # oldest bucket counts half


# ==========================================================================
class Window:
    def __init__(self, start, p, budget, seed):
        self.start, self.n, self.pairs = start, 0, set()   # exact baseline: distinct (pickup, dropoff)
        self.hll = HyperLogLog(p=p)
        self.ev, self.ent = Reservoir(budget, seed + start), EntitySampler(budget)

    def add(self, rows):                                   # rows = int array [[pickup, dropoff], ...]
        self.n += len(rows)
        for pu, do in rows.tolist():
            self.pairs.add((pu, do))
            self.hll.update(f"{pu}-{do}".encode())         # route_key
        self.ev.add(rows)
        self.ent.add(rows)


class Stage2State:
    """Everything that is pickled after each batch."""
    def __init__(self, cfg):
        s2 = cfg["stage2"]
        self.W, self.lateness_s = int(cfg["window_minutes"]) * 60, int(cfg["allowed_lateness_min"]) * 60
        self.p, self.budget, self.seed = int(cfg["hll_precision"]), int(s2["sample_budget_events"]), int(s2["seed"])
        self.dmin = int(s2["diverse_min_destinations"])
        N = int(cfg["dgim_max_window"])
        self.dgim, self.bits = DGIM(N), deque(maxlen=N)   # bits = exact baseline for DGIM
        self.dgim_queries = [min(int(k), N) for k in s2["dgim_queries"]]
        self.res = {int(k): Reservoir(int(k), self.seed + int(k)) for k in s2["reservoir_sizes"]}

        nb, fp = int(cfg["stage1"]["state_buckets"]), cfg["bloom_false_positive_target"]
        per = math.ceil(cfg["bloom_expected_items"] / nb)  # same sizing as Stage 1
        m = math.ceil(-per * math.log(fp) / math.log(2) ** 2)
        self.bloom_cfg = {"bit_array_size": m * nb, "num_hashes": max(1, round(m / per * math.log(2))),
                          "state_buckets": nb, "expected_items": cfg["bloom_expected_items"], "fp_target": fp}
        self.bl = {"tp": 0, "fp": 0, "tn": 0, "fn": 0, "fp_examples": []}

        self.events, self.sum_fare, self.sum_dist = 0, 0.0, 0.0
        self.windows, self.windows_closed = {}, 0
        self.max_event_ts = self.watermark = None
        self.late = {"out_of_order": 0, "included": 0, "discarded": 0, "examples": []}
        self.last_batch_id = -1

    # ---------------------------------------------------------------
    def process(self, pdf):
        """pdf is in producer order. Returns the messages of windows that closed."""
        W = self.W
        e = pdf["event_time"].values.astype("datetime64[s]").astype(np.int64)

        seen, rs = pdf["bloom_seen_before"].to_numpy(bool), pdf["is_resend"].to_numpy(bool)
        for k, mask in (("tp", seen & rs), ("fp", seen & ~rs), ("fn", ~seen & rs), ("tn", ~seen & ~rs)):
            self.bl[k] += int(mask.sum())
        self.bl["fp_examples"] += pdf["event_id"].values[seen & ~rs].tolist()[:20 - len(self.bl["fp_examples"])]

        fd = pdf[["fare_amount", "trip_distance"]].to_numpy(float)
        self.events += len(pdf)
        self.sum_fare, self.sum_dist = self.sum_fare + fd[:, 0].sum(), self.sum_dist + fd[:, 1].sum()
        for r in self.res.values():
            r.add(fd)
        slow = pdf["slow_trip_bit"].tolist()
        for s in slow:
            self.dgim.add(s)
        self.bits.extend(slow)

        # event time: the watermark from the PREVIOUS batch decides what is too late (Spark semantics)
        prev = np.iinfo(np.int64).min if self.max_event_ts is None else self.max_event_ts
        run_max = np.maximum.accumulate(np.maximum(e, prev))
        behind, ws = run_max - e, e - e % W
        closed = (ws + W <= self.watermark) if self.watermark is not None else np.zeros(len(e), bool)
        delayed = pdf["is_delayed"].to_numpy(bool)
        self.late["out_of_order"] += int((behind > 0).sum())
        self.late["discarded"] += int(closed.sum())
        self.late["included"] += int(((behind > 0) & ~closed).sum())
        for i in np.where((behind >= 60) & (delayed | closed))[0][:max(0, 50 - len(self.late["examples"]))]:
            self.late["examples"].append({
                "event_id": pdf["event_id"].iloc[i], "event_time": iso(e[i]), "is_delayed": bool(delayed[i]),
                "stream_max_event_time": iso(run_max[i]), "behind_min": round(behind[i] / 60, 1),
                "window_start": iso(ws[i]),
                "decision": "discarded (window already closed)" if closed[i] else "included (window still open)"})

        rows = pdf[["pickup_zone", "dropoff_zone"]].to_numpy(np.int64)
        for s in np.unique(ws[~closed]):
            w = self.windows.setdefault(int(s), Window(int(s), self.p, self.budget, self.seed))
            w.add(rows[(ws == s) & ~closed])

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

        exact_frac, active, diverse = diverse_fraction(np.array(list(w.pairs)), self.dmin)
        div = {"min_destinations": self.dmin, "active_zones": active, "diverse_zones": diverse,
               "exact_fraction": None if exact_frac is None else round(exact_frac, 4),
               "budget_events": self.budget, "window_events": w.n}
        for name, rows, extra in (("event_sampling", w.ev.rows, {}),
                                  ("entity_sampling", w.ent.rows, {"threshold": round(w.ent.thr, 6)})):
            frac, zones, _ = diverse_fraction(rows, self.dmin)
            div[name] = None if frac is None or exact_frac is None else {
                "estimate": round(frac, 4), "abs_error": round(abs(frac - exact_frac), 4),
                "zones_in_sample": zones, "events_stored": len(rows), **extra}
        return {"type": "window", "window_start": iso(w.start), "window_end": iso(w.start + self.W),
                "closed_by": reason, "watermark": iso(self.watermark) if self.watermark else None,
                "allowed_lateness_min": self.lateness_s // 60, "hll": hll, "diversity": div}

    # ---------------------------------------------------------------
    def snapshot(self, rt):
        n, bl = self.events, self.bl
        new_ids, dup_ids = bl["fp"] + bl["tn"], bl["tp"] + bl["fn"]
        bloom = {**self.bloom_cfg, "inserted_ids": new_ids, **{k: bl[k] for k in ("tp", "fp", "tn", "fn")},
                 "fp_rate": round(bl["fp"] / new_ids, 6) if new_ids else 0.0,
                 "fn_rate": round(bl["fn"] / dup_ids, 6) if dup_ids else 0.0, "fp_examples": bl["fp_examples"][:3]}

        ef, ed = self.sum_fare / max(n, 1), self.sum_dist / max(n, 1)
        res = []
        for k, r in self.res.items():
            sf, sd = r.rows.mean(axis=0) if r.rows is not None else (0.0, 0.0)
            res.append({"k": k, "sample_size": len(r.keys),
                        "exact_mean_fare": round(ef, 4), "sample_mean_fare": round(sf, 4), "error_pct_fare": pct(sf, ef),
                        "exact_mean_distance": round(ed, 4), "sample_mean_distance": round(sd, 4),
                        "error_pct_distance": pct(sd, ed)})
        top = res[-1] if res else {}
        sampling = {"k": top.get("k"), "exact_mean_fare": top.get("exact_mean_fare"),
                    "sample_mean_fare": top.get("sample_mean_fare"), "error_pct": top.get("error_pct_fare"),
                    "reservoirs": res}

        bits = np.fromiter(self.bits, dtype=np.int64)
        queries = []
        for k in self.dgim_queries:
            exact, est = int(bits[-k:].sum()), self.dgim.query(k)
            queries.append({"k": k, "exact_ones": exact, "estimate": round(est, 1),
                            "abs_error": round(abs(est - exact), 1), "error_pct": pct(est, exact)})
        dgim = {**(queries[len(queries) // 2] if queries else {}),
                "window": self.dgim.N, "buckets": len(self.dgim.buckets), "queries": queries}

        p50, p95 = np.percentile(rt.lat, [50, 95]) if rt.lat else (0, 0)
        wall = (rt.last_end - rt.first) if rt.first and rt.last_end else 0
        span = (rt.max_pts - rt.min_pts) / 1000.0 if rt.min_pts else 0
        return {"run_id": rt.run_id, "report_ts_ms": int(time.time() * 1000), "last_producer_ts_ms": rt.max_pts,
                "events_processed": n,
                "producer_eps": round(rt.events / span, 1) if span > 0 else 0.0,
                "stage2_eps": round(rt.events / wall, 1) if wall > 0 else 0.0,
                "stage2_busy_eps": round(rt.events / rt.busy, 1) if rt.busy > 0 else 0.0,
                "consumer_lag": rt.lag, "kafka_offsets_processed": rt.offsets,
                "latency_p50_ms": int(p50), "latency_p95_ms": int(p95),
                "bloom": bloom, "sampling": sampling, "dgim": dgim,
                "late_events": {k: self.late[k] for k in ("out_of_order", "included", "discarded")},
                "windows_open": len(self.windows), "windows_closed": self.windows_closed}


# ==========================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--slots", type=int)
    ap.add_argument("--fresh", action="store_true", help="delete Spark checkpoint + state snapshot")
    ap.add_argument("--idle-exit", type=int, default=0)
    ap.add_argument("--lateness-min", type=int)
    ap.add_argument("--no-snapshot", action="store_true", help="do not persist/restore state")
    ap.add_argument("--run-id")
    a = ap.parse_args()

    cfg = yaml.safe_load(open(a.config))
    s2, T = cfg["stage2"], cfg["kafka"]["topics"]
    slots = a.slots or cfg["spark"]["local_slots"]
    os.makedirs(cfg["results_dir"], exist_ok=True)
    stats_path = os.path.join(cfg["results_dir"], "stage2_stats.json")
    state_file = s2["state_file"]
    os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)
    if a.fresh:
        reset_state(s2["checkpoint_dir"], state_file)

    if not a.no_snapshot and os.path.exists(state_file):
        state = pickle.load(open(state_file, "rb"))
        print(f"[stage2] RESTORED state: events={state.events}, last_batch_id={state.last_batch_id}, "
              f"open windows={len(state.windows)}", flush=True)
    else:
        state = Stage2State(cfg)
        print("[stage2] starting with EMPTY state", flush=True)
    if a.lateness_min is not None:
        state.lateness_s = a.lateness_min * 60
    print(f"[stage2] window={state.W // 60} min, allowed lateness={state.lateness_s // 60} min", flush=True)

    rt = SimpleNamespace(run_id=a.run_id or cfg["run_id"], lat=deque(maxlen=200000), pending=[], events=0,
                         busy=0.0, lag=None, offsets=None, first=None, last_end=None, min_pts=None, max_pts=None)
    producer = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "linger.ms": 5})
    lock = threading.Lock()

    def publish(msgs):
        for m in msgs:
            producer.produce(T["metrics"], key=rt.run_id.encode(), value=json.dumps(m, separators=(",", ":")).encode())
        producer.flush()                                    # the report is now in taxi.metrics
        now = int(time.time() * 1000)
        for pts in rt.pending:                              # latency = report time - producer_ts_ms
            rt.lat.extend((now - pts).tolist())
        rt.pending.clear()

    def with_snapshot(windows, snap):
        return [{**w, **{k: v for k, v in snap.items() if k not in w}} for w in windows]

    spark = spark_session("stage2-analytics", slots)
    parsed = read_topic(spark, cfg, T["clean"], s2["max_offsets_per_trigger"], SCHEMA).filter(F.col("event_id").isNotNull())

    def process_batch(df, batch_id):
        with lock:
            if batch_id <= state.last_batch_id:
                print(f"[stage2] batch {batch_id} already applied (replay after restart) -> skipped", flush=True)
                return
            t0 = time.time()
            pdf = df.toPandas()
            if pdf.empty:
                return
            pdf = pdf.sort_values(["producer_ts_ms", "event_id"], kind="stable")
            pts = pdf["producer_ts_ms"].to_numpy(np.int64)
            rt.first = rt.first or t0
            rt.events += len(pdf)
            rt.min_pts = int(pts.min()) if rt.min_pts is None else min(rt.min_pts, int(pts.min()))
            rt.max_pts = int(pts.max()) if rt.max_pts is None else max(rt.max_pts, int(pts.max()))

            windows = state.process(pdf)
            rt.busy += time.time() - t0
            rt.last_end = time.time()
            rt.pending.append(pts)

            snap = state.snapshot(rt)
            publish(with_snapshot(windows, snap) + [{"type": "progress", **snap}])   # one report per batch
            state.last_batch_id = batch_id
            if not a.no_snapshot:
                tmp = state_file + ".tmp"
                pickle.dump(state, open(tmp, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
                os.replace(tmp, state_file)
            print(f"[stage2] batch {batch_id}: n={len(pdf)} total={state.events} windows_closed="
                  f"{state.windows_closed} open={len(state.windows)} ({time.time() - t0:.2f}s)", flush=True)

    def write_stats():
        with lock:
            snap = {**state.snapshot(rt), "late_examples": state.late["examples"][:10]}
        json.dump(snap, open(stats_path, "w"), indent=2)
        return snap

    query = (parsed.writeStream.foreachBatch(process_batch).option("checkpointLocation", s2["checkpoint_dir"])
             .trigger(processingTime=f"{int(s2['trigger_seconds'])} seconds").start())
    print(f"[stage2] running with local[{slots}], reading {T['clean']} ...", flush=True)

    end_of_stream = False
    try:
        while query.isActive:
            query.awaitTermination(1)
            lag, done = lag_and_offsets(query.lastProgress)
            if lag is not None:
                rt.lag, rt.offsets = lag, done
            write_stats()
            if a.idle_exit and rt.last_end and time.time() - rt.last_end > a.idle_exit:
                print("[stage2] idle -> end of stream", flush=True)
                end_of_stream = True
                break
    except KeyboardInterrupt:
        print("[stage2] Ctrl+C: stopping WITHOUT flushing windows (state stays in the snapshot)")

    query.stop()
    if end_of_stream:                                       # flush still-open windows + final report
        with lock:
            windows = [state.finalize(state.windows.pop(s), "end_of_stream") for s in sorted(state.windows)]
            rt.lag = rt.lag or 0
            snap = state.snapshot(rt)
            publish(with_snapshot(windows, snap) + [{"type": "progress", "final": True, **snap}])
        print(f"[stage2] flushed {len(windows)} open windows", flush=True)
    print(json.dumps(write_stats(), indent=2))
    spark.stop()


if __name__ == "__main__":
    main()