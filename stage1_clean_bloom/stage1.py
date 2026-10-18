"""Stage 1 (Spark Structured Streaming): taxi.raw -> validate + enrich + Bloom -> taxi.clean

  Kafka source (taxi.raw) -> from_json -> validation + enrichment (Spark SQL)
  -> groupBy(hash(event_id) % B).applyInPandasWithState   Bloom filter = per-shard Spark state
  -> foreachBatch: accepted rows -> Kafka sink (taxi.clean), stats

Duplicates are MARKED (bloom_seen_before), never dropped.

  python stage1_clean_bloom/stage1.py --fresh --idle-exit 30
"""
import argparse
import hashlib
import json
import math
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import lag_and_offsets, read_topic, reset_state, spark_session  # noqa: E402

import yaml  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

RAW_SCHEMA = ("event_id string, event_time string, dropoff_time string, pickup_zone long, dropoff_zone long, "
              "trip_distance double, fare_amount double, passenger_count long, payment_type long, "
              "producer_ts_ms long, is_resend boolean, is_delayed boolean")
OUT_DDL = RAW_SCHEMA + (", route_key string, trip_duration_min double, slow_trip_bit long, "
                        "reject_reason string, bucket int, bloom_seen_before boolean")
OUT_COLS = [c.split()[0] for c in OUT_DDL.split(", ")]


def bloom_params(n, p):
    """Optimal bit-array size m and number of hash functions k for n items and FP target p."""
    m = math.ceil(-n * math.log(p) / math.log(2) ** 2)
    return m, max(1, round(m / n * math.log(2)))


def make_bloom_func(m, k):
    """Stateful function for one shard. State = the shard's Bloom bit array (bytes)."""
    import pandas as pd

    def func(key, pdf_iter, state):
        bits = bytearray(state.get[0]) if state.exists else bytearray((m + 7) // 8)
        pdf = pd.concat(list(pdf_iter), ignore_index=True)
        pdf = pdf.sort_values("producer_ts_ms", kind="stable").reset_index(drop=True)  # original before resend
        seen = []
        for eid, bad in zip(pdf["event_id"], pdf["reject_reason"].notna()):
            if bad:                                    # invalid rows never touch the filter
                seen.append(False)
                continue
            d = hashlib.blake2b(eid.encode(), digest_size=16).digest()
            h1, h2 = int.from_bytes(d[:8], "little"), int.from_bytes(d[8:], "little") | 1
            idx = [(h1 + i * h2) % m for i in range(k)]                  # double hashing -> k positions
            seen.append(all(bits[j >> 3] >> (j & 7) & 1 for j in idx))   # membership test BEFORE insert
            for j in idx:
                bits[j >> 3] |= 1 << (j & 7)
        pdf["bloom_seen_before"] = seen
        state.update((bytes(bits),))
        yield pdf[OUT_COLS]

    return func


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--slots", type=int)
    ap.add_argument("--fresh", action="store_true", help="delete the checkpoint (clean run)")
    ap.add_argument("--idle-exit", type=int, default=0, help="stop after N s without data (0 = never)")
    a = ap.parse_args()

    cfg = yaml.safe_load(open(a.config))
    s1, T = cfg["stage1"], cfg["kafka"]["topics"]
    slots = a.slots or cfg["spark"]["local_slots"]
    nb = int(s1["state_buckets"])
    os.makedirs(cfg["results_dir"], exist_ok=True)
    stats_path = os.path.join(cfg["results_dir"], "stage1_stats.json")
    if a.fresh:
        reset_state(s1["checkpoint_dir"])

    m, k = bloom_params(math.ceil(cfg["bloom_expected_items"] / nb), cfg["bloom_false_positive_target"])
    bloom_cfg = {"bit_array_size": m * nb, "num_hashes": k, "state_buckets": nb,
                 "expected_items": cfg["bloom_expected_items"], "fp_target": cfg["bloom_false_positive_target"]}
    print(f"[stage1] Bloom: {nb} shards x {m} bits = {m * nb} bits, k={k}", flush=True)

    spark = spark_session("stage1-clean-bloom", slots, shuffle_partitions=nb)
    p = read_topic(spark, cfg, T["raw"], s1["max_offsets_per_trigger"], RAW_SCHEMA)

    # ---------------- validation + enrichment (Spark SQL) ----------------
    pick, drop = F.to_timestamp("event_time"), F.to_timestamp("dropoff_time")
    dur = (F.unix_timestamp(drop) - F.unix_timestamp(pick)) / 60.0
    c = F.col
    reason = (F.when(c("event_id").isNull() | pick.isNull() | drop.isNull() | c("pickup_zone").isNull()
                     | c("dropoff_zone").isNull() | c("fare_amount").isNull() | c("trip_distance").isNull()
                     | c("producer_ts_ms").isNull(), "malformed")
              .when((c("pickup_zone") <= 0) | (c("dropoff_zone") <= 0), "bad_zone")
              .when((dur < 0) | (dur > int(s1["max_duration_min"])), "bad_duration")
              .when((c("fare_amount") < 0) | (c("trip_distance") < 0), "negative_value")
              .when(c("trip_distance") > float(s1["max_distance_miles"]), "bad_distance"))
    defaults = {"event_id": "", "event_time": "", "dropoff_time": "", "pickup_zone": 0, "dropoff_zone": 0,
                "trip_distance": 0.0, "fare_amount": 0.0, "passenger_count": 0, "payment_type": 0,
                "producer_ts_ms": 0, "is_resend": False, "is_delayed": False}
    enriched = (p.withColumn("_dur", dur).withColumn("reject_reason", reason)
                .fillna(defaults)
                .withColumn("route_key", F.concat_ws("-", c("pickup_zone").cast("string"),
                                                     c("dropoff_zone").cast("string")))
                .withColumn("trip_duration_min", F.round(F.coalesce(c("_dur"), F.lit(0.0)), 3))
                .withColumn("slow_trip_bit", F.when(c("_dur") > 30, 1).otherwise(0).cast("long"))
                .withColumn("bucket", F.pmod(F.hash("event_id"), F.lit(nb)).cast("int"))
                .drop("_dur"))

    # ---------------- Bloom filter as Spark state ----------------
    flagged = (enriched.groupBy("bucket")
               .applyInPandasWithState(make_bloom_func(m, k), outputStructType=OUT_DDL,
                                      stateStructType="bits binary", outputMode="append",
                                      timeoutConf="NoTimeout"))

    S = {"events_in": 0, "accepted": 0, "rejected": 0, "reject_reasons": {}, "tp": 0, "fp": 0, "tn": 0,
         "fn": 0, "fp_examples": [], "batches": 0, "first": None, "last_end": None, "lag": None, "offsets": None}
    lock = threading.Lock()
    bootstrap, key_mode = cfg["kafka"]["bootstrap_servers"], cfg["key_mode"]

    def process_batch(df, batch_id):
        t0 = time.time()
        df.persist(StorageLevel.MEMORY_AND_DISK)
        ok, seen, rs = F.col("reject_reason").isNull(), F.col("bloom_seen_before"), F.col("is_resend")

        def cnt(cond):
            return F.sum(F.when(cond, 1).otherwise(0))

        r = df.agg(F.count("*").alias("n"), cnt(ok).alias("ok"), cnt(ok & seen & rs).alias("tp"),
                   cnt(ok & seen & ~rs).alias("fp"), cnt(ok & ~seen & rs).alias("fn"),
                   cnt(ok & ~seen & ~rs).alias("tn")).collect()[0]
        if not r["n"]:
            df.unpersist()
            return
        rejected = df.filter(~ok).groupBy("reject_reason").count().collect()
        fp_ex = ([x["event_id"] for x in df.filter(ok & seen & ~rs).select("event_id").limit(5).collect()]
                 if r["fp"] else [])

        good = (df.filter(ok).drop("reject_reason", "bucket")
                  .withColumn("stage1_ts_ms", F.lit(int(time.time() * 1000)).cast("long")))   # emit time
        key = F.col("pickup_zone").cast("string") if key_mode == "pickup_zone" else F.lit(None).cast("string")
        (good.select(key.alias("key"), F.to_json(F.struct(*good.columns)).alias("value"))
             .write.format("kafka").option("kafka.bootstrap.servers", bootstrap)
             .option("topic", T["clean"]).save())
        df.unpersist()

        with lock:
            S["first"] = S["first"] or t0
            S["last_end"] = time.time()
            S["batches"] += 1
            S["events_in"] += r["n"]
            S["accepted"] += int(r["ok"] or 0)
            for k_ in ("tp", "fp", "fn", "tn"):
                S[k_] += int(r[k_] or 0)
            for x in rejected:
                S["rejected"] += x["count"]
                S["reject_reasons"][x["reject_reason"]] = S["reject_reasons"].get(x["reject_reason"], 0) + x["count"]
            S["fp_examples"] = (S["fp_examples"] + fp_ex)[:20]
        print(f"[stage1] batch {batch_id}: in={r['n']} ok={r['ok']} fp={r['fp']} ({time.time() - t0:.2f}s)",
              flush=True)

    def write_stats():
        with lock:
            el = (S["last_end"] - S["first"]) if S["first"] else 0
            new_ids, dup_ids = S["fp"] + S["tn"], S["tp"] + S["fn"]
            st = {"events_in": S["events_in"], "accepted": S["accepted"], "rejected": S["rejected"],
                  "reject_reasons": S["reject_reasons"], "batches": S["batches"], "elapsed_s": round(el, 2),
                  "stage1_eps": round(S["events_in"] / el, 2) if el > 0 else 0,
                  "kafka_lag_taxi_raw": S["lag"], "kafka_offsets_processed": S["offsets"],
                  "bloom": {**bloom_cfg, "inserted_ids": new_ids,
                            **{k_: S[k_] for k_ in ("tp", "fp", "tn", "fn")},
                            "fp_rate": round(S["fp"] / new_ids, 6) if new_ids else 0.0,
                            "fn_rate": round(S["fn"] / dup_ids, 6) if dup_ids else 0.0,
                            "fp_examples": S["fp_examples"][:5]}}
        json.dump(st, open(stats_path, "w"), indent=2)
        return st

    query = (flagged.writeStream.foreachBatch(process_batch)
             .option("checkpointLocation", s1["checkpoint_dir"])
             .trigger(processingTime=f"{int(s1['trigger_seconds'])} seconds").start())
    print(f"[stage1] running with local[{slots}], waiting for data on {T['raw']} ...", flush=True)

    try:
        while query.isActive:
            query.awaitTermination(1)
            lag, done = lag_and_offsets(query.lastProgress)
            if lag is not None:
                S["lag"], S["offsets"] = lag, done
            write_stats()
            if a.idle_exit and S["last_end"] and time.time() - S["last_end"] > a.idle_exit:
                print("[stage1] idle, stopping", flush=True)
                break
    except KeyboardInterrupt:
        print("[stage1] Ctrl+C, stopping")
    query.stop()
    print(json.dumps(write_stats(), indent=2))
    spark.stop()


if __name__ == "__main__":
    main()