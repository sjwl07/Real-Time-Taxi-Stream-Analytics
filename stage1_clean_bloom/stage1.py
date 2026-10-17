"""Stage 1 (Spark Structured Streaming): taxi.raw -> validate + enrich + Bloom -> taxi.clean

Pipeline:
  Kafka source (taxi.raw)
    -> from_json, validation, enrichment (Spark SQL)
    -> groupBy(hash(event_id) % B).applyInPandasWithState   (Bloom filter = per-bucket state)
    -> foreachBatch: write valid rows to Kafka sink (taxi.clean) + compute stats

Usage:
  python stage1_clean_bloom/stage1.py --fresh --idle-exit 30
  python stage1_clean_bloom/stage1.py --slots 4
"""
import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import threading
import time

import yaml
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType)

# make the Spark Python workers use the same interpreter as this script
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# --------------------------------------------------------------------------
# Bloom filter helpers (module level so they ship to the Spark workers)
# --------------------------------------------------------------------------
def bloom_params(expected_items, fp_target):
    m = int(math.ceil(-expected_items * math.log(fp_target) / (math.log(2) ** 2)))
    k = max(1, int(round(m / expected_items * math.log(2))))
    return m, k


def bloom_indexes(item, m, k):
    d = hashlib.blake2b(item.encode(), digest_size=16).digest()
    h1 = int.from_bytes(d[:8], "little")
    h2 = int.from_bytes(d[8:], "little") | 1
    return [(h1 + i * h2) % m for i in range(k)]            # double hashing


def make_bloom_func(m, k):
    """Returns the stateful function. State per bucket = the Bloom bit array (bytes)."""
    import pandas as pd

    def func(key, pdf_iter, state):
        if state.exists:
            bits = bytearray(state.get[0])
        else:
            bits = bytearray((m + 7) // 8)

        pdf = pd.concat(list(pdf_iter), ignore_index=True)
        # process in producer order so an original always precedes its resend
        pdf = pdf.sort_values("producer_ts_ms", kind="stable").reset_index(drop=True)

        seen_flags = []
        for eid, reason in zip(pdf["event_id"].tolist(), pdf["reject_reason"].tolist()):
            if reason is not None and reason == reason:      # invalid row: never touches the filter
                seen_flags.append(False)
                continue
            idx = bloom_indexes(eid, m, k)
            seen = all(bits[i >> 3] & (1 << (i & 7)) for i in idx)
            for i in idx:
                bits[i >> 3] |= 1 << (i & 7)
            seen_flags.append(seen)

        pdf["bloom_seen_before"] = seen_flags
        pdf["stage1_ts_ms"] = int(time.time() * 1000)
        state.update((bytes(bits),))
        yield pdf[OUT_COLS]

    return func


# --------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------
RAW_SCHEMA = StructType([
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
])

OUT_DDL = (
    "event_id string, event_time string, dropoff_time string, "
    "pickup_zone long, dropoff_zone long, trip_distance double, fare_amount double, "
    "passenger_count long, payment_type long, producer_ts_ms long, "
    "is_resend boolean, is_delayed boolean, "
    "route_key string, trip_duration_min double, slow_trip_bit long, "
    "reject_reason string, bucket int, "
    "bloom_seen_before boolean, stage1_ts_ms long"
)
OUT_COLS = [c.split()[0] for c in OUT_DDL.split(", ")]


# --------------------------------------------------------------------------
# Shared driver-side statistics
# --------------------------------------------------------------------------
S = {
    "events_in": 0, "accepted": 0, "rejected": 0, "reject_reasons": {},
    "tp": 0, "fp": 0, "tn": 0, "fn": 0, "fp_examples": [],
    "batches": 0, "first_ts": None, "last_data_ts": None, "last_end_ts": None,
    "lag": None,
}
LOCK = threading.Lock()


def write_stats(path, bloom_cfg):
    with LOCK:
        elapsed = (S["last_end_ts"] - S["first_ts"]) if S["first_ts"] and S["last_end_ts"] else 0
        new_ids = S["fp"] + S["tn"]          # accepted events whose id was truly new
        dup_ids = S["tp"] + S["fn"]          # accepted events that were truly resent
        stats = {
            "events_in": S["events_in"], "accepted": S["accepted"], "rejected": S["rejected"],
            "reject_reasons": S["reject_reasons"],
            "batches": S["batches"],
            "elapsed_s": round(elapsed, 2),
            "stage1_eps": round(S["events_in"] / elapsed, 2) if elapsed > 0 else 0,
            "kafka_lag_taxi_raw": S["lag"],
            "bloom": {
                "inserted_ids": new_ids,
                "bit_array_size": bloom_cfg["m_total"],
                "num_hashes": bloom_cfg["k"],
                "state_buckets": bloom_cfg["buckets"],
                "expected_items": bloom_cfg["expected"],
                "fp_target": bloom_cfg["fp_target"],
                "tp": S["tp"], "fp": S["fp"], "tn": S["tn"], "fn": S["fn"],
                "fp_rate": round(S["fp"] / new_ids, 6) if new_ids else 0.0,
                "fn_rate": round(S["fn"] / dup_ids, 6) if dup_ids else 0.0,
                "fp_examples": S["fp_examples"][:5],
            },
        }
        with open(path, "w") as f:
            json.dump(stats, f, indent=2)
    return stats


def lag_from_progress(p):
    """Kafka lag = latest offset in the topic - offset Spark has processed."""
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


def spark_jars():
    import glob
    jar_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jars")
    jars = sorted(glob.glob(os.path.abspath(jar_dir) + "/*.jar"))
    if not jars:
        sys.exit("ERROR: no jars in ./jars - run Part F first")
    return ",".join(jars)

# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--slots", type=int, default=None, help="Spark local[N] processing slots")
    ap.add_argument("--fresh", action="store_true", help="delete the checkpoint (clean run)")
    ap.add_argument("--idle-exit", type=int, default=0,
                    help="stop after N seconds without new data (0 = run until Ctrl+C)")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    s1 = cfg["stage1"]
    topics = cfg["kafka"]["topics"]
    bootstrap = cfg["kafka"]["bootstrap_servers"]
    slots = args.slots or cfg["spark"]["local_slots"]
    buckets = int(s1["state_buckets"])
    key_mode = cfg["key_mode"]
    out_dir = cfg["results_dir"]
    os.makedirs(out_dir, exist_ok=True)
    stats_path = os.path.join(out_dir, "stage1_stats.json")

    ckpt = s1["checkpoint_dir"]
    if args.fresh and os.path.exists(ckpt):
        shutil.rmtree(ckpt)

    # Bloom sizing: total capacity split across the state buckets
    per_bucket = int(math.ceil(cfg["bloom_expected_items"] / buckets))
    m, k = bloom_params(per_bucket, cfg["bloom_false_positive_target"])
    bloom_cfg = {"m_total": m * buckets, "k": k, "buckets": buckets,
                 "expected": cfg["bloom_expected_items"],
                 "fp_target": cfg["bloom_false_positive_target"]}
    print(f"Bloom: {buckets} buckets x {m} bits (total {m*buckets} bits), k={k}")

    spark = (SparkSession.builder.appName("stage1-clean-bloom")
             .master(f"local[{slots}]")
             .config("spark.jars", spark_jars())
             .config("spark.sql.shuffle.partitions", str(buckets))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---------------- source ----------------
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topics["raw"])
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", int(s1["max_offsets_per_trigger"]))
           .option("failOnDataLoss", "false")
           .load())

    parsed = raw.select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("e")).select("e.*")

    # ---------------- validation + enrichment (Spark SQL) ----------------
    pick = F.to_timestamp("event_time")
    drop = F.to_timestamp("dropoff_time")
    dur = (F.unix_timestamp(drop) - F.unix_timestamp(pick)) / 60.0

    reason = (
        F.when(F.col("event_id").isNull() | pick.isNull() | drop.isNull()
               | F.col("pickup_zone").isNull() | F.col("dropoff_zone").isNull()
               | F.col("fare_amount").isNull() | F.col("trip_distance").isNull()
               | F.col("producer_ts_ms").isNull(), "malformed")
        .when((F.col("pickup_zone") <= 0) | (F.col("dropoff_zone") <= 0), "bad_zone")
        .when((dur < 0) | (dur > int(s1["max_duration_min"])), "bad_duration")
        .when((F.col("fare_amount") < 0) | (F.col("trip_distance") < 0), "negative_value")
        .when(F.col("trip_distance") > float(s1["max_distance_miles"]), "bad_distance")
    )
    p = parsed.withColumn("_dur", dur).withColumn("reject_reason", reason)

    def nz(col, default):
        return F.coalesce(F.col(col), F.lit(default))

    pu = nz("pickup_zone", 0).cast("long")
    do = nz("dropoff_zone", 0).cast("long")
    enriched = p.select(
        nz("event_id", "").alias("event_id"),
        nz("event_time", "").alias("event_time"),
        nz("dropoff_time", "").alias("dropoff_time"),
        pu.alias("pickup_zone"),
        do.alias("dropoff_zone"),
        nz("trip_distance", 0.0).cast("double").alias("trip_distance"),
        nz("fare_amount", 0.0).cast("double").alias("fare_amount"),
        nz("passenger_count", 0).cast("long").alias("passenger_count"),
        nz("payment_type", 0).cast("long").alias("payment_type"),
        nz("producer_ts_ms", 0).cast("long").alias("producer_ts_ms"),
        nz("is_resend", False).cast("boolean").alias("is_resend"),
        nz("is_delayed", False).cast("boolean").alias("is_delayed"),
        F.concat_ws("-", pu.cast("string"), do.cast("string")).alias("route_key"),
        F.round(F.coalesce(F.col("_dur"), F.lit(0.0)), 3).alias("trip_duration_min"),
        F.when(F.col("_dur") > 30, 1).otherwise(0).cast("long").alias("slow_trip_bit"),
        F.col("reject_reason"),
        F.pmod(F.hash(nz("event_id", "")), F.lit(buckets)).cast("int").alias("bucket"),
    )

    # ---------------- Bloom filter as Spark state ----------------
    flagged = (enriched.groupBy("bucket")
               .applyInPandasWithState(make_bloom_func(m, k),
                                      outputStructType=OUT_DDL,
                                      stateStructType="bits binary",
                                      outputMode="append",
                                      timeoutConf="NoTimeout"))

    # ---------------- sink: Kafka (inside foreachBatch) ----------------
    def process_batch(df, batch_id):
        t_begin = time.time()
        df.persist(StorageLevel.MEMORY_AND_DISK)
        ok = F.col("reject_reason").isNull()
        seen, rs = F.col("bloom_seen_before"), F.col("is_resend")

        def cnt(cond):
            return F.sum(F.when(cond, 1).otherwise(0))

        a = df.agg(F.count("*").alias("n"),
                   cnt(ok).alias("ok"),
                   cnt(ok & seen & rs).alias("tp"),
                   cnt(ok & seen & ~rs).alias("fp"),
                   cnt(ok & ~seen & rs).alias("fn"),
                   cnt(ok & ~seen & ~rs).alias("tn")).collect()[0]
        n = a["n"]
        if not n:
            df.unpersist()
            return

        rejected = df.filter(~ok).groupBy("reject_reason").count().collect()
        fp_ex = []
        if a["fp"]:
            fp_ex = [r["event_id"] for r in
                     df.filter(ok & seen & ~rs).select("event_id").limit(5).collect()]

        good = df.filter(ok).drop("reject_reason", "bucket")
        key = (F.col("pickup_zone").cast("string") if key_mode == "pickup_zone"
               else F.lit(None).cast("string"))
        (good.select(key.alias("key"), F.to_json(F.struct(*good.columns)).alias("value"))
             .write.format("kafka")
             .option("kafka.bootstrap.servers", bootstrap)
             .option("topic", topics["clean"])
             .save())
        df.unpersist()

        now = time.time()
        with LOCK:
            if S["first_ts"] is None:
                S["first_ts"] = t_begin
            S["last_data_ts"] = now
            S["last_end_ts"] = now
            S["batches"] += 1
            S["events_in"] += n
            S["accepted"] += int(a["ok"] or 0)
            S["tp"] += int(a["tp"] or 0); S["fp"] += int(a["fp"] or 0)
            S["fn"] += int(a["fn"] or 0); S["tn"] += int(a["tn"] or 0)
            for r in rejected:
                S["rejected"] += r["count"]
                S["reject_reasons"][r["reject_reason"]] = \
                    S["reject_reasons"].get(r["reject_reason"], 0) + r["count"]
            for e in fp_ex:
                if len(S["fp_examples"]) < 20:
                    S["fp_examples"].append(e)
        print(f"[stage1] batch {batch_id}: in={n} ok={a['ok']} fp={a['fp']} "
              f"({time.time()-t_begin:.2f}s)")

    query = (flagged.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", ckpt)
             .trigger(processingTime=f"{int(s1['trigger_seconds'])} seconds")
             .start())

    print(f"[stage1] running with local[{slots}], waiting for data on {topics['raw']} ...")
    last_write = time.time()
    try:
        while query.isActive:
            query.awaitTermination(2)
            lag = lag_from_progress(query.lastProgress)
            if lag is not None:
                S["lag"] = lag
            if time.time() - last_write > 5:
                write_stats(stats_path, bloom_cfg)
                last_write = time.time()
            if (args.idle_exit and S["last_data_ts"]
                    and time.time() - S["last_data_ts"] > args.idle_exit):
                print("[stage1] idle, stopping")
                break
    except KeyboardInterrupt:
        print("[stage1] Ctrl+C, stopping")

    query.stop()
    final = write_stats(stats_path, bloom_cfg)
    print(json.dumps(final, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()