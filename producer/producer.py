"""Taxi replay producer -> taxi.raw

- reads taxi data in pickup-time order
- publishes at a controllable rate (pacing on messages actually sent)
- ~2% duplicates: SAME event_id, always emitted AFTER the original, flagged is_resend=true
- ~2% delayed originals (1..max_delay seconds), flagged is_delayed=true
- records actual events/s and partition skew in results/producer_stats.json

Usage:
  python producer/producer.py
  python producer/producer.py --rate 2000 --key-mode round_robin
"""
import argparse
import heapq
import json
import os
import random
import time
from collections import Counter

import pandas as pd
import yaml
from confluent_kafka import Producer


def load_trips(cfg):
    d = cfg["data"]
    path = d["source_file"]
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)

    rename = {
        "tpep_pickup_datetime": "pickup_datetime",
        "lpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    df = df.reset_index(drop=True)
    df["row_id"] = df.index                      # stable id source (assigned before filtering)

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    for c in ("PULocationID", "DOLocationID"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime",
                           "PULocationID", "DOLocationID"])
    df = df[df["dropoff_datetime"] >= df["pickup_datetime"]]
    if d.get("start_date"):
        df = df[df["pickup_datetime"] >= pd.Timestamp(d["start_date"])]
    if d.get("end_date"):
        df = df[df["pickup_datetime"] < pd.Timestamp(d["end_date"])]

    for c in ("passenger_count", "payment_type"):
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in ("trip_distance", "fare_amount"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df = df.sort_values("pickup_datetime", kind="stable")
    if d.get("max_events"):
        df = df.head(int(d["max_events"]))

    prefix = d["event_id_prefix"]
    events = []
    for r in df.itertuples(index=False):
        events.append({
            "event_id": f"{prefix}-row-{int(r.row_id):08d}",
            "event_time": r.pickup_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
            "dropoff_time": r.dropoff_datetime.strftime("%Y-%m-%dT%H:%M:%S"),  # extra field
            "pickup_zone": int(r.PULocationID),
            "dropoff_zone": int(r.DOLocationID),
            "trip_distance": round(float(r.trip_distance), 3),
            "fare_amount": round(float(r.fare_amount), 2),
            "passenger_count": int(r.passenger_count),
            "payment_type": int(r.payment_type),
        })
    return events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--rate", type=float, default=None)
    ap.add_argument("--key-mode", choices=["pickup_zone", "round_robin"], default=None)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    rate = args.rate or cfg["producer_rate"]
    key_mode = args.key_mode or cfg["key_mode"]
    dup_rate = cfg["duplicate_rate"]
    late_rate = cfg["delayed_event_rate"]
    max_delay = cfg["max_delay_seconds"]
    topic = cfg["kafka"]["topics"]["raw"]
    out_dir = cfg["results_dir"]
    os.makedirs(out_dir, exist_ok=True)

    random.seed(args.seed)
    print("Loading data ...")
    events = load_trips(cfg)
    print(f"{len(events)} valid events ready; rate={rate}/s key_mode={key_mode}")

    producer = Producer({
        "bootstrap.servers": cfg["kafka"]["bootstrap_servers"],
        "linger.ms": 5,
        "queue.buffering.max.messages": 500000,
    })
    n_partitions = len(producer.list_topics(topic, timeout=10).topics[topic].partitions)

    part_counts = Counter()
    delivered = {"n": 0, "err": 0}

    def on_delivery(err, msg):
        if err:
            delivered["err"] += 1
        else:
            delivered["n"] += 1
            part_counts[msg.partition()] += 1

    heap = []          # (release_time, seq, event, is_resend, is_delayed)
    seq = 0
    sent = 0
    n_dups = 0
    n_late = 0
    rr = 0
    late_log = open(os.path.join(out_dir, "delayed_events.jsonl"), "w")

    t0 = time.time()
    last_print = t0

    def emit(ev, is_resend=False, is_delayed=False):
        nonlocal sent, rr
        wait = t0 + sent / rate - time.time()      # pace on messages actually sent
        if wait > 0:
            time.sleep(wait)
        msg = dict(ev)
        msg["is_resend"] = is_resend               # ground truth for the Bloom filter
        msg["is_delayed"] = is_delayed
        msg["producer_ts_ms"] = int(time.time() * 1000)
        kwargs = dict(topic=topic, value=json.dumps(msg).encode(), on_delivery=on_delivery)
        if key_mode == "pickup_zone":
            kwargs["key"] = str(ev["pickup_zone"]).encode()
        else:                                      # K2: explicit round-robin
            kwargs["partition"] = rr % n_partitions
            rr += 1
        while True:
            try:
                producer.produce(**kwargs)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        sent += 1

    def drain_due():
        now = time.time()
        while heap and heap[0][0] <= now:
            _, _, ev, resend, delayed = heapq.heappop(heap)
            emit(ev, resend, delayed)

    for ev in events:
        drain_due()

        delay = 0.0
        if random.random() < late_rate:            # delayed original
            delay = random.uniform(1, max_delay)
            seq += 1
            heapq.heappush(heap, (time.time() + delay, seq, ev, False, True))
            n_late += 1
            late_log.write(json.dumps({
                "event_id": ev["event_id"], "event_time": ev["event_time"],
                "delay_s": round(delay, 2),
                "scheduled_wall_ms": int(time.time() * 1000)}) + "\n")
        else:
            emit(ev)

        if random.random() < dup_rate:             # duplicate: same event_id, AFTER original
            seq += 1
            heapq.heappush(heap, (time.time() + delay + random.uniform(0.1, 2.0),
                                  seq, ev, True, False))
            n_dups += 1

        if time.time() - last_print > 5:
            el = time.time() - t0
            print(f"  sent={sent} actual={sent/el:.0f}/s dups={n_dups} late={n_late}")
            last_print = time.time()

    while heap:                                    # flush remaining delayed/duplicate events
        drain_due()
        time.sleep(0.01)
    producer.flush()
    late_log.close()

    elapsed = time.time() - t0
    total = sum(part_counts.values()) or 1
    stats = {
        "run_id": cfg["run_id"],
        "requested_rate": rate,
        "actual_eps": round(sent / elapsed, 2),
        "messages_sent": sent,
        "delivered": delivered["n"],
        "delivery_errors": delivered["err"],
        "unique_events": len(events),
        "duplicates_sent": n_dups,
        "delayed_sent": n_late,
        "elapsed_s": round(elapsed, 2),
        "key_mode": key_mode,
        "partition_counts": dict(sorted(part_counts.items())),
        "max_partition_fraction": round(max(part_counts.values()) / total, 4) if part_counts else None,
        "start_ts_ms": int(t0 * 1000),
        "end_ts_ms": int(time.time() * 1000),
    }
    with open(os.path.join(out_dir, "producer_stats.json"), "w") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()