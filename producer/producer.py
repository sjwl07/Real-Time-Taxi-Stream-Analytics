"""Taxi replay producer -> taxi.raw

- reads taxi trips in pickup-time order and publishes at a controllable rate
- key = pickup_zone (baseline) or explicit round-robin partitions
- ~2% duplicates: SAME event_id, emitted after the original, flagged is_resend=true (ground truth)
- ~2% delayed originals (1..max_delay seconds), flagged is_delayed=true
- writes actual events/s and partition skew to results/producer_stats.json

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

RENAME = {"tpep_pickup_datetime": "pickup_datetime", "lpep_pickup_datetime": "pickup_datetime",
          "tpep_dropoff_datetime": "dropoff_datetime", "lpep_dropoff_datetime": "dropoff_datetime"}


def load_trips(cfg):
    d = cfg["data"]
    path = d["source_file"]
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
    df = df.rename(columns={k: v for k, v in RENAME.items() if k in df.columns}).reset_index(drop=True)
    df["row_id"] = df.index                                   # stable event_id source
    for c in ("pickup_datetime", "dropoff_datetime"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in ("PULocationID", "DOLocationID"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # remove rows with missing zones or invalid timestamps
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID"])
    df = df[df["dropoff_datetime"] >= df["pickup_datetime"]]
    if d.get("start_date"):
        df = df[df["pickup_datetime"] >= pd.Timestamp(d["start_date"])]
    if d.get("end_date"):
        df = df[df["pickup_datetime"] < pd.Timestamp(d["end_date"])]
    for c in ("passenger_count", "payment_type"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int) if c in df else 0
    for c in ("trip_distance", "fare_amount"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df = df.sort_values("pickup_datetime", kind="stable")       # timestamp order
    if d.get("max_events"):
        df = df.head(int(d["max_events"]))

    fmt = "%Y-%m-%dT%H:%M:%S"
    return [{"event_id": f"{d['event_id_prefix']}-row-{int(r.row_id):08d}",
             "event_time": r.pickup_datetime.strftime(fmt),
             "dropoff_time": r.dropoff_datetime.strftime(fmt),          # extra field (trip duration)
             "pickup_zone": int(r.PULocationID), "dropoff_zone": int(r.DOLocationID),
             "trip_distance": round(float(r.trip_distance), 3), "fare_amount": round(float(r.fare_amount), 2),
             "passenger_count": int(r.passenger_count), "payment_type": int(r.payment_type)}
            for r in df.itertuples(index=False)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--rate", type=float)
    ap.add_argument("--key-mode", choices=["pickup_zone", "round_robin"])
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    cfg = yaml.safe_load(open(a.config))
    rate = a.rate or cfg["producer_rate"]
    key_mode = a.key_mode or cfg["key_mode"]
    topic, out_dir = cfg["kafka"]["topics"]["raw"], cfg["results_dir"]
    os.makedirs(out_dir, exist_ok=True)
    random.seed(a.seed)

    print("Loading data ...")
    events = load_trips(cfg)
    print(f"{len(events)} valid events; rate={rate}/s key_mode={key_mode}", flush=True)

    p = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "linger.ms": 5,
                  "queue.buffering.max.messages": 500000})
    n_parts = len(p.list_topics(topic, timeout=10).topics[topic].partitions)
    part_counts, errors = Counter(), [0]

    def on_delivery(err, msg):
        if err:
            errors[0] += 1
        else:
            part_counts[msg.partition()] += 1

    heap, seq, sent, n_dup, n_late = [], 0, 0, 0, 0
    late_log = open(os.path.join(out_dir, "delayed_events.jsonl"), "w")
    t0 = last_print = time.time()

    def emit(ev, resend=False, delayed=False):
        nonlocal sent
        wait = t0 + sent / rate - time.time()                # pace on messages actually sent
        if wait > 0:
            time.sleep(wait)
        msg = {**ev, "is_resend": resend, "is_delayed": delayed, "producer_ts_ms": int(time.time() * 1000)}
        kw = {"topic": topic, "value": json.dumps(msg).encode(), "on_delivery": on_delivery}
        if key_mode == "pickup_zone":
            kw["key"] = str(ev["pickup_zone"]).encode()
        else:
            kw["partition"] = sent % n_parts                 # K2: round-robin
        while True:
            try:
                p.produce(**kw)
                break
            except BufferError:
                p.poll(0.1)
        p.poll(0)
        sent += 1

    def drain_due():
        while heap and heap[0][0] <= time.time():
            _, _, ev, resend, delayed = heapq.heappop(heap)
            emit(ev, resend, delayed)

    for ev in events:
        drain_due()
        delay = 0.0
        if random.random() < cfg["delayed_event_rate"]:      # delayed original
            delay = random.uniform(1, cfg["max_delay_seconds"])
            seq += 1
            heapq.heappush(heap, (time.time() + delay, seq, ev, False, True))
            n_late += 1
            late_log.write(json.dumps({"event_id": ev["event_id"], "event_time": ev["event_time"],
                                       "delay_s": round(delay, 2)}) + "\n")
        else:
            emit(ev)
        if random.random() < cfg["duplicate_rate"]:          # duplicate: same event_id, after the original
            seq += 1
            heapq.heappush(heap, (time.time() + delay + random.uniform(0.1, 2.0), seq, ev, True, False))
            n_dup += 1
        if time.time() - last_print > 5:
            print(f"  sent={sent} actual={sent / (time.time() - t0):.0f}/s dups={n_dup} late={n_late}", flush=True)
            last_print = time.time()

    while heap:
        drain_due()
        time.sleep(0.01)
    p.flush()
    late_log.close()

    elapsed = time.time() - t0
    total = sum(part_counts.values()) or 1
    stats = {"run_id": cfg["run_id"], "requested_rate": rate, "actual_eps": round(sent / elapsed, 2),
             "messages_sent": sent, "delivered": sum(part_counts.values()), "delivery_errors": errors[0],
             "unique_events": len(events), "duplicates_sent": n_dup, "delayed_sent": n_late,
             "elapsed_s": round(elapsed, 2), "key_mode": key_mode,
             "partition_counts": dict(sorted(part_counts.items())),
             "max_partition_fraction": round(max(part_counts.values()) / total, 4) if part_counts else None}
    json.dump(stats, open(os.path.join(out_dir, "producer_stats.json"), "w"), indent=2)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()