"""Live message counts of the four topics + lag of Stage 1 and Stage 2.

  python tools/topic_counts.py --watch
"""
import argparse
import json
import time

import yaml
from confluent_kafka import Consumer, TopicPartition

ap = argparse.ArgumentParser()
ap.add_argument("--watch", action="store_true")
ap.add_argument("--config", default="config.yaml")
a = ap.parse_args()
cfg = yaml.safe_load(open(a.config))
T = cfg["kafka"]["topics"]
c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "group.id": f"peek-{time.time()}"})


def ends(topic):
    try:
        parts = sorted(c.list_topics(topic, timeout=5).topics[topic].partitions)
        return [c.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1] for p in parts]
    except Exception:
        return []


def processed(f):
    try:
        return json.load(open(f"results/{f}")).get("kafka_offsets_processed") or 0
    except Exception:
        return 0


while True:
    e = {k: ends(v) for k, v in T.items()}
    print(time.strftime("%H:%M:%S"), " | ".join(f"{T[k]}={sum(e[k])}" for k in ("raw", "clean", "metrics", "lsh")),
          f"| raw per partition={e['raw']}",
          f"| lag raw={sum(e['raw']) - processed('stage1_stats.json')}",
          f"lag clean={sum(e['clean']) - processed('stage2_stats.json')}", flush=True)
    if not a.watch:
        break
    time.sleep(2)
