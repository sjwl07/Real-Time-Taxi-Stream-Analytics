"""Create (or recreate) the four Kafka topics.

  python producer/create_topics.py --reset                 # clean run, partitions from config
  python producer/create_topics.py --reset --partitions 1
"""
import argparse
import time

import yaml
from confluent_kafka.admin import AdminClient, NewTopic

ap = argparse.ArgumentParser()
ap.add_argument("--config", default="config.yaml")
ap.add_argument("--partitions", type=int, help="override partitions of taxi.raw and taxi.clean")
ap.add_argument("--reset", action="store_true", help="delete the topics first")
a = ap.parse_args()

cfg = yaml.safe_load(open(a.config))
T = cfg["kafka"]["topics"]
n = a.partitions or cfg["partitions"]
admin = AdminClient({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"]})
wanted = {T["raw"]: n, T["clean"]: n, T["metrics"]: 1, T["lsh"]: 1}


def existing():
    return set(admin.list_topics(timeout=10).topics)


if a.reset:
    old = [t for t in wanted if t in existing()]
    if old:
        for t, f in admin.delete_topics(old, operation_timeout=30).items():
            try:
                f.result()
            except Exception as e:
                print(f"delete {t}: {e}")
        for _ in range(60):                       # wait until the broker really forgot them
            if not set(old) & existing():
                break
            time.sleep(0.5)

new = [NewTopic(t, num_partitions=p, replication_factor=1) for t, p in wanted.items() if t not in existing()]
for t, f in (admin.create_topics(new).items() if new else []):
    try:
        f.result()
        print(f"created {t} ({wanted[t]} partitions)")
    except Exception as e:
        print(f"create {t}: {e}")
if not new:
    print("all topics already exist (use --reset for a clean run)")