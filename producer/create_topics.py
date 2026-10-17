"""Create (or recreate) the four Kafka topics.

Usage:
  python producer/create_topics.py --reset
  python producer/create_topics.py --reset --partitions 1
"""
import argparse
import time

import yaml
from confluent_kafka.admin import AdminClient, NewTopic


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--partitions", type=int, default=None,
                    help="override partitions for taxi.raw and taxi.clean")
    ap.add_argument("--reset", action="store_true",
                    help="delete existing topics first (clean run)")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    topics = cfg["kafka"]["topics"]
    n = args.partitions or cfg["partitions"]
    admin = AdminClient({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"]})

    wanted = {
        topics["raw"]: n,
        topics["clean"]: n,
        topics["metrics"]: 1,
        topics["lsh"]: 1,
    }

    existing = set(admin.list_topics(timeout=10).topics)
    if args.reset:
        to_delete = [t for t in wanted if t in existing]
        if to_delete:
            for t, f in admin.delete_topics(to_delete, operation_timeout=30).items():
                try:
                    f.result()
                except Exception as e:
                    print(f"delete {t}: {e}")
            for _ in range(60):          # wait until the broker really forgot them
                if not (set(admin.list_topics(timeout=10).topics) & set(to_delete)):
                    break
                time.sleep(0.5)

    existing = set(admin.list_topics(timeout=10).topics)
    new_topics = [NewTopic(t, num_partitions=p, replication_factor=1)
                  for t, p in wanted.items() if t not in existing]
    if new_topics:
        for t, f in admin.create_topics(new_topics).items():
            try:
                f.result()
                print(f"created {t} ({wanted[t]} partitions)")
            except Exception as e:
                print(f"create {t}: {e}")
    else:
        print("all topics already exist (use --reset for a clean run)")


if __name__ == "__main__":
    main()