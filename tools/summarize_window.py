"""Summarise all window messages in taxi.metrics (run after Stage 2 finishes)."""
import json, time
import numpy as np, yaml
from confluent_kafka import Consumer

cfg = yaml.safe_load(open("config.yaml"))
c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"],
              "group.id": f"summary-{time.time()}", "auto.offset.reset": "earliest"})
c.subscribe([cfg["kafka"]["topics"]["metrics"]])
rows, idle = {}, 0
while idle < 5:
    m = c.poll(1.0)
    if m is None or m.error():
        idle += 1
        continue
    idle = 0
    d = json.loads(m.value())
    if d.get("type") == "window":
        rows[d["window_start"]] = d          # keep the last message per window
c.close()

rows = list(rows.values())
print(f"windows: {len(rows)}")
h = [r["hll"]["error_pct"] for r in rows]
print(f"HLL error %: mean {np.mean(h):.2f}, max {np.max(h):.2f}")

ev_abs, en_abs, ev_bias, en_bias = [], [], [], []
for r in rows:
    d = r["diversity"]
    if d["window_events"] <= d["budget_events"] or not d["event_sampling"] or not d["entity_sampling"]:
        continue                              # window fits in the budget: nothing is sampled
    ex = d["exact_fraction"]
    ev_abs.append(d["event_sampling"]["abs_error"]);  ev_bias.append(d["event_sampling"]["estimate"] - ex)
    en_abs.append(d["entity_sampling"]["abs_error"]); en_bias.append(d["entity_sampling"]["estimate"] - ex)
print(f"windows compared: {len(ev_abs)}")
print(f"event  sampling: mean abs error {np.mean(ev_abs):.3f}, mean signed error {np.mean(ev_bias):+.3f}")
print(f"entity sampling: mean abs error {np.mean(en_abs):.3f}, mean signed error {np.mean(en_bias):+.3f}")