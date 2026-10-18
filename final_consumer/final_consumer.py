"""Final consumer: taxi.metrics + taxi.lsh -> console + results.jsonl

One JSON record per 60-min window (Stage 2 window report joined with the Stage 3 window report
on window_start), plus one final summary record of the run.

  python final_consumer/final_consumer.py --idle-exit 30
  python final_consumer/final_consumer.py --run-id P3 --out results/runs/P3/results.jsonl
"""
import argparse
import json
import os
import time

import numpy as np
import yaml
from confluent_kafka import Consumer

ap = argparse.ArgumentParser()
ap.add_argument("--config", default="config.yaml")
ap.add_argument("--run-id", help="only messages of this run_id (default: run_id in config)")
ap.add_argument("--idle-exit", type=int, default=30, help="stop after N s without messages")
ap.add_argument("--out")
a = ap.parse_args()

cfg = yaml.safe_load(open(a.config))
T = cfg["kafka"]["topics"]
run_id = a.run_id or cfg["run_id"]
out_path = a.out or os.path.join(cfg["results_dir"], "results.jsonl")
os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "group.id": f"final-{time.time()}",
              "auto.offset.reset": "earliest"})
c.subscribe([T["metrics"], T["lsh"]])
out = open(out_path, "a")
pending, e2e, final = {}, [], None


def pick(d, keys):
    return {k: d.get(k) for k in keys} if d else None


def emit(w, m, l):
    """Standardized record (Section 9 fields first, extra fields after)."""
    rec = {k: m.get(k) for k in ("run_id", "window_start", "window_end", "events_processed", "producer_eps",
                                 "stage2_eps", "consumer_lag", "latency_p50_ms", "latency_p95_ms")}
    rec["bloom"] = pick(m["bloom"], ("fp_rate", "fn_rate"))
    rec["sampling"] = pick(m["sampling"], ("exact_mean_fare", "sample_mean_fare", "error_pct"))
    rec["hll"] = pick(m["hll"], ("exact_routes", "estimate", "error_pct"))
    rec["dgim"] = pick(m["dgim"], ("k", "exact_ones", "estimate", "error_pct"))
    rec["lsh"] = l["lsh"] if l else None
    rec.update(diversity=m.get("diversity"), lsh_configs=l["configs"] if l else None, closed_by=m.get("closed_by"))
    if m.get("last_producer_ts_ms"):
        rec["e2e_report_latency_ms"] = int(time.time() * 1000) - m["last_producer_ts_ms"]
        e2e.append(rec["e2e_report_latency_ms"])
    out.write(json.dumps(rec) + "\n")
    out.flush()
    ls = rec["lsh"] or {}
    print(f"[{w}] events={rec['events_processed']} lag={rec['consumer_lag']} p50={rec['latency_p50_ms']}ms "
          f"HLL err={rec['hll']['error_pct']}% DGIM err={rec['dgim']['error_pct']}% "
          f"fare err={rec['sampling']['error_pct']}% LSH cand={ls.get('candidates')} recall={ls.get('recall')}",
          flush=True)


idle = 0
while idle < a.idle_exit:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        idle += 1
        continue
    idle = 0
    d = json.loads(msg.value())
    if d.get("run_id") != run_id:
        continue
    if d.get("type") == "progress" and d.get("final"):
        final = d
    elif d.get("type") in ("window", "lsh_window"):
        slot = pending.setdefault(d["window_start"], {})
        slot["m" if d["type"] == "window" else "l"] = d     # a re-sent window (restart) overwrites
        if "m" in slot and "l" in slot:
            emit(d["window_start"], slot["m"], slot["l"])
            del pending[d["window_start"]]
c.close()

for w, slot in sorted(pending.items()):                    # window with only the Stage 2 side
    if "m" in slot:
        emit(w, slot["m"], slot.get("l"))

if final:
    summary = {"run_id": run_id, "type": "final_summary",
               **{k: final.get(k) for k in ("events_processed", "producer_eps", "stage2_eps", "stage2_busy_eps",
                                            "consumer_lag", "latency_p50_ms", "latency_p95_ms", "bloom",
                                            "sampling", "dgim", "late_events")}}
    if e2e:
        summary["e2e_report_latency_p50_ms"], summary["e2e_report_latency_p95_ms"] = \
            [int(x) for x in np.percentile(e2e, [50, 95])]
    out.write(json.dumps(summary) + "\n")
    print("FINAL:", json.dumps({k: summary[k] for k in ("events_processed", "stage2_eps", "latency_p50_ms",
                                                        "latency_p95_ms")}), flush=True)
out.close()
print(f"written to {out_path}")