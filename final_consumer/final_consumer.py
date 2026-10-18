"""Final consumer: taxi.metrics + taxi.lsh -> console + results/results.jsonl

One output record per 60-min window (metrics window message joined with LSH window message),
plus one final summary record.

Usage:
  python final_consumer/final_consumer.py --idle-exit 10
  python final_consumer/final_consumer.py --run-id P3-medium --idle-exit 10
"""
import argparse, json, os, time
import numpy as np
import yaml
from confluent_kafka import Consumer

ap = argparse.ArgumentParser()
ap.add_argument("--config", default="config.yaml")
ap.add_argument("--run-id", default=None, help="only use messages of this run_id")
ap.add_argument("--idle-exit", type=int, default=10)
ap.add_argument("--out", default=None)
args = ap.parse_args()

cfg = yaml.safe_load(open(args.config))
topics = cfg["kafka"]["topics"]
run_id = args.run_id or cfg["run_id"]
out_path = args.out or os.path.join(cfg["results_dir"], "results.jsonl")
os.makedirs(os.path.dirname(out_path), exist_ok=True)

c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"],
              "group.id": f"final-{time.time()}", "auto.offset.reset": "earliest"})
c.subscribe([topics["metrics"], topics["lsh"]])

pending, e2e, final = {}, [], None
out = open(out_path, "a")


def emit(w, m, l):
    """Build the standardized record from a metrics window msg and an lsh window msg."""
    rec = {k: m.get(k) for k in (
        "run_id", "window_start", "window_end", "events_processed", "producer_eps", "stage2_eps",
        "consumer_lag", "latency_p50_ms", "latency_p95_ms", "bloom", "sampling", "hll", "dgim")}
    rec["bloom"] = {k: m["bloom"][k] for k in ("fp_rate", "fn_rate")}
    rec["sampling"] = {k: m["sampling"][k] for k in ("exact_mean_fare", "sample_mean_fare", "error_pct")}
    rec["hll"] = {k: m["hll"][k] for k in ("exact_routes", "estimate", "error_pct")}
    rec["dgim"] = {k: m["dgim"].get(k) for k in ("k", "exact_ones", "estimate", "error_pct")}
    rec["lsh"] = l["lsh"] if l else None
    # extras
    rec["diversity"] = m.get("diversity")
    rec["lsh_configs"] = l["configs"] if l else None
    rec["closed_by"] = m.get("closed_by")
    lp = m.get("last_producer_ts_ms")
    if lp:
        e2e.append(int(time.time() * 1000) - lp)
        rec["e2e_latency_ms"] = e2e[-1]
    out.write(json.dumps(rec) + "\n")
    out.flush()
    ls = rec["lsh"] or {}
    print(f"[{w}] events={rec['events_processed']} lag={rec['consumer_lag']} "
          f"HLL err={rec['hll']['error_pct']}% DGIM err={rec['dgim']['error_pct']}% "
          f"fare err={rec['sampling']['error_pct']}% LSH recall={ls.get('recall')} "
          f"cand={ls.get('candidates')}")


idle = 0
while idle < args.idle_exit:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        idle += 1
        continue
    idle = 0
    d = json.loads(msg.value())
    if d.get("run_id") != run_id:
        continue
    t = d.get("type")
    if t == "progress" and d.get("final"):
        final = d
    elif t in ("window", "lsh_window"):
        slot = pending.setdefault(d["window_start"], {})
        slot["m" if t == "window" else "l"] = d     # later message overwrites earlier (restart reruns)
        if "m" in slot and "l" in slot:
            emit(d["window_start"], slot["m"], slot["l"])
            del pending[d["window_start"]]
c.close()

for w, slot in sorted(pending.items()):             # windows with only one side available
    if "m" in slot:
        emit(w, slot["m"], slot.get("l"))

if final:
    summary = {"run_id": run_id, "type": "final_summary",
               "events_processed": final["events_processed"], "producer_eps": final["producer_eps"],
               "stage2_eps": final["stage2_eps"], "stage2_busy_eps": final["stage2_busy_eps"],
               "consumer_lag": final["consumer_lag"], "latency_p50_ms": final["latency_p50_ms"],
               "latency_p95_ms": final["latency_p95_ms"], "bloom": final["bloom"],
               "sampling": final["sampling"], "dgim": final["dgim"],
               "late_events": final["late_events"]}
    if e2e:
        summary["e2e_report_latency_p50_ms"], summary["e2e_report_latency_p95_ms"] = \
            [int(x) for x in np.percentile(e2e, [50, 95])]
    out.write(json.dumps(summary) + "\n")
    print("FINAL:", json.dumps({k: summary[k] for k in ("events_processed", "stage2_eps", "consumer_lag")}))
out.close()
print(f"written to {out_path}")