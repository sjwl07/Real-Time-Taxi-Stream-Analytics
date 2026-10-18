"""Run ONE live end-to-end experiment (Kafka must be up).

Recreates the topics, starts Stage 1/2/3 + final consumer + producer, samples lag every 2 s,
optionally kills/restarts Stage 2, then writes results/runs/<id>/* and one row of
results/experiment_summary.csv. Every option defaults to config.yaml.

  python experiments/run_experiment.py --id DEFAULT
  python experiments/run_experiment.py --id P3 --partitions 4 --slots 4 --rate 2000
  python experiments/run_experiment.py --id DEMO --dgim-k 500,2000,10000 --reservoir 50,500,5000
  python experiments/run_experiment.py --id F1 --kill-stage2-at 40 --down-for 25
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

import numpy as np
import pandas as pd
import yaml
from confluent_kafka import Consumer, TopicPartition

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
PY = sys.executable
STATS = ("producer_stats.json", "stage1_stats.json", "stage2_stats.json", "stage3_stats.json", "delayed_events.jsonl")

ap = argparse.ArgumentParser()
ap.add_argument("--id", required=True)
ap.add_argument("--config", default="config.yaml")
ap.add_argument("--partitions", type=int)
ap.add_argument("--slots", type=int, help="Spark local[N] for Stage 1 and Stage 2")
ap.add_argument("--key-mode", choices=["pickup_zone", "round_robin"])
ap.add_argument("--rate", type=float)
ap.add_argument("--max-events", type=int)
ap.add_argument("--cap", type=int, help="max offsets per 1 s trigger for Stage 1+2 (fixes consumer capacity)")
ap.add_argument("--lateness-min", type=int)
ap.add_argument("--dgim-k", help="comma list, e.g. 500,2000,10000")
ap.add_argument("--reservoir", help="comma list, e.g. 50,500,5000")
ap.add_argument("--kill-stage2-at", type=float, help="seconds after the producer starts")
ap.add_argument("--down-for", type=float, default=25)
ap.add_argument("--no-snapshot", action="store_true", help="Stage 2 without state snapshot")
ap.add_argument("--timeout", type=float, default=1500)
a = ap.parse_args()
rid = a.id

# ---------------- per-run config (copy of config.yaml + overrides) ----------------
cfg = yaml.safe_load(open(a.config))
a.partitions = a.partitions or cfg["partitions"]
a.slots = a.slots or cfg["spark"]["local_slots"]
a.key_mode = a.key_mode or cfg["key_mode"]
a.rate = a.rate or cfg["producer_rate"]
cfg.update(run_id=rid, partitions=a.partitions, key_mode=a.key_mode, producer_rate=a.rate)
if a.max_events:
    cfg["data"]["max_events"] = a.max_events
if a.cap:
    cfg["stage1"]["max_offsets_per_trigger"] = cfg["stage2"]["max_offsets_per_trigger"] = a.cap
if a.lateness_min is not None:
    cfg["allowed_lateness_min"] = a.lateness_min
if a.dgim_k:
    cfg["stage2"]["dgim_queries"] = [int(x) for x in a.dgim_k.split(",")]
if a.reservoir:
    cfg["stage2"]["reservoir_sizes"] = [int(x) for x in a.reservoir.split(",")]

rd = f"results/runs/{rid}"
shutil.rmtree(rd, ignore_errors=True)
os.makedirs(rd)
cfgp = f"{rd}/config.yaml"
yaml.safe_dump(cfg, open(cfgp, "w"), sort_keys=False)
T = cfg["kafka"]["topics"]
for f in STATS:
    if os.path.exists(f"results/{f}"):
        os.remove(f"results/{f}")


def rj(p):
    try:
        return json.load(open(p))
    except Exception:
        return {}


def start(cmd, log):
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    return subprocess.Popen(cmd, stdout=open(log, "w"), stderr=subprocess.STDOUT, start_new_session=True, env=env)


def kill(p, sig=signal.SIGTERM):
    if p is not None and p.poll() is None:
        try:
            os.killpg(p.pid, sig)
        except ProcessLookupError:
            pass


def wait_log(p, log, text, timeout=300):
    t = time.time()
    while time.time() - t < timeout:
        if text in open(log, errors="ignore").read():
            return True
        if p.poll() is not None:
            return False
        time.sleep(1)
    return False


subprocess.run([PY, "producer/create_topics.py", "--config", cfgp, "--reset"], check=True)
time.sleep(3)

mon = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "group.id": f"mon-{time.time()}"})


def end_offsets(topic):
    parts = sorted(mon.list_topics(topic, timeout=10).topics[topic].partitions)
    return [mon.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1] for p in parts]


def sample(t, event=""):
    """Lag = end offset of the topic - offsets the stage has processed (from its Spark checkpoint)."""
    raw, clean = sum(end_offsets(T["raw"])), sum(end_offsets(T["clean"]))
    s1, s2 = rj("results/stage1_stats.json"), rj("results/stage2_stats.json")
    return {"t_s": round(t, 1), "raw_end": raw, "clean_end": clean,
            "stage1_offsets": s1.get("kafka_offsets_processed") or 0,
            "stage2_offsets": s2.get("kafka_offsets_processed") or 0,
            "lag_raw": raw - (s1.get("kafka_offsets_processed") or 0),
            "lag_clean": clean - (s2.get("kafka_offsets_processed") or 0),
            "stage2_events_in_state": s2.get("events_processed", 0), "event": event}


# ---------------- start the pipeline ----------------
s1c = [PY, "stage1_clean_bloom/stage1.py", "--config", cfgp, "--slots", str(a.slots), "--idle-exit", "15"]
s2c = [PY, "stage2_stream_analytics/stage2.py", "--config", cfgp, "--slots", str(a.slots), "--idle-exit", "15"] \
    + (["--no-snapshot"] if a.no_snapshot else [])
s3c = [PY, "stage3_lsh/stage3.py", "--config", cfgp, "--slots", "2", "--idle-exit", "15"]
s1 = start(s1c + ["--fresh"], f"{rd}/stage1.log")
s2 = start(s2c + ["--fresh"], f"{rd}/stage2.log")
s3 = start(s3c + ["--fresh"], f"{rd}/stage3.log")
print("starting Spark stages (1-2 min) ...", flush=True)
for p, name in ((s1, "stage1"), (s2, "stage2"), (s3, "stage3")):
    if not wait_log(p, f"{rd}/{name}.log", "running with"):
        for q in (s1, s2, s3):
            kill(q)
        sys.exit(f"{name} failed to start, see {rd}/{name}.log")
time.sleep(5)
print("stages are up -> starting final consumer and producer", flush=True)

fc = start([PY, "final_consumer/final_consumer.py", "--config", cfgp, "--run-id", rid,
            "--idle-exit", "45", "--out", f"{rd}/results.jsonl"], f"{rd}/final_consumer.log")
t0 = time.time()
prod = start([PY, "producer/producer.py", "--config", cfgp], f"{rd}/producer.log")

tl, killed, restarted, kill_t = [], False, False, None
while True:
    now, ev = time.time() - t0, ""
    if a.kill_stage2_at is not None and not killed and now >= a.kill_stage2_at:
        kill(s2, signal.SIGKILL)
        killed, kill_t, ev = True, now, "KILL stage2"
        print(f"[{now:.0f}s] Stage 2 killed", flush=True)
    if killed and not restarted and now >= kill_t + a.down_for:
        s2 = start(s2c, f"{rd}/stage2_restart.log")          # no --fresh: resume checkpoint (+ snapshot)
        restarted, ev = True, "RESTART stage2"
        print(f"[{now:.0f}s] Stage 2 restarted", flush=True)
    tl.append(sample(now, ev))
    if int(now) % 10 < 2:
        x = tl[-1]
        print(f"[{now:.0f}s] raw={x['raw_end']} clean={x['clean_end']} lag_raw={x['lag_raw']} "
              f"lag_clean={x['lag_clean']} {ev}", flush=True)
    waiting = a.kill_stage2_at is not None and not restarted
    if prod.poll() is not None and not waiting and all(p.poll() is not None for p in (s1, s2, s3)):
        break
    if now > a.timeout:
        print("TIMEOUT", flush=True)
        break
    time.sleep(2)

try:
    fc.wait(timeout=90)
except subprocess.TimeoutExpired:
    pass
for p in (s1, s2, s3, prod, fc):
    kill(p)
pd.DataFrame(tl).to_csv(f"{rd}/timeline.csv", index=False)

# ---------------- collect ----------------
for f in STATS:
    if os.path.exists(f"results/{f}"):
        shutil.copy(f"results/{f}", f"{rd}/{f}")
clean_counts = end_offsets(T["clean"])
pr, st1, st2 = rj(f"{rd}/producer_stats.json"), rj(f"{rd}/stage1_stats.json"), rj(f"{rd}/stage2_stats.json")
recs = [json.loads(x) for x in open(f"{rd}/results.jsonl")] if os.path.exists(f"{rd}/results.jsonl") else []
wins = [r for r in recs if r.get("type") != "final_summary"]
fin = next((r for r in recs if r.get("type") == "final_summary"), {})
if recs:
    with open("results/results.jsonl", "a") as f:
        f.write(open(f"{rd}/results.jsonl").read())

dup, idle = {}, 0                                         # window reports sent more than once
mon2 =Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "group.id": f"dup-{time.time()}",
                 "auto.offset.reset": "earliest"})
mon2.subscribe([T["metrics"]])
while idle < 10:
    m = mon2.poll(1.0)
    if m is None or m.error():
        idle += 1
        continue
    idle = 0
    d = json.loads(m.value())
    if d.get("run_id") == rid and d.get("type") == "window":
        dup[d["window_start"]] = dup.get(d["window_start"], 0) + 1
mon2.close()


def mean(x):
    x = [v for v in x if v is not None]
    return round(float(np.mean(x)), 3) if x else None


tld = pd.DataFrame(tl)
lsh = [w["lsh"] for w in wins if w.get("lsh")]
row = {
    "run_id": rid, "partitions": a.partitions, "slots": a.slots, "key_mode": a.key_mode,
    "requested_rate": a.rate, "cap_per_trigger": a.cap, "lateness_min": cfg["allowed_lateness_min"],
    "kill_stage2": a.kill_stage2_at is not None, "no_snapshot": a.no_snapshot,
    "producer_eps": pr.get("actual_eps"), "stage1_eps": st1.get("stage1_eps"),
    "stage2_eps": st2.get("stage2_eps"), "stage2_busy_eps": st2.get("stage2_busy_eps"),
    "latency_p50_ms": st2.get("latency_p50_ms"), "latency_p95_ms": st2.get("latency_p95_ms"),
    "max_lag_raw": int(tld["lag_raw"].max()), "max_lag_clean": int(tld["lag_clean"].max()),
    "skew_raw": pr.get("max_partition_fraction"),
    "skew_clean": round(max(clean_counts) / max(sum(clean_counts), 1), 4),
    "stage1_accepted": st1.get("accepted"), "stage2_events": st2.get("events_processed"),
    "state_matches_accepted": st2.get("events_processed") == st1.get("accepted"),
    "hll_err_mean_pct": mean([w["hll"]["error_pct"] for w in wins]),
    "dgim_err_pct": st2.get("dgim", {}).get("error_pct"),
    "fare_sample_err_pct": st2.get("sampling", {}).get("error_pct"),
    "lsh_recall_mean": mean([x.get("recall") for x in lsh]),
    "lsh_candidates_mean": mean([x["candidates"] for x in lsh]),
    "bloom_fp_rate": st2.get("bloom", {}).get("fp_rate"),
    "late_discarded": st2.get("late_events", {}).get("discarded"),
    "dup_window_msgs": sum(v - 1 for v in dup.values()), "windows": len(wins),
    "duration_s": round(time.time() - t0, 1),
}
csvp = "results/experiment_summary.csv"
df = pd.DataFrame([row])
if os.path.exists(csvp) and os.path.getsize(csvp) > 0:
    try:
        old = pd.read_csv(csvp)
        df = pd.concat([old[old.run_id != rid], df], ignore_index=True)
    except pd.errors.EmptyDataError:
        pass
df.to_csv(csvp, index=False)
print(json.dumps(row, indent=2))
