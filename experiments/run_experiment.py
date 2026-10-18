"""Run ONE live end-to-end experiment (Kafka must already be up).

Resets topics, starts Stage 1/2/3 + final consumer + producer, samples lag every 2 s,
optionally kills/restarts Stage 2, then writes results/runs/<id>/* and one row in
results/experiment_summary.csv.

Examples:
  python experiments/run_experiment.py --id P3 --partitions 4 --slots 4
  python experiments/run_experiment.py --id F1 --kill-stage2-at 40 --down-for 25
"""
import argparse, json, os, shutil, signal, subprocess, sys, time
import numpy as np, pandas as pd, yaml
from confluent_kafka import Consumer, TopicPartition

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
PY = sys.executable

ap = argparse.ArgumentParser()
ap.add_argument("--id", required=True)
ap.add_argument("--partitions", type=int, default=4)
ap.add_argument("--slots", type=int, default=4, help="Spark local[N] for Stage 1 and Stage 2")
ap.add_argument("--key-mode", default="pickup_zone", choices=["pickup_zone", "round_robin"])
ap.add_argument("--rate", type=float, default=600)
ap.add_argument("--max-events", type=int, default=60000)
ap.add_argument("--cap", type=int, default=None, help="max_offsets_per_trigger for Stage 1+2 (limits consumer capacity)")
ap.add_argument("--lateness-min", type=int, default=None)
ap.add_argument("--kill-stage2-at", type=float, default=None, help="seconds after producer start")
ap.add_argument("--down-for", type=float, default=25)
ap.add_argument("--no-snapshot", action="store_true", help="Stage 2 without state snapshot")
ap.add_argument("--timeout", type=float, default=1500)
a = ap.parse_args()
rid = a.id


def rj(p):
    try:
        return json.load(open(p))
    except Exception:
        return {}


def start(cmd, log):
    env = dict(os.environ, PYTHONUNBUFFERED="1")      # flush prints to the log immediately
    return subprocess.Popen(cmd, stdout=open(log, "w"), stderr=subprocess.STDOUT,
                            start_new_session=True, env=env)

def kill(p, sig=signal.SIGTERM):
    if p is not None and p.poll() is None:
        try:
            os.killpg(p.pid, sig)
        except ProcessLookupError:
            pass


def wait_log(p, log, text, timeout=240):
    t = time.time()
    while time.time() - t < timeout:
        if os.path.exists(log) and text in open(log, errors="ignore").read():
            return True
        if p.poll() is not None:
            return False
        time.sleep(1)
    return False


# ---------------- per-run config ----------------
rd = f"results/runs/{rid}"
shutil.rmtree(rd, ignore_errors=True)
os.makedirs(rd)
cfg = yaml.safe_load(open("config.yaml"))
cfg.update({"run_id": rid, "partitions": a.partitions, "key_mode": a.key_mode, "producer_rate": a.rate})
cfg["data"]["max_events"] = a.max_events
if a.cap:
    cfg["stage1"]["max_offsets_per_trigger"] = a.cap
    cfg["stage2"]["max_offsets_per_trigger"] = a.cap
cfgp = f"{rd}/config.yaml"
yaml.safe_dump(cfg, open(cfgp, "w"))
T = cfg["kafka"]["topics"]

for f in ("producer_stats.json", "stage1_stats.json", "stage2_stats.json", "stage3_stats.json", "delayed_events.jsonl"):
    if os.path.exists(f"results/{f}"):
        os.remove(f"results/{f}")

subprocess.run([PY, "producer/create_topics.py", "--config", cfgp, "--reset",
                "--partitions", str(a.partitions)], check=True)
time.sleep(3)


class Mon:
    def __init__(self):
        self.c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "group.id": f"mon-{time.time()}"})
        self.parts = {}

    def end(self, topic):
        if topic not in self.parts:
            self.parts[topic] = sorted(self.c.list_topics(topic, timeout=10).topics[topic].partitions)
        return [self.c.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1] for p in self.parts[topic]]

    def sample(self, t, event=""):
        raw, clean = sum(self.end(T["raw"])), sum(self.end(T["clean"]))
        s1, s2 = rj("results/stage1_stats.json"), rj("results/stage2_stats.json")
        return {"t_s": round(t, 1), "raw_end": raw, "clean_end": clean,
                "stage1_in": s1.get("events_in", 0), "stage2_events": s2.get("events_processed", 0),
                "lag_raw": raw - s1.get("events_in", 0), "lag_clean": clean - s2.get("events_processed", 0),
                "spark_lag_raw": s1.get("kafka_lag_taxi_raw"), "spark_lag_clean": s2.get("consumer_lag"),
                "event": event}


mon = Mon()

# ---------------- commands ----------------
late = ["--lateness-min", str(a.lateness_min)] if a.lateness_min is not None else []
s1c = [PY, "stage1_clean_bloom/stage1.py", "--config", cfgp, "--slots", str(a.slots), "--idle-exit", "15"]
s2c = ([PY, "stage2_stream_analytics/stage2.py", "--config", cfgp, "--slots", str(a.slots), "--idle-exit", "15"]
       + late + (["--no-snapshot"] if a.no_snapshot else []))
s3c = [PY, "stage3_lsh/stage3.py", "--config", cfgp, "--slots", "2", "--idle-exit", "15"] + late

s1 = start(s1c + ["--fresh"], f"{rd}/stage1.log")
s2 = start(s2c + ["--fresh"], f"{rd}/stage2.log")
s3 = start(s3c + ["--fresh"], f"{rd}/stage3.log")

for p, lg, name in ((s1, "stage1.log", "stage1"), (s2, "stage2.log", "stage2"), (s3, "stage3.log", "stage3")):
    if not wait_log(p, f"{rd}/{lg}", "running with", timeout=300):
        for q in (s1, s2, s3):
            kill(q)
        sys.exit(f"{name} failed to start, see {rd}/{lg}")

time.sleep(5)
print("stages are up -> starting final consumer and producer")

fc = start([PY, "final_consumer/final_consumer.py", "--config", cfgp, "--run-id", rid,
            "--idle-exit", "45", "--out", f"{rd}/results.jsonl"], f"{rd}/final_consumer.log")
t0 = time.time()
prod = start([PY, "producer/producer.py", "--config", cfgp], f"{rd}/producer.log")

# ---------------- monitor loop ----------------
tl, killed, restarted, kill_t = [], False, False, None
while True:
    now, ev = time.time() - t0, ""
    if a.kill_stage2_at is not None and not killed and now >= a.kill_stage2_at:
        kill(s2, signal.SIGKILL)
        killed, kill_t, ev = True, now, "KILL stage2"
        print(f"[{now:.0f}s] Stage 2 killed")
    if killed and not restarted and now >= kill_t + a.down_for:
        s2 = start(s2c, f"{rd}/stage2_restart.log")          # no --fresh: resume from checkpoint + snapshot
        restarted, ev = True, "RESTART stage2"
        print(f"[{now:.0f}s] Stage 2 restarted")
    tl.append(mon.sample(now, ev))
    pending = a.kill_stage2_at is not None and not restarted
    if prod.poll() is not None and not pending and all(p.poll() is not None for p in (s1, s2, s3)):
        break
    if now > a.timeout:
        print("TIMEOUT")
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
for f in ("producer_stats.json", "stage1_stats.json", "stage2_stats.json", "stage3_stats.json", "delayed_events.jsonl"):
    if os.path.exists(f"results/{f}"):
        shutil.copy(f"results/{f}", f"{rd}/{f}")

clean_counts = mon.end(T["clean"])
pr, st1, st2 = rj(f"{rd}/producer_stats.json"), rj(f"{rd}/stage1_stats.json"), rj(f"{rd}/stage2_stats.json")
recs = [json.loads(l) for l in open(f"{rd}/results.jsonl")] if os.path.exists(f"{rd}/results.jsonl") else []
wins = [r for r in recs if r.get("type") != "final_summary"]
fin = next((r for r in recs if r.get("type") == "final_summary"), {})
if recs:
    with open("results/results.jsonl", "a") as out:
        out.write(open(f"{rd}/results.jsonl").read())


def dup_windows():
    c = Consumer({"bootstrap.servers": cfg["kafka"]["bootstrap_servers"], "group.id": f"dup-{time.time()}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([T["metrics"]])
    seen, idle = {}, 0
    while idle < 10:
        m = c.poll(1.0)
        if m is None or m.error():
            idle += 1
            continue
        idle = 0
        d = json.loads(m.value())
        if d.get("run_id") == rid and d.get("type") == "window":
            seen[d["window_start"]] = seen.get(d["window_start"], 0) + 1
    c.close()
    return sum(v - 1 for v in seen.values())


mean = lambda x: round(float(np.mean(x)), 3) if x else None
tld = pd.DataFrame(tl)
lsh = [w["lsh"] for w in wins if w.get("lsh")]
row = {
    "run_id": rid, "partitions": a.partitions, "slots": a.slots, "key_mode": a.key_mode,
    "requested_rate": a.rate, "cap_per_trigger": a.cap, "lateness_min": a.lateness_min,
    "kill_stage2": a.kill_stage2_at is not None, "no_snapshot": a.no_snapshot,
    "producer_eps": pr.get("actual_eps"), "stage1_eps": st1.get("stage1_eps"),
    "stage2_eps": st2.get("stage2_eps"), "stage2_busy_eps": st2.get("stage2_busy_eps"),
    "latency_p50_ms": st2.get("latency_p50_ms"), "latency_p95_ms": st2.get("latency_p95_ms"),
    "e2e_report_p50_ms": fin.get("e2e_report_latency_p50_ms"), "e2e_report_p95_ms": fin.get("e2e_report_latency_p95_ms"),
    "max_lag_raw": int(tld["lag_raw"].max()), "max_lag_clean": int(tld["lag_clean"].max()),
    "skew_raw": pr.get("max_partition_fraction"),
    "skew_clean": round(max(clean_counts) / max(sum(clean_counts), 1), 4),
    "stage1_accepted": st1.get("accepted"), "stage2_events": st2.get("events_processed"),
    "state_matches_accepted": st2.get("events_processed") == st1.get("accepted"),
    "hll_err_mean_pct": mean([w["hll"]["error_pct"] for w in wins]),
    "dgim_err_pct": st2.get("dgim", {}).get("error_pct"),
    "fare_sample_err_pct": st2.get("sampling", {}).get("error_pct"),
    "lsh_recall_mean": mean([l["recall"] for l in lsh if l.get("recall") is not None]),
    "lsh_candidates_mean": mean([l["candidates"] for l in lsh]),
    "bloom_fp_rate": st2.get("bloom", {}).get("fp_rate"),
    "late_discarded": st2.get("late_events", {}).get("discarded"),
    "dup_window_msgs": dup_windows(), "windows": len(wins),
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