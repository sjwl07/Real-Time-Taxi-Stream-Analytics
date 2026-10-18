"""Event vs entity sampling and HLL over all windows of a run.

  python tools/summarize_windows.py DEFAULT
"""
import json
import sys

import numpy as np

run = sys.argv[1] if len(sys.argv) > 1 else "DEFAULT"
wins = [json.loads(x) for x in open(f"results/runs/{run}/results.jsonl")]
wins = [r for r in wins if r.get("type") != "final_summary"]
h = [w["hll"]["error_pct"] for w in wins]
print(f"{run}: {len(wins)} windows | HLL error % mean {np.mean(h):.2f}, max {np.max(h):.2f}")

err = {"event_sampling": [], "entity_sampling": []}
for w in wins:
    d = w["diversity"]
    if d["window_events"] <= d["budget_events"] or not d["event_sampling"] or not d["entity_sampling"]:
        continue                          # whole window fits in the budget: nothing was sampled
    for k in err:
        err[k].append(d[k]["estimate"] - d["exact_fraction"])
for k, v in err.items():
    if v:
        print(f"{k:<16} windows={len(v)} mean abs error {np.mean(np.abs(v)):.3f}  mean signed error {np.mean(v):+.3f}")
