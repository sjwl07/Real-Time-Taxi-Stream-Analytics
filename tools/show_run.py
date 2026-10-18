"""Exact vs approximate results of a finished run (Bloom, HLL, DGIM, reservoir, sampling).

  python tools/show_run.py DEFAULT
  python tools/show_run.py DEMO --window 2
"""
import argparse
import json

ap = argparse.ArgumentParser()
ap.add_argument("run")
ap.add_argument("--window", type=int, default=0, help="index of the window to show")
a = ap.parse_args()
d = f"results/runs/{a.run}"
s2 = json.load(open(f"{d}/stage2_stats.json"))
wins = [json.loads(x) for x in open(f"{d}/results.jsonl")]
wins = [r for r in wins if r.get("type") != "final_summary"]
w = wins[min(a.window, len(wins) - 1)]

print(f"== {a.run}: {s2['events_processed']} events | stage2 {s2['stage2_eps']}/s | "
      f"latency p50/p95 {s2['latency_p50_ms']}/{s2['latency_p95_ms']} ms | {len(wins)} windows")
b = s2["bloom"]
print(f"\nBLOOM  bits={b['bit_array_size']} hashes={b['num_hashes']} inserted={b['inserted_ids']}"
      f"\n  ground truth vs filter: tp={b['tp']} fp={b['fp']} tn={b['tn']} fn={b['fn']}"
      f" -> fp_rate={b['fp_rate']} fn_rate={b['fn_rate']}  fp examples={b['fp_examples']}")
h = w["hll"]
print(f"\nHLL  window {w['window_start']}: exact routes={h['exact_routes']} estimate={h['estimate']} "
      f"error={h['error_pct']}%")
dg = s2["dgim"]
print(f"\nDGIM  last {dg['window']} events, buckets stored={dg['buckets']}")
for q in dg["queries"]:
    print(f"  k={q['k']:>6}  exact={q['exact_ones']:>5}  estimate={q['estimate']:>8}  "
          f"abs err={q['abs_error']}  err={q['error_pct']}%")
print("\nRESERVOIR SAMPLING")
for r in s2["sampling"]["reservoirs"]:
    print(f"  k={r['k']:>6}  fare exact={r['exact_mean_fare']} sample={r['sample_mean_fare']} "
          f"err={r['error_pct_fare']}% | distance exact={r['exact_mean_distance']} "
          f"sample={r['sample_mean_distance']} err={r['error_pct_distance']}%")
dv = w["diversity"]
print(f"\nDIVERSE ZONES  window {w['window_start']}: exact fraction={dv['exact_fraction']} "
      f"({dv['diverse_zones']}/{dv['active_zones']} zones, {dv['window_events']} events, budget {dv['budget_events']})")
for name in ("event_sampling", "entity_sampling"):
    s = dv[name]
    if s:
        print(f"  {name:<16} estimate={s['estimate']}  abs err={s['abs_error']}  "
              f"zones in sample={s['zones_in_sample']}  events stored={s['events_stored']}")
