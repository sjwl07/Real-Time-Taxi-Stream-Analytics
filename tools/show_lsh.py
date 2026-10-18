"""LSH candidate pairs + exact Jaccard for one window (default setting).

  python tools/show_lsh.py DEFAULT
  python tools/show_lsh.py DEFAULT --window 3 --top 0      # --top 0 = all candidates
"""
import argparse
import json

ap = argparse.ArgumentParser()
ap.add_argument("run")
ap.add_argument("--window", type=int, default=0)
ap.add_argument("--top", type=int, default=15, help="show the N most and N least similar candidates (0 = all)")
a = ap.parse_args()
wins = [json.loads(x) for x in open(f"results/runs/{a.run}/results.jsonl")]
wins = [r for r in wins if r.get("type") != "final_summary" and r.get("lsh_configs")]
w = wins[min(a.window, len(wins) - 1)]

print(f"Window {w['window_start']} -> {w['window_end']}")
for c in w["lsh_configs"]:
    print(f"  {c['bands']:>2} x {c['rows']:<2} midpoint~{c['threshold_approx']}: candidates={c['candidates']} "
          f"true pairs={c['true_pairs']} found={c['found_pairs']} recall={c['recall']} "
          f"precision={c['candidate_precision']} reduction={c['candidate_reduction']} "
          f"| LSH {c['lsh_latency_ms']} ms vs exact all-pairs {c['exact_latency_ms']} ms")

c = w["lsh_configs"][0]
pairs = c.get("pairs", [])
show = pairs if a.top == 0 or len(pairs) <= 2 * a.top else pairs[:a.top] + [None] + pairs[-a.top:]
print(f"\nCandidate pairs of the default {c['bands']}x{c['rows']} setting (exact Jaccard, threshold 0.5):")
print("  zone A  zone B  exact Jaccard  kept")
for p in show:
    if p is None:
        print("     ...")
        continue
    print(f"  {p['zone_a']:>6}  {p['zone_b']:>6}  {p['exact_jaccard']:>13}  {'YES' if p['similar'] else 'no (false candidate)'}")
missed = c.get("missed_true_pairs", [])
print(f"\nTrue similar pairs missed by LSH: {len(missed)}  (most similar first)")
for p in missed[:10]:
    print(f"  {p['zone_a']:>6}  {p['zone_b']:>6}  {p['exact_jaccard']:>13}")
