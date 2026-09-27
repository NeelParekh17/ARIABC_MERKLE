#!/usr/bin/env python3
"""Throughput timeline for a cluster run (stall / recovery-impact check).

Reads the gateway's tx_latency.csv (tx_idx,latency_ms,finish_ms), buckets
client-visible completions by finish time and reports per-bucket TPS, the
longest completion gap, empty buckets, and the throughput inside the online
recovery window versus the rest of the run.  The recovery window starts at the
fault injection (fault_timeline.env) and ends when the RECOVERY_EVENT reported
by the gateway completes.
"""
import argparse
import csv
import os
import re
import sys


def load_finish_ms(path):
    out = []
    with open(path) as f:
        r = csv.DictReader(f)
        if "finish_ms" not in (r.fieldnames or []):
            return None
        for row in r:
            out.append(float(row["finish_ms"]))
    out.sort()
    return out


def read_env(path):
    env = {}
    if os.path.exists(path):
        for line in open(path):
            if "=" in line:
                k, v = line.strip().split("=", 1)
                env[k] = v
    return env


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-dir", required=True)
    ap.add_argument("--bucket-ms", type=int, default=100)
    ap.add_argument("--csv", default=None)
    ap.add_argument("--gw-log", default=None)
    a = ap.parse_args()

    csv_path = a.csv or os.path.join(a.log_dir, "tx_latency.csv")
    if not os.path.exists(csv_path):
        print(f"TPS_TIMELINE skipped: {csv_path} missing")
        return 0
    fin = load_finish_ms(csv_path)
    if not fin:
        print("TPS_TIMELINE skipped: no finish_ms column")
        return 0

    env = read_env(os.path.join(a.log_dir, "fault_timeline.env"))
    gw_start = float(env.get("gateway_start_epoch_ms", "0") or 0)
    inject_ms = None
    if "fault_inject_start_epoch_ms" in env and gw_start:
        inject_ms = float(env["fault_inject_start_epoch_ms"]) - gw_start

    # RECOVERY_EVENT total_ms counts from detection; place the window end at
    # the latest completion we can bound: injection + detection + recovery.
    events = []
    gw_log = a.gw_log or os.path.join(a.log_dir, "gateway_test.log")
    if os.path.exists(gw_log):
        for line in open(gw_log, errors="replace"):
            if line.startswith("RECOVERY_EVENT "):
                events.append(line.strip())

    b = a.bucket_ms
    first, last = fin[0], fin[-1]
    nb = int((last - first) // b) + 1
    counts = [0] * nb
    for t in fin:
        counts[int((t - first) // b)] += 1
    # Ignore the ramp-up and drain tails (first/last 2% of the run).
    lo = int(nb * 0.02)
    hi = max(lo + 1, int(nb * 0.98))
    steady = counts[lo:hi]
    to_tps = 1000.0 / b
    gaps = [fin[i + 1] - fin[i] for i in range(len(fin) - 1)]
    max_gap = max(gaps) if gaps else 0.0
    max_gap_at = fin[gaps.index(max_gap)] - first if gaps else 0.0
    empty = sum(1 for c in steady if c == 0)
    mean_tps = (len(fin) / ((last - first) / 1000.0)) if last > first else 0.0

    print(f"TPS_TIMELINE completions={len(fin)} duration_ms={last - first:.0f} bucket_ms={b} "
          f"mean_tps={mean_tps:.1f} steady_min_tps={min(steady) * to_tps if steady else 0:.1f} "
          f"steady_max_tps={max(steady) * to_tps if steady else 0:.1f} empty_buckets={empty} "
          f"max_completion_gap_ms={max_gap:.2f} max_gap_at_ms={max_gap_at:.0f}")

    if inject_ms is not None:
        inj = inject_ms - first
        rec_total = 0.0
        for ev in events:
            m = re.search(r"total_ms=(\d+)", ev)
            if m:
                rec_total = max(rec_total, float(m.group(1)))
        # Window: injection -> injection + (detection+recovery) + 2 s of catch-up.
        w_lo = max(0.0, inj)
        w_hi = w_lo + max(rec_total, 1000.0) + 2000.0
        inside = [t for t in fin if w_lo <= t - first < w_hi]
        outside_span = (last - first) - (w_hi - w_lo)
        outside = len(fin) - len(inside)
        in_tps = len(inside) / ((w_hi - w_lo) / 1000.0) if w_hi > w_lo else 0.0
        out_tps = outside / (outside_span / 1000.0) if outside_span > 0 else 0.0
        win_buckets = counts[int(w_lo // b): int(w_hi // b) + 1]
        print(f"TPS_RECOVERY_WINDOW inject_at_ms={inj:.0f} window_ms=[{w_lo:.0f},{w_hi:.0f}) "
              f"tps_inside={in_tps:.1f} tps_outside={out_tps:.1f} "
              f"ratio={in_tps / out_tps if out_tps else 0:.3f} "
              f"min_bucket_tps_inside={min(win_buckets) * to_tps if win_buckets else 0:.1f} "
              f"empty_buckets_inside={sum(1 for c in win_buckets if c == 0)}")

    with open(os.path.join(a.log_dir, "tps_timeline.csv"), "w") as f:
        f.write("t_ms,tps\n")
        for i, c in enumerate(counts):
            f.write(f"{i * b},{c * to_tps:.1f}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
