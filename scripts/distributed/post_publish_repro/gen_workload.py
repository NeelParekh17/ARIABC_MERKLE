#!/usr/bin/env python3
"""Generate the post-publication reproducer workload (one SQL statement per line)."""
import argparse, random
ap = argparse.ArgumentParser()
ap.add_argument("--count", type=int, default=20000)
ap.add_argument("--seed", type=int, default=7)
ap.add_argument("--hot", type=int, default=8, help="hot acct keys read by ab_proc and bumped by bump_proc")
ap.add_argument("--uq", type=float, default=0.10, help="share of uq_proc (0 for the historical-mode run)")
ap.add_argument("-o", "--out", required=True)
a = ap.parse_args()
r = random.Random(a.seed)
with open(a.out, "w") as f:
    for _ in range(a.count):
        p = r.random()
        rest = (1.0 - a.uq) / 2
        if p < rest:
            f.write(f"SELECT public.ab_proc({r.randrange(a.hot)}, {r.randrange(64)});\n")
        elif p < 2 * rest:
            f.write(f"SELECT public.bump_proc({r.randrange(a.hot)}, {r.randrange(1, 100)});\n")
        else:
            f.write(f"SELECT public.uq_proc({r.randrange(32)}, {r.randrange(48)});\n")
