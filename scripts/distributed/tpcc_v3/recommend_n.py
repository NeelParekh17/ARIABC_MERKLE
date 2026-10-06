#!/usr/bin/env python3
"""Choose one identical transaction count across modes from pilot rate estimates."""
import argparse
import json
import math


def recommend(rates, warmup=60, window=300, drain=60, inflight=65536, margin=1.25):
    if not rates or any(not math.isfinite(r) or r <= 0 for r in rates.values()):
        raise ValueError('every rate must be positive and finite')
    # Add maximum submission lead separately: the measurement ends before sent==N.
    n = math.ceil(max(rates.values()) * (warmup + window + drain) * margin + inflight)
    return dict(count=n, fastest_rate=max(rates.values()), rates=rates, warmup_s=warmup,
                min_window_s=window, drain_s=drain, inflight_allowance=inflight, margin=margin,
                estimated_duration_s={k: n/v for k, v in rates.items()},
                note='Pilot estimate only; every final attempt still must pass measured window/checkpoint acceptance.')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rates', nargs='+', required=True, help='pg=TPS det=TPS merkle=TPS')
    p.add_argument('--warmup', type=float, default=60)
    p.add_argument('--window', type=float, default=300)
    p.add_argument('--drain', type=float, default=60)
    p.add_argument('--inflight', type=int, default=65536)
    p.add_argument('--margin', type=float, default=1.25)
    p.add_argument('--count-only', action='store_true')
    a = p.parse_args()
    if min(a.warmup, a.window, a.drain, a.inflight) < 0 or a.margin < 1:
        p.error('durations/inflight must be nonnegative; margin >= 1')
    rates = {k: float(v) for k, v in (s.split('=', 1) for s in a.rates)}
    result = recommend(rates, a.warmup, a.window, a.drain, a.inflight, a.margin)
    print(result['count'] if a.count_only else json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
