"""Bounded-memory inverse CDF for finite Zipf distributions, including theta=1.

The prefix is summed directly; Euler-Maclaurin integrates the tail with B2/B4
corrections. At the 16,384-item boundary the omitted term is below double
precision for the supported theta range [0, 2]. One RNG draw per skewed key
preserves the suite generator's operation/value RNG sequence.
"""
import bisect
import math


class LargeZipfGenerator:
    def __init__(self, n, theta, rng):
        if n < 1 or not 0 <= theta <= 2:
            raise ValueError("Zipf requires n >= 1 and 0 <= theta <= 2")
        self.n, self.theta, self.rng = n, theta, rng
        self.prefix = []
        total = 0.0
        if theta > 1e-6:
            for k in range(1, min(n, 16384) + 1):
                total += 1.0 / math.pow(float(k), theta)
                self.prefix.append(total)
            self.total = self.harmonic(n)

    def harmonic(self, n):
        m = len(self.prefix)
        if n <= m:
            return self.prefix[n - 1]
        s = self.theta
        integral = (math.log(n / m) if s == 1 else
                    m ** (1 - s) * math.expm1((1 - s) * math.log(n / m)) / (1 - s))
        tail = integral + (n ** -s - m ** -s) / 2
        tail += s * (m ** (-s - 1) - n ** (-s - 1)) / 12
        tail += s * (s + 1) * (s + 2) * (n ** (-s - 3) - m ** (-s - 3)) / 720
        return self.prefix[-1] + tail

    def next_index(self):
        if self.theta <= 1e-6:
            return self.rng.randint(1, self.n)
        value = self.rng.random() * self.total
        if value <= self.prefix[-1]:
            return bisect.bisect_left(self.prefix, value) + 1
        lo, hi = len(self.prefix) + 1, self.n
        while lo < hi:
            mid = (lo + hi) // 2
            if self.harmonic(mid) >= value:
                hi = mid
            else:
                lo = mid + 1
        return lo
