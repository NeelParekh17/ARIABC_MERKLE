#!/usr/bin/env python3
"""Random-read latency probe used to wait for an SSD to return to its idle state.

Consumer QLC/TLC drives (the 100M host uses an Intel 660p) absorb large writes in
an SLC cache and fold it into the main array while idle. That background work
is invisible to host block counters but inflates read latency for minutes after
a large restore. The runner calls this script remotely between the restore and
the cold start; it performs O_DIRECT reads of existing files (no page-cache
effect) and reports latency at queue depth 1 and 8, commit-like O_DSYNC 8KB write latency (a full
SLC cache sends these to slow QLC), and the device's write rate.

Modes:
  calibrate  -- wait until the device shows no writes, then report idle latency
  settle     -- repeat probes until latency is within threshold of calibration
Output is one JSON object on stdout.
"""
import argparse
import glob
import json
import mmap
import os
import random
import statistics
import sys
import threading
import time

BLOCK = 4096


def device_write_sectors(stat_path):
    with open(stat_path) as handle:
        return int(handle.read().split()[6])


def open_files(data_dir, min_bytes):
    paths = sorted(p for p in glob.glob(os.path.join(data_dir, "base", "*", "*"))
                   if os.path.isfile(p) and os.path.getsize(p) >= min_bytes)
    if not paths:
        raise SystemExit(f"No probe files of at least {min_bytes} bytes under {data_dir}")
    fds = [os.open(p, os.O_RDONLY | os.O_DIRECT) for p in paths]
    return fds, [os.fstat(fd).st_size // BLOCK for fd in fds]


def probe(fds, blocks, rng, reads, depth):
    latencies = []
    lock = threading.Lock()
    offsets = [(rng.randrange(len(fds)),) for _ in range(reads)]
    offsets = [(k, rng.randrange(blocks[k]) * BLOCK) for (k,) in offsets]

    def worker(chunk):
        buf = mmap.mmap(-1, BLOCK)
        local = []
        for k, off in chunk:
            start = time.perf_counter_ns()
            os.preadv(fds[k], [buf], off)
            local.append((time.perf_counter_ns() - start) / 1000.0)
        with lock:
            latencies.extend(local)

    threads = [threading.Thread(target=worker, args=(offsets[i::depth],)) for i in range(depth)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    latencies.sort()
    return dict(median_us=statistics.median(latencies),
                p99_us=latencies[int(len(latencies) * 0.99) - 1],
                mean_us=statistics.fmean(latencies))


WRITE_BLOCK = 8192
WRITE_FILE_BYTES = 64 << 20


def open_write_file(path):
    """Preallocated scratch file for commit-like O_DSYNC 8KB overwrites."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_DIRECT | os.O_DSYNC, 0o600)
    if os.fstat(fd).st_size != WRITE_FILE_BYTES:
        os.posix_fallocate(fd, 0, WRITE_FILE_BYTES)
        buf = mmap.mmap(-1, 1 << 20)
        buf.write(os.urandom(1 << 20))
        for off in range(0, WRITE_FILE_BYTES, 1 << 20):
            os.pwritev(fd, [buf], off)
    return fd


def write_probe(fd, rng, writes):
    buf = mmap.mmap(-1, WRITE_BLOCK)
    buf.write(os.urandom(WRITE_BLOCK))
    latencies = []
    for _ in range(writes):
        off = rng.randrange(WRITE_FILE_BYTES // WRITE_BLOCK) * WRITE_BLOCK
        start = time.perf_counter_ns()
        os.pwritev(fd, [buf], off)
        latencies.append((time.perf_counter_ns() - start) / 1000.0)
    latencies.sort()
    return dict(median_us=statistics.median(latencies),
                p99_us=latencies[int(len(latencies) * 0.99) - 1],
                mean_us=statistics.fmean(latencies)), writes * WRITE_BLOCK


def sample(fds, blocks, rng, stat_path, interval, reads, wfd, writes):
    before = device_write_sectors(stat_path)
    started = time.monotonic()
    qd1 = probe(fds, blocks, rng, reads, 1)
    qd8 = probe(fds, blocks, rng, reads * 4, 8)
    wr, own = write_probe(wfd, rng, writes)
    time.sleep(max(0.0, interval - (time.monotonic() - started)))
    # Exclude the probe's own writes (plus filesystem journal slack) from idleness.
    written = max(0.0, ((device_write_sectors(stat_path) - before) * 512 - 2 * own) / 2**20)
    return dict(t=round(time.monotonic(), 3), qd1=qd1, qd8=qd8, wr=wr, write_mib=round(written, 3))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["calibrate", "settle", "watch"])
    parser.add_argument("--data-dir", required=True, help="Stopped PostgreSQL directory to read")
    parser.add_argument("--device-stat", required=True, help="/sys/dev/block/<maj:min>/stat")
    parser.add_argument("--min-file-bytes", type=int, default=256 << 20)
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--reads", type=int, default=1000)
    parser.add_argument("--idle-write-mib", type=float, default=2.0,
                        help="Maximum device writes per interval that still counts as idle")
    parser.add_argument("--qd1-threshold-us", type=float)
    parser.add_argument("--qd8-threshold-us", type=float)
    parser.add_argument("--wr-threshold-us", type=float)
    parser.add_argument("--write-file", required=True, help="Scratch file on the data filesystem")
    parser.add_argument("--writes", type=int, default=200)
    parser.add_argument("--consecutive", type=int, default=3)
    parser.add_argument("--min-wait", type=float, default=0.0)
    parser.add_argument("--max-wait", type=float, default=1800.0)
    parser.add_argument("--seed", type=int, default=20260928)
    args = parser.parse_args()

    fds, blocks = open_files(args.data_dir, args.min_file_bytes)
    wfd = open_write_file(args.write_file)
    rng = random.Random(args.seed)
    started = time.monotonic()
    history = []

    if args.mode == "watch":
        while time.monotonic() - started < args.max_wait:
            entry = sample(fds, blocks, rng, args.device_stat, args.interval, args.reads, wfd, args.writes)
            entry["elapsed_s"] = round(time.monotonic() - started, 1)
            print(json.dumps(entry), flush=True)
        return 0

    if args.mode == "settle" and None in (args.qd1_threshold_us, args.qd8_threshold_us, args.wr_threshold_us):
        parser.error("settle requires --qd1-threshold-us, --qd8-threshold-us and --wr-threshold-us")

    streak = 0
    while True:
        entry = sample(fds, blocks, rng, args.device_stat, args.interval, args.reads, wfd, args.writes)
        entry["elapsed_s"] = round(time.monotonic() - started, 1)
        history.append(entry)
        idle = entry["write_mib"] <= args.idle_write_mib
        if args.mode == "settle":
            idle = (idle and entry["qd1"]["median_us"] <= args.qd1_threshold_us
                    and entry["qd8"]["median_us"] <= args.qd8_threshold_us
                    and entry["wr"]["median_us"] <= args.wr_threshold_us)
        streak = streak + 1 if idle else 0
        elapsed = time.monotonic() - started
        if streak >= args.consecutive and elapsed >= args.min_wait:
            window = history[-args.consecutive:]
            result = dict(mode=args.mode, settled=True, wait_s=round(elapsed, 1),
                          probes=len(history), files=len(fds),
                          qd1_median_us=min(e["qd1"]["median_us"] for e in window),
                          qd8_median_us=min(e["qd8"]["median_us"] for e in window),
                          wr_median_us=min(e["wr"]["median_us"] for e in window),
                          history=history)
            print(json.dumps(result))
            return 0
        if elapsed >= args.max_wait:
            print(json.dumps(dict(mode=args.mode, settled=False, wait_s=round(elapsed, 1),
                                  probes=len(history), history=history)))
            return 3


if __name__ == "__main__":
    sys.exit(main())
