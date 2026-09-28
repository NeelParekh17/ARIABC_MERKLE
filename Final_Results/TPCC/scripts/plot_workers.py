import csv, statistics as st, collections, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

here = os.path.dirname(os.path.abspath(__file__))
rows = list(csv.DictReader(open(os.path.expanduser("~/claude_checks/chart/workers_results.csv"))))
tps = collections.defaultdict(list)
for r in rows:
    tps[(r["mode"] + "_" + r["layout"], int(r["workers"]))].append(float(r["tps"]))
WS = [8, 16, 24, 32, 48, 64]

# Fixed categorical order (reference palette slots 1-4); marker per series so
# identity never relies on color alone.
SERIES = [
    ("pg_cur",      "PostgreSQL (pg)",                 "#2a78d6", "o"),
    ("det_cur",     "Deterministic (det)",             "#eb6834", "s"),
    ("merkle_wh16", "Merkle, warehouse routing",       "#1baf7a", "D"),
    ("merkle_cur",  "Merkle, hash % 200 (previous)",   "#eda100", "^"),
]
TEXT, TEXT2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"

with open(os.path.join(here, "tpcc_ranking_workers_summary.csv"), "w") as f:
    f.write("config,workers,trials,best_tps,median_tps,min_tps\n")
    for key, *_ in SERIES:
        for w in WS:
            v = tps[(key, w)]
            f.write(f"{key},{w},{len(v)},{max(v):.1f},{st.median(v):.1f},{min(v):.1f}\n")

def panel(ax, keys, title):
    for key, label, color, marker in SERIES:
        if key not in keys:
            continue
        med = [max(tps[(key, w)]) for w in WS]  # best of 3 (peak)
        lo = [min(tps[(key, w)]) for w in WS]
        hi = [max(tps[(key, w)]) for w in WS]
        ax.fill_between(WS, lo, hi, color=color, alpha=0.08, linewidth=0)
        ax.plot(WS, med, color=color, linewidth=2, marker=marker, markersize=7,
                markeredgecolor=SURFACE, markeredgewidth=1.5, label=label, zorder=3)
        ax.annotate(f"{med[-1]:,.0f}", (WS[-1], med[-1]), xytext=(8, 0),
                    textcoords="offset points", va="center", fontsize=9, color=TEXT2)
    ax.set_title(title, loc="left", fontsize=12, color=TEXT, pad=10)
    ax.set_xlabel("Workers", color=TEXT2)
    ax.set_ylabel("Throughput (TPS, best of 3)", color=TEXT2)
    ax.set_xticks(WS)
    ax.set_xlim(0, 72)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=TEXT2, labelsize=9)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.legend(frameon=False, fontsize=9, loc="upper left", labelcolor=TEXT)

fig, axes = plt.subplots(1, 2, figsize=(14, 5.2), facecolor=SURFACE)
for ax in axes:
    ax.set_facecolor(SURFACE)
panel(axes[0], {"pg_cur", "det_cur", "merkle_wh16", "merkle_cur"}, "All modes")
panel(axes[1], {"merkle_wh16", "merkle_cur"}, "Merkle mode: partition layout")
fig.suptitle("TPC-C throughput vs workers at 100 warehouses on ranking (EPYC 9654)", x=0.01, ha="left",
             fontsize=14, color=TEXT, y=0.985)
fig.text(0.01, 0.925,
         "20,000 tx/run · 100 warehouses · shared_buffers 32GB · prewarmed · SERIALIZABLE · det key-tag fix in det & Merkle · "
         "line = best of 3 trials (peak), band = min–max",
         fontsize=9, color=TEXT2, ha="left")
fig.tight_layout(rect=(0, 0, 1, 0.93))
fig.savefig(os.path.join(here, "tpcc_ranking_modes_vs_workers.png"), dpi=200, facecolor=SURFACE)
print("wrote", os.path.join(here, "tpcc_ranking_modes_vs_workers.png"))
