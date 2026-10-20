#!/usr/bin/env python3
"""Publication-sized per-minute plots for every accepted or rejected headline."""
import csv
import json
from pathlib import Path
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

run=Path(sys.argv[1]); minute_file=run/'per_minute.csv'
rows=list(csv.DictReader(minute_file.open())) if minute_file.exists() else []
a=json.loads((run/'acceptance.json').read_text()); m=a.get('direct_metrics',{}); s=m.get('steady_state',{})
plt.rcParams.update({'font.size':8,'axes.spines.top':False,'axes.spines.right':False,'pdf.fonttype':42})
fig,ax=plt.subplots(figsize=(3.4,2.35),layout='constrained')
minutes=[float(r['offset_s'])/60+float(r['seconds'])/120 for r in rows]
values=[float(r['tpmc_equivalent']) for r in rows]
if rows:
    ax.plot(minutes,values,color='#0072B2',lw=1,label='Direct New-Order completions')
    ax.axhline(m['tpmc_equivalent_raw'],color='#D55E00',ls='--',lw=1,label='Interval mean')
else:
    # A gateway crash may leave no completion timeline. Preserve an explicit
    # unavailable-data artifact for this rejected attempt, never invent zeros.
    ax.text(.5,.5,'Minute throughput unavailable\nSee acceptance.json and gateway.log',
            transform=ax.transAxes,ha='center',va='center',fontsize=7)
    ax.set_xlim(0,120);ax.set_yticks([])
ax.set(xlabel='Minute in measurement interval',ylabel='TPC-C-derived\ntpmC-equivalent',
       title=f"{a['mode']} W{a['warehouses']} / {a['workers']} workers; "+('accepted' if a['acceptance']['accepted'] and 'stall_ge_10s_in_window' not in a['noise_flags'] else 'rejected'))
cv=f"{s['cv']:.3f}" if s.get('cv') is not None else 'unavailable'
trend=f"{s['fitted_end_to_start_change_fraction']:.1%}" if s.get('fitted_end_to_start_change_fraction') is not None else 'unavailable'
ax.text(.02,.03,f"Unaudited; CV={cv}; fitted change={trend}",transform=ax.transAxes,fontsize=6)
ax.grid(alpha=.2)
if rows:ax.legend(fontsize=6,loc='upper right')
for suffix in ('pdf','png'):fig.savefig(run/f'headline_per_minute.{suffix}',dpi=300)
plt.close(fig)
