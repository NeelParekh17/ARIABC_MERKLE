"""Plot already-collected recovery CSV measurements."""
import csv,pathlib,sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
B=pathlib.Path(__file__).parent
ROOT=B.parent.parent
rows=list(csv.DictReader((B/'comparison.csv').open()))
names=['A','B','C','M_mixed','L_mix_prio']
labels=['A\nBaseline','B\nNo fault','C\nFollower\nupdate','M\nFollower\nmixed','L\nLeader\nmixed']
oldids=['cluster4_final_A_baseline_180113','cluster4_final_B_nofault_180113','cluster4_final_C_fault_180113','cluster4_test_mixed_utkarsh_184953','cluster4_test_leader_mixed_192316']
old={r['run_id']:r for r in rows if r['dataset']=='published'}
new={(r['group'],r['scenario']):r for r in rows if r['dataset']=='new'}
assert len(new)==8 and all(r['evidence_status']=='PASS' for r in new.values())
fig,axs=plt.subplots(2,2,figsize=(12.5,9.7))
colors=['#576d85','#267f77','#cb8543']
width=.25;x=np.arange(5)
for ax,(field,title,ylabel) in zip(axs.flat,[('tps_majority_visible','Majority completion throughput','Transactions / second'),('cut_ms','Recovery cut','Milliseconds'),('repair_ms','Incremental repair','Milliseconds'),('catchup_ms','Catchup / replay','Milliseconds')]):
    vals=[float(old[i][field]) for i in oldids]
    ax.bar(x-width,vals,width,label='Published F4/S32/M8',color=colors[0])
    vals=[float(new['f32s1024',n][field]) for n in names]
    ax.bar(x,vals,width,label='Current F32/S1024/M256',color=colors[1])
    vals=[float(new['f4s32',n][field]) if ('f4s32',n) in new else np.nan for n in names]
    ax.bar(x+width,vals,width,label='Current F4/S32/M8 control',color=colors[2])
    ax.set_title(title,loc='left',fontweight='bold');ax.set_ylabel(ylabel)
    ax.set_xticks(x,labels);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    ax.spines[['top','right']].set_visible(False)
fig.suptitle('Online recovery: published results and current synchronous Merkle code',fontsize=15,fontweight='bold')
fig.legend(*axs[0,0].get_legend_handles_labels(), fontsize=9, loc='upper center', bbox_to_anchor=(.5,.942), ncol=3, frameon=False)
fig.subplots_adjust(top=.86,bottom=.18,hspace=.52,wspace=.23)
fig.supxlabel('160,000 YCSB transactions · 96 lanes · one trial per scenario and geometry\nNew trials use SERIALIZABLE; B and L have no control trial.\nControl M: eight serialization retries delayed the fault; replay was 80 entries vs 354 in new M.',fontsize=10)
out=ROOT/'Final_Results/ONLINE_RECOVERY/graphs/s1024_comparison.png'
assert out.read_bytes()==(B/'s1024_comparison.png').read_bytes()
(B/'s1024_comparison_v1.png').write_bytes(out.read_bytes())
fig.savefig(B/'s1024_comparison.png',dpi=180)
out.write_bytes((B/'s1024_comparison.png').read_bytes())
print(out)
