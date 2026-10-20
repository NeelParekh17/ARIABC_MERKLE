#!/usr/bin/env python3
"""Direct gateway completion metrics, fixed duration, and per-minute stability.

Counts finish timestamps in [ramp, ramp+window), relative to first submission.
NewOrder includes reserved business rollbacks. Never extrapolate from DB growth.
"""
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

TYPES = ['new_order', 'payment', 'order_status', 'delivery', 'stock_level']
FUNCTIONS = dict(new_order_proc_exec=0, payment_proc_exec=1, payment_by_name_proc_exec=1,
                 order_status_proc_exec=2, order_status_by_name_exec=2,
                 delivery_proc_exec=3, stock_level_exec=4)
MIN_MIX = dict(payment=.43, order_status=.04, delivery=.04, stock_level=.04)


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values)*p)-1)] if values else None


def stability(counts, bin_s):
    rates = [n*60/bin_s for n in counts]
    mean = statistics.mean(rates) if rates else 0
    cv = statistics.stdev(rates)/mean if len(rates)>1 and mean else None
    x = [i*bin_s/60 for i in range(len(rates))]
    xbar = statistics.mean(x) if x else 0
    den = sum((v-xbar)**2 for v in x)
    slope = sum((a-xbar)*(b-mean) for a,b in zip(x,rates))/den if den else None
    trend = slope*(x[-1]-x[0])/mean if slope is not None and mean else None
    # 95% Newey-West interval, lag5 minutes for the configured checkpoint
    # cadence. A small but systematic linear trend must not pass simply because
    # its endpoint change is below the magnitude bound. Never remove any bin.
    lag=0; lower=upper=None; no_trend=False; standard_error=None; critical=None
    if len(rates)>=4 and den and slope is not None:
        lag=min(5,len(rates)-3)
        residual=[v-mean-slope*(a-xbar) for a,v in zip(x,rates)]
        score=[(a-xbar)*e for a,e in zip(x,residual)]
        meat=sum(v*v for v in score)
        for j in range(1,lag+1):
            meat+=2*(1-j/(lag+1))*sum(score[i]*score[i-j] for i in range(j,len(score)))
        standard_error=math.sqrt(max(0,meat)*len(rates)/(len(rates)-2))/den
        df=len(rates)-2; z=1.959963984540054
        critical=z+(z**3+z)/(4*df)+(5*z**5+16*z**3+3*z)/(96*df**2)+(3*z**7+19*z**5+17*z**3-15*z)/(384*df**3)
        lower,upper=slope-critical*standard_error,slope+critical*standard_error
        no_trend=lower<=0<=upper
    return dict(mean_tpmc_equivalent=mean, cv=cv, slope_tpmc_per_minute=slope,
                fitted_end_to_start_change_fraction=trend,
                slope_ci95_lower=lower,slope_ci95_upper=upper,slope_standard_error=standard_error,
                slope_ci_method='Newey-West HAC with small-sample t approximation',
                slope_hac_lag_bins=lag,slope_ci_critical_value=critical,no_significant_linear_trend=no_trend,
                cv_limit=.40, abs_fitted_change_limit=.20,
                passed=cv is not None and cv<=.40 and abs(trend)<=.20 and no_trend)


def classify(path):
    labels=bytearray(); counts=[0]*5; rollbacks=0
    archive=path.with_name(path.name+'.gz')
    with (path.open() if path.exists() else gzip.open(archive,'rt')) as f:
        for line in f:
            match=re.match(r'^SELECT public\.(\w+)\(',line)
            if not match or match[1] not in FUNCTIONS:
                raise ValueError('unknown workload transaction: '+line[:100])
            t=FUNCTIONS[match[1]]
            abort=t==0 and re.search(r'ARRAY\[[0-9,]*\b100001\]',line) is not None
            labels.append(t | (128 if abort else 0)); counts[t]+=1; rollbacks+=abort
    return labels,counts,rollbacks


def analyze(run, cfg):
    timeline=run/'tx_timeline.csv'
    labels, input_counts, input_aborts=classify(run/'workload.sql')
    n=cfg['count']; ramp=cfg['warmup_s']; duration=cfg['min_window_s']
    if len(labels)!=n:
        raise ValueError('workload count differs from configuration')
    bin_s=60 if duration>=120 else 10
    if duration % bin_s:
        raise ValueError('window must have complete throughput bins')
    start,end=ramp*1000,(ramp+duration)*1000
    seen=bytearray(n); window_counts=[0]*5; whole_counts=[0]*5
    latencies=[[] for _ in TYPES]; bins=[[0]*5 for _ in range(int(duration/bin_s))]
    seconds=[0]*int(duration)
    aborts=0; whole_aborts=0; rows=0; max_finish=max_submit=0.
    capacity_bins={}
    timeline_stream=timeline.open() if timeline.exists() else gzip.open(timeline.with_name(timeline.name+'.gz'),'rt')
    for row in csv.DictReader(timeline_stream):
        i=int(row['tx_idx']); sub=float(row['submit_ms']); fin=float(row['finish_ms']); lat=float(row['latency_ms'])
        if not (0<=i<n) or seen[i]:
            raise ValueError('out-of-range or duplicate transaction index')
        if not all(math.isfinite(v) and v>=0 for v in (sub,fin,lat)) or fin<sub or abs(fin-sub-lat)>.001:
            raise ValueError('invalid completion/latency timestamp')
        seen[i]=1; rows+=1; t=labels[i]&127; aborted=bool(labels[i]&128)
        whole_counts[t]+=1; whole_aborts+=aborted
        max_finish=max(max_finish,fin); max_submit=max(max_submit,sub)
        if fin>=start:
            ci=int((fin-start)/60000)
            capacity_bins.setdefault(ci,[0]*5)[t]+=1
        if start<=fin<end:
            window_counts[t]+=1; aborts+=aborted; latencies[t].append(lat)
            bins[int((fin-start)/(bin_s*1000))][t]+=1
            seconds[int((fin-start)/1000)]+=1
    if rows!=n or whole_counts!=input_counts or whole_aborts!=input_aborts:
        raise ValueError('missing completions or completion/workload count mismatch')
    total=sum(window_counts); no=window_counts[0]
    mix={t:window_counts[i]/total if total else None for i,t in enumerate(TYPES)}
    rt={}
    for i,t in enumerate(TYPES):
        values=sorted(latencies[i]); limit=20000 if t=='stock_level' else 5000
        q={f'p{p}_ms':values[max(0,math.ceil(len(values)*p/100)-1)] if values else None for p in (50,90,99)}
        mean=statistics.mean(values) if values else None
        rt[t]=dict(count=len(values),**q,p90_limit_ms=limit,p90_limit_pass=q['p90_ms'] is not None and q['p90_ms']<=limit,
                   mean_ms=mean,max_ms=values[-1] if values else None,
                   p90_at_least_mean_with_100ms_tolerance=q['p90_ms'] is not None and q['p90_ms']+100>mean)
    steady=stability([b[0] for b in bins],bin_s)
    capacity_series=[dict(offset_s=i*60,seconds=60,total_business_count=sum(b),total_business_tps=sum(b)/60)
                     for i,b in sorted(capacity_bins.items()) if start+(i+1)*60000<=max_submit]
    capacity_rates=[b['total_business_tps'] for b in capacity_series]
    capacity=dict(purpose='N sizing and ETA only; never substitutes for the fixed measurement metric',
                  boundary='complete post-ramp 60s bins ending before last submission',
                  mean_business_tps=statistics.mean(capacity_rates) if capacity_rates else total/duration,
                  max_business_tps=max(capacity_rates) if capacity_rates else total/duration,
                  min_business_tps=min(capacity_rates) if capacity_rates else total/duration,
                  complete_bins=len(capacity_series),series=capacity_series)
    longest=run_s=0
    for count in seconds:
        run_s=run_s+1 if count==0 else 0
        longest=max(longest,run_s)
    series=[dict(bin=i,offset_s=i*bin_s,seconds=bin_s,total_business_count=sum(b),
                 total_business_tps=sum(b)/bin_s,new_order_count=b[0],tpmc_equivalent=b[0]*60/bin_s,
                 **{t:b[j] for j,t in enumerate(TYPES)}) for i,b in enumerate(bins)]
    out=dict(analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),metric_label='TPC-C-derived, unaudited tpmC-equivalent',
        metric_source='per-transaction gateway finish timestamps; NewOrder includes expected rollbacks',
        origin='first gateway transaction submission; gateway steady clock',boundary='[start_ms,end_ms)',
        ramp_s=ramp,duration_s=duration,start_ms=start,end_ms=end,
        window_fully_covered_before_submission_ends=max_submit>end,
        ramp_at_least_5_minutes=ramp>=300,window_at_least_120_minutes=duration>=7200,
        max_submit_ms=max_submit,max_finish_ms=max_finish,completion_records=rows,
        input_counts=dict(zip(TYPES,input_counts)),whole_completed_counts=dict(zip(TYPES,whole_counts)),
        whole_expected_rollback_completions=whole_aborts,
        window_counts=dict(zip(TYPES,window_counts)),window_business_count=total,
        window_new_order_including_rollbacks=no,window_new_order_rollbacks=aborts,
        tpmc_equivalent_raw=no*60/duration,tpmc_equivalent=math.floor(no*60/duration),
        total_business_tps=total/duration,committed_only_nopm=(no-aborts)*60/duration,
        realised_mix=mix,mix_minimum_checks={t:mix[t] is not None and mix[t]>=v for t,v in MIN_MIX.items()},
        capacity_profile=capacity,response_times=rt,delivery='synchronous; no deferred Delivery queue',steady_state=steady,
        longest_zero_business_completion_gap_s=longest,zero_business_completion_seconds=seconds.count(0),
        throughput_series=series,throughput_bin_s=bin_s,
        timestamp_precision_ms=.0001,
        limitations=['no terminal keying or think time','unaudited','no eight-hour sustainability proof',
                     'fixed warehouse scale does not establish spec throughput per warehouse band',
                     'gateway observes batch completion acknowledgments; per-tx latencies include that wait'])
    # Conservative alignment for environmental samples/checkpoints, not counts:
    # progress starts before first submit and final progress follows last finish.
    # Rounded final elapsed gives an upper bound on their origin offset.
    raw=(run/'gateway.log').read_text()
    final=[line for line in raw.splitlines() if line.startswith('PROGRESS_GATEWAY_DET') and 'final=1' in line]
    if not final:
        raise ValueError('missing final progress for timestamp alignment')
    f=dict(re.findall(r'(\w+)=([^\s]+)',final[-1]))
    upper=max(0,float(f['elapsed_s'])+.051-max_finish/1000)
    out['progress_origin_offset_upper_bound_s']=upper
    out['clock_alignment_valid']=upper<=1.0
    (run/'direct_metrics.json').write_text(json.dumps(out,indent=2)+'\n')
    with (run/'per_minute.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(series[0]) if series else ['bin'])
        writer.writeheader();writer.writerows(series)
    with (run/'capacity_series.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=['offset_s','seconds','total_business_count','total_business_tps'])
        writer.writeheader();writer.writerows(capacity_series)
    with (run/'response_times.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=['type','count','p50_ms','p90_ms','p99_ms','p90_limit_ms','p90_limit_pass','mean_ms','max_ms','p90_at_least_mean_with_100ms_tolerance'])
        writer.writeheader();writer.writerows(dict(type=t,**r) for t,r in rt.items())
    return out


def window(run,cfg,rows):
    m=json.loads((run/'direct_metrics.json').read_text()) if (run/'direct_metrics.json').exists() else analyze(run,cfg)
    # Fixed predeclared one-second conservative bound allows streaming WAL
    # accounting before old segments recycle. Primary completion counts still
    # use exactly [ramp,ramp+duration). The alignment gate requires offset<=1s.
    lo=cfg['warmup_s']+1.0
    hi=cfg['warmup_s']+cfg['min_window_s']
    # SQL sample time follows the progress line. Include its recorded age and
    # elapsed rounding error at both bounds, rather than treating stale progress
    # as the SQL sample's timestamp. No completion-count metric uses this bound.
    valid=[r for r in rows if r.get('elapsed_s') is not None and r.get('progress_age_s') is not None
           and lo<=r['elapsed_s']+r['progress_age_s']-.051
           and r['elapsed_s']+r['progress_age_s']+.051<hi
           and r.get('wall_epoch') is not None and r.get('district_next') is not None and r.get('wal_lsn') and not r.get('error')]
    if len(valid)<2:
        raise ValueError('missing samples inside the fixed measurement interval')
    a,b=valid[0],valid[-1]
    return dict(start=a,end=b,duration_s=cfg['min_window_s'],sampled_duration_s=b['wall_epoch']-a['wall_epoch'],
        tps=m['total_business_tps'],tps_source='direct_gateway_completion_records',
        nopm=m['committed_only_nopm'],tpmc_equivalent=m['tpmc_equivalent'],
        completed_delta=m['window_business_count'],outcomes_delta=m['window_business_count'],
        committed_delta=m['window_business_count']-m['window_new_order_rollbacks'],
        user_aborts_delta=m['window_new_order_rollbacks'],new_orders_delta=m['window_new_order_including_rollbacks']-m['window_new_order_rollbacks'],
        longest_stall_s=m['longest_zero_business_completion_gap_s'],stall_seconds=m['zero_business_completion_seconds'],end_reason='fixed_duration',sample_count=len(valid),
        wal_note='WAL/host samples cover the conservative inner subinterval; primary metric interval uses exact gateway timestamps')
