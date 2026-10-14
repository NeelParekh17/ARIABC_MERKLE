/* =========================================================
   EVIDENCE — data-driven sections. Every number here is read from the JSON that
   scripts/build_architecture_explorer.py embeds from the curated CSVs; nothing is
   retyped. Empty CSV cells arrive as null and are shown as "n/a", never as 0.
   ========================================================= */
const readJSON=id=>{const el=document.getElementById(id);try{return el?JSON.parse(el.textContent):null}catch(e){return null}};
const RESULTS=readJSON('explorer-results')||{};
const SOURCES=readJSON('explorer-sources')||{};
const na=(v,f=x=>fmt(x))=>v==null||v===''?'<span class="muted">n/a</span>':f(v);
const pct=v=>(v>=0?'+':'−')+Math.abs(v).toFixed(2)+'%';
const MODE3=[['pg','pg (SERIALIZABLE)',K('#3987e5')],['bcdb_det','det',K('#d95926')],['bcdb_merkle','det + Merkle',K('#199e70')]];
const OOMCFG=[['a','0.0','A · uniform'],['a','0.99','A · θ 0.99'],['a','1.2','A · θ 1.2'],['b','0.99','B · θ 0.99'],['c','0.99','C · θ 0.99'],['d','0.99','D · θ 0.99'],['f','0.99','F · θ 0.99']];
const WLNAME={a:'YCSB-A: 50% read / 50% update',b:'YCSB-B: 95% read / 5% update',c:'YCSB-C: read only',d:'YCSB-D: read latest / insert',f:'YCSB-F: read-modify-write'};

/* median line + min–max band + individual trials, categorical x */
function bandChart(el,tipEl,{xs,series,xlab,ylab,yfmt=v=>fmt(v),height=360,width=1080,ref=null,ymin=0}){
 const W=width,H=height,pl=64,pr=150,pt=16,pb=40;const all=series.flatMap(s=>s.pts.flatMap(p=>[p.hi??p.v,p.v,...(p.dots||[])])).filter(v=>v!=null);
 let lo=ymin==null?Math.min(...all,ref??Infinity):ymin,hiV=Math.max(...all,ref??-Infinity);if(ymin==null){const pad=(hiV-lo)*.12||.05;lo-=pad;hiV+=pad}
 const yt=ymin==null?(()=>{const st=axisTicks(hiV-lo)[1]||.05;const t=[];for(let v=Math.ceil(lo/st)*st;v<=hiV+1e-9;v+=st)t.push(+v.toFixed(6));return t})():axisTicks(hiV);const y0=ymin==null?lo:0,y1=ymin==null?hiV:yt[yt.length-1];
 const X=i=>pl+(i+.5)/xs.length*(W-pl-pr),Y=v=>pt+(1-(v-y0)/(y1-y0))*(H-pt-pb);
 let h=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(ylab)} by ${esc(xlab)}">`;
 yt.forEach(v=>h+=`<line class="gridl" x1="${pl}" x2="${W-pr}" y1="${Y(v)}" y2="${Y(v)}"/><text class="ax" x="${pl-8}" y="${Y(v)+4}" text-anchor="end">${yfmt(v)}</text>`);
 if(ref!=null)h+=`<line x1="${pl}" x2="${W-pr}" y1="${Y(ref)}" y2="${Y(ref)}" style="stroke:var(--ink3);stroke-dasharray:5 4;stroke-width:1.4"/><text class="ax" x="${W-pr+6}" y="${Y(ref)+4}">${yfmt(ref)}</text>`;
 xs.forEach((x,i)=>h+=`<text class="ax" x="${X(i)}" y="${H-pb+18}" text-anchor="middle">${esc(x)}</text>`);
 h+=`<text class="axl" x="${(pl+W-pr)/2}" y="${H-4}" text-anchor="middle">${esc(xlab)}</text><text class="axl" transform="translate(14 ${(pt+H-pb)/2}) rotate(-90)" text-anchor="middle">${esc(ylab)}</text>`;
 const ends=[];
 series.forEach(s=>{const P=s.pts;const okp=P.map((p,i)=>[p,i]).filter(([p])=>p.v!=null);
  if(okp.some(([p])=>p.lo!=null)){const up=okp.map(([p,i])=>`${X(i)},${Y(p.hi)}`),dn=okp.slice().reverse().map(([p,i])=>`${X(i)},${Y(p.lo)}`);h+=`<polygon points="${up.concat(dn).join(' ')}" fill="${s.c}" opacity=".13"/>`;okp.forEach(([p,i])=>h+=`<line x1="${X(i)}" x2="${X(i)}" y1="${Y(p.lo)}" y2="${Y(p.hi)}" stroke="${s.c}" stroke-width="1.4" opacity=".7"/>`)}
  h+=`<path d="${okp.map(([p,i],j)=>`${j?'L':'M'}${X(i).toFixed(1)} ${Y(p.v).toFixed(1)}`).join('')}" fill="none" stroke="${s.c}" stroke-width="${s.dash?1.6:2.4}" ${s.dash?'stroke-dasharray="6 5"':''} stroke-linejoin="round"/>`;
  okp.forEach(([p,i])=>{(p.dots||[]).forEach((d,k)=>{if(d!=null)h+=`<circle cx="${X(i)+(k-1)*5}" cy="${Y(d)}" r="2.3" fill="${s.c}" opacity=".55"/>`});h+=`<circle cx="${X(i)}" cy="${Y(p.v)}" r="${s.dash?3:4.5}" fill="${s.dash?'var(--card)':s.c}" stroke="${s.c}" stroke-width="2"/>`});
  const last=okp[okp.length-1];if(last)ends.push({y:Y(last[0].v),n:s.n,c:s.c})});
 ends.sort((a,b)=>a.y-b.y);for(let i=1;i<ends.length;i++)if(ends[i].y-ends[i-1].y<15)ends[i].y=ends[i-1].y+15;
 ends.forEach(e=>h+=`<line x1="${W-pr+6}" x2="${W-pr+14}" y1="${e.y}" y2="${e.y}" stroke="${e.c}" stroke-width="2"/><text class="dl" x="${W-pr+18}" y="${e.y+4}">${esc(e.n)}</text>`);
 h+=`<rect class="hitarea" x="${pl}" y="${pt}" width="${W-pl-pr}" height="${H-pt-pb}" fill="transparent"/></svg>`;el.innerHTML=h;
 const svg=el.querySelector('svg'),hit=el.querySelector('.hitarea');
 hit.addEventListener('mousemove',e=>{const r=svg.getBoundingClientRect(),sx=(e.clientX-r.left)/r.width*W;const i=Math.max(0,Math.min(xs.length-1,Math.floor((sx-pl)/(W-pl-pr)*xs.length)));
  tipEl.innerHTML=`<b>${esc(xlab)} ${esc(xs[i])}</b>`+series.map(s=>{const p=s.pts[i];return`<div class="r"><span><i style="background:${s.c}"></i>${esc(s.n)}</span><span>${p.v==null?'n/a':yfmt(p.v)}${p.lo!=null?` <small class="muted">(${yfmt(p.lo)}–${yfmt(p.hi)})</small>`:''}</span></div>`}).join('')+(series[0].pts[i].note?`<p>${series[0].pts[i].note}</p>`:'');
  tipEl.style.display='block';const px=X(i)/W*r.width;tipEl.style.left=(px>r.width*.55?px-tipEl.offsetWidth-16:px+16)+'px';tipEl.style.top='8px'});
 hit.addEventListener('mouseleave',()=>tipEl.style.display='none')}

/* ---------------- 08 RESULTS: OOM 100M paper campaign ---------------- */
let oomCfg=1,oomView='tps';
function oomPoint(w,s,W,m){return(RESULTS.oom&&RESULTS.oom.points||[]).find(p=>p.workload===w&&p.skew===s&&p.workers===W&&p.mode===m)}
function renderOom(){const O=RESULTS.oom;if(!O||!O.points){$('#oom-svg').innerHTML='<p class="small">OOM data not embedded. Run scripts/build_architecture_explorer.py.</p>';return}
 const [w,s,lab]=OOMCFG[oomCfg],Ws=[1,4,8,16],v2=$('#oom-v2').checked;$('#oom-cap').textContent=`${WLNAME[w]} · Zipf θ ${s} · 20,000 statements per case · median of 3 trials, band = min–max`;
 if(oomView==='tps'){const ser=[];MODE3.forEach(([m,n,c])=>{ser.push({n,c,pts:Ws.map(W=>{const p=oomPoint(w,s,W,m);return p?{v:p.median,lo:p.min,hi:p.max,dots:p.trials}:{v:null}})});
   if(v2)ser.push({n:n+' · v2 (historical)',c,dash:true,pts:Ws.map(W=>{const p=oomPoint(w,s,W,m);return{v:p?p.previous_v2_tps:null}})})});
  if($('#oom-xor').checked)ser.push({n:'det + Merkle · XOR (historical)',c:K('#9085e9'),dash:true,pts:Ws.map(W=>{const p=oomPoint(w,s,W,'bcdb_merkle');return{v:p&&p.xor?p.xor.median:null}})});
  bandChart($('#oom-svg'),$('#oom-tip'),{xs:Ws.map(W=>'W '+W),series:ser,xlab:'Client workers',ylab:'Throughput (tx/s)'})}
 else{const R=(O.ratios||[]).filter(r=>r.workload===w&&r.skew===s);const C=[['det_over_pg','det ÷ pg',K('#d95926')],['merkle_over_det','Merkle ÷ det',K('#199e70')],['merkle_over_pg','Merkle ÷ pg',K('#9085e9')]];
  bandChart($('#oom-svg'),$('#oom-tip'),{xs:Ws.map(W=>'W '+W),series:C.map(([k,n,c])=>({n,c,pts:Ws.map(W=>{const r=R.find(x=>x.workers===W);return{v:r?r[k]:null}})})),xlab:'Client workers',ylab:'Ratio of medians',yfmt:v=>v.toFixed(2)+'×',ref:1,ymin:null})}
 $('#oom-legend').innerHTML=MODE3.map(([m,n,c])=>`<span><i class="sw" style="background:${c}"></i>${n}</span>`).join('')+(oomView==='tps'&&(v2||$('#oom-xor').checked)?'<span>dashed = historical series (v2 single campaign; XOR Merkle of paper_20261010)</span>':'')+'<span>band and whisker = min–max of 3 trials · small dots = trials</span>';
 const rows=[];Ws.forEach(W=>MODE3.forEach(([m,n])=>{const p=oomPoint(w,s,W,m);if(p)rows.push(`<tr><td>W ${W}</td><td>${n}<br><small class="muted">${esc(p.source||'')}</small></td><td class="num">${fmt(p.median,1)}</td><td class="num">${fmt(p.min,1)} – ${fmt(p.max,1)}</td><td class="num">${na(p.cv_pct,x=>x.toFixed(2)+'%')}</td><td class="num">${p.trials.map(t=>na(t,x=>fmt(x,0))).join(' · ')}</td><td class="num">${na(p.retry_median,x=>fmt(x,0))}</td><td class="num">${na(p.previous_v2_tps,x=>fmt(x,0))}</td><td class="num">${p.xor?fmt(p.xor.median,0):'—'}</td></tr>`)}));
 $('#oom-table').innerHTML=`<table class="t"><thead><tr><th>Workers</th><th>Mode</th><th class="num">Median TPS</th><th class="num">Min – max</th><th class="num">CV</th><th class="num">Trials</th><th class="num">Retries (median)</th><th class="num">v2 TPS (historical)</th><th class="num">XOR Merkle (historical)</th></tr></thead><tbody>${rows.join('')}</tbody></table>`}
function initOom(){const O=RESULTS.oom;if(!O)return;
 $('#oom-cfg').innerHTML=OOMCFG.map((c,i)=>`<button data-i="${i}" aria-pressed="${i===oomCfg}">${esc(c[2])}</button>`).join('');
 $('#oom-cfg').onclick=e=>{const b=e.target.closest('[data-i]');if(!b)return;oomCfg=+b.dataset.i;$$('#oom-cfg button').forEach(x=>x.setAttribute('aria-pressed',String(x===b)));renderOom()};
 $('#oom-view').onclick=e=>{const b=e.target.closest('[data-v]');if(!b)return;oomView=b.dataset.v;$$('#oom-view button').forEach(x=>x.setAttribute('aria-pressed',String(x===b)));$('#oom-v2').disabled=$('#oom-xor').disabled=oomView!=='tps';renderOom()};
 $('#oom-v2').onchange=renderOom;$('#oom-xor').onchange=renderOom;$('#oom-tbl').onclick=()=>{const t=$('#oom-table');t.hidden=!t.hidden;$('#oom-tbl').textContent=t.hidden?'Show table':'Hide table'};
 const H=O.headline||{},A=O.audit||{},M=A.by_mode||{},hv=k=>H[k]||{},rng=k=>`range ${na(hv(k).min,x=>x.toFixed(2))}–${na(hv(k).max,x=>x.toFixed(2))}`;
 const dc=O.det_counters||{};
 $('#oom-tiles').innerHTML=[
  ['Accepted cases',`${na(A.cases)} + ${na(A.smoke_cases)}`,`PG ${na(M.pg)} · det ${na(M.bcdb_det)} · Merkle ${na(M.bcdb_merkle)} + smoke`,'ok'],
  ['Terminal results',na(A.terminal_results),`divergence ${na(A.divergence)} · permanent failures ${na(A.permanent_failures)}`],
  ['Merkle verify PASS',`${na(A.merkle_pass)} / ${na(A.merkle_cases)}`,'every AdHash Merkle case'],
  ['det ÷ pg (median of 28)',na(hv('det_over_pg').median,x=>x.toFixed(3)+'×'),rng('det_over_pg')],
  ['Merkle ÷ det (median)',na(hv('merkle_over_det').median,x=>x.toFixed(3)+'×'),rng('merkle_over_det')],
  ['Merkle ÷ pg (median)',na(hv('merkle_over_pg').median,x=>x.toFixed(3)+'×'),rng('merkle_over_pg')],
  ['AdHash ÷ XOR Merkle',na(hv('adhash_over_xor_merkle').median,x=>x.toFixed(3)+'×'),rng('adhash_over_xor_merkle')],
  ['Max CV',na(O.cv_max_pct,x=>x.toFixed(2)+'%'),`${na(O.cv_gt10_count)} points above 10%`],
  ['Det trace counters',dc.trials_with_values?`${dc.trials_with_values}/${dc.trials_checked}`:'unavailable',`${(dc.labels||[]).join(', ')} · not zero`,'warn']
 ].map(([a,b,c,k])=>`<div class="tile ${k||''}"><small>${a}</small><strong>${b}</strong><em>${c}</em></div>`).join('');
 $('#oom-meta').innerHTML=`PG and det rows: engine <code>${esc((O.frozen_commit||'').slice(0,8))}</code>, ${esc(O.campaign_start_utc||'')} → ${esc(O.campaign_end_utc||'')}. Merkle rows: AdHash engine <code>${esc((O.merkle_commit||'').slice(0,8))}</code> (= merged 6b19ff92), re-measured 2026-10-12/13 on a REINDEXed format-11 baseline. Inputs <code>${esc(O.source_dir||'')}</code>; XOR-era archive <code>${esc(O.history_dir||'')}</code>.`;
 $('#oom-settings').innerHTML=`<thead><tr><th>Setting</th><th>Value</th><th>Read from</th></tr></thead><tbody>${(O.settings||[]).map(([a,b,c])=>`<tr><td>${esc(a)}</td><td class="mono">${b===''||b==null?'<span class="muted">n/a</span>':esc(b)}</td><td class="small mono">${esc(c)}</td></tr>`).join('')}${(O.binaries||[]).map(([b,h])=>`<tr><td>binary ${esc(b)}</td><td class="mono" title="${esc(h)}">sha256 ${esc(h.slice(0,16))}…</td><td class="small mono">binary_hashes.csv</td></tr>`).join('')}</tbody>`;
 renderOom()}

/* ---------------- 08 RESULTS: AdHash TPS check ---------------- */
function initAdhash(){const D=RESULTS.adhash;if(!D||!D.points){$('#adh-svg').innerHTML='<p class="small">AdHash comparison not embedded.</p>';return}
 const P=D.points,W=1080,H=300,pl=64,pr=24,pt=18,pb=70,lo=.94,hi=1.06,X=i=>pl+(i+.5)/P.length*(W-pl-pr),Y=v=>pt+(1-(Math.max(lo,Math.min(hi,v))-lo)/(hi-lo))*(H-pt-pb);
 let h=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="AdHash Merkle throughput relative to the published XOR campaign">`;
 [.94,.96,.98,1,1.02,1.04,1.06].forEach(v=>h+=`<line class="gridl" x1="${pl}" x2="${W-pr}" y1="${Y(v)}" y2="${Y(v)}" ${v===1?'style="stroke:var(--ink3);stroke-dasharray:5 4"':''}/><text class="ax" x="${pl-8}" y="${Y(v)+4}" text-anchor="end">${v.toFixed(2)}×</text>`);
 let prev=null;P.forEach((p,i)=>{const g=p.workload+' θ'+p.skew;if(g!==prev){h+=`<text class="axl" x="${X(i)-6}" y="${H-pb+44}">${esc(g)}</text>`;if(i)h+=`<line x1="${X(i)-W/P.length/2}" x2="${X(i)-W/P.length/2}" y1="${pt}" y2="${H-pb+30}" style="stroke:var(--rule)"/>`;prev=g}
  const up=p.merkle_ratio>=1,c=up?K('#199e70'):K('#d95926');h+=`<rect class="adhb" data-i="${i}" x="${X(i)-9}" y="${Math.min(Y(1),Y(p.merkle_ratio))}" width="18" height="${Math.max(1.5,Math.abs(Y(1)-Y(p.merkle_ratio)))}" rx="3" fill="${c}" opacity=".85"/>`;
  if(p.new_min!=null&&p.published_median)h+=`<line x1="${X(i)}" x2="${X(i)}" y1="${Y(p.new_min/p.published_median)}" y2="${Y(p.new_max/p.published_median)}" style="stroke:var(--ink2);stroke-width:1.2"/>`;
  if(p.adjusted!=null)h+=`<circle cx="${X(i)}" cy="${Y(p.adjusted)}" r="3.2" fill="var(--card)" stroke="${K('#9085e9')}" stroke-width="2"/>`;
  h+=`<text class="ax" x="${X(i)}" y="${H-pb+16}" text-anchor="middle">${p.workers}</text>`});
 h+=`<text class="axl" x="${pl}" y="${H-6}">workers per configuration · bars: new AdHash median ÷ published Merkle median · line: new min–max ÷ published median · ○ drift-adjusted (÷ det control)</text></svg>`;
 $('#adh-svg').innerHTML=h;
 const tip=$('#adh-tip');$('#adh-svg').onmousemove=e=>{const b=e.target.closest('.adhb');if(!b){tip.style.display='none';return}const p=P[+b.dataset.i],box=$('#adh-svg').getBoundingClientRect();
  tip.innerHTML=`<b>${esc(p.workload)} θ${esc(p.skew)} · W ${p.workers}</b><div class="r"><span>AdHash Merkle (n=${p.new_n})</span><span>${fmt(p.new_median,0)} tx/s</span></div><div class="r"><span>published XOR Merkle</span><span>${fmt(p.published_median,0)} tx/s</span></div><div class="r"><span>ratio</span><span>${p.merkle_ratio.toFixed(4)}×</span></div><div class="r"><span>det control (1 trial)</span><span>${na(p.det_ratio,x=>x.toFixed(3)+'×')}</span></div><div class="r"><span>drift-adjusted</span><span>${na(p.adjusted,x=>x.toFixed(3)+'×')}</span></div>`;
  tip.style.display='block';tip.style.left=Math.min(box.width-240,e.clientX-box.left+12)+'px';tip.style.top=(e.clientY-box.top+12)+'px'};$('#adh-svg').onmouseleave=()=>tip.style.display='none';
 const S=D.summary||{};$('#adh-tiles').innerHTML=[['Accepted',`${na(S.accepted)}/${na(S.cases)}`,'84 Merkle + 28 det cases','ok'],['Complete points',`${na(S.complete)}/${P.length}`,'3 Merkle trials each'],['Median ratio',na(S.median_ratio,x=>x.toFixed(4)+'×'),`range ${na(S.min_ratio,x=>x.toFixed(3))}–${na(S.max_ratio,x=>x.toFixed(3))}`],['Median drift-adjusted',na(S.median_adjusted,x=>x.toFixed(4)+'×'),'diagnostic, not causal']].map(([a,b,c,k])=>`<div class="tile ${k||''}"><small>${a}</small><strong>${b}</strong><em>${c}</em></div>`).join('')}

/* ---------------- 06 RECOVERY: paper campaign matrix ---------------- */
const RVSC={A:'baseline · recovery off',B:'recovery on · no fault',C:'UPDATE on follower 4',M_passive:'UPDATE on 4 · passive Merkle compare',M_active:'UPDATE on 4 · active divergence',M_mixed:'mixed UPDATE/DELETE/INSERT on 4',L:'UPDATE on leader 1',L_mix:'mixed faults on leader 1',C_deep:'as C, with deep tracing'};
let camSel='C';
function camRun(s){const R=RESULTS.recovery;const prim=R.attempts.filter(a=>a.scenario===s.scenario&&a.primary);return prim.find(a=>a.total_recovery_ms===s.median_total_recovery_ms)||prim[0]}
function renderCampaign(){const R=RESULTS.recovery;if(!R||!R.scenarios){$('#cam-svg').innerHTML='<p class="small">Recovery campaign not embedded.</p>';return}
 const S=R.scenarios,W=1080,rowH=38,pl=210,pr=250,pt=26,H=pt+S.length*rowH+34,mx=14000,x=v=>pl+v/mx*(W-pl-pr);
 const C=[['cut_ms','CUT',K('#ffc87b')],['repair_ms','repair',K('#94e5b1')],['catchup_ms','catch-up',K('#70dfef')],['other','other',K('#8ea2ba')]];
 let h=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Recovery time and throughput per scenario">`;
 [0,2000,4000,6000,8000,10000,12000,14000].forEach(v=>h+=`<line class="gridl" x1="${x(v)}" x2="${x(v)}" y1="${pt-6}" y2="${H-28}"/><text class="ax" x="${x(v)}" y="${H-12}" text-anchor="middle">${v/1000} s</text>`);
 h+=`<text class="axl" x="${pl}" y="14">recovery phases of the median-total run · whisker = min–max total of 3 runs</text><text class="axl" x="${W-pr+18}" y="14">throughput vs A (median, min–max)</text>`;
 const tx=v=>W-pr+30+(v+3)/4.5*(pr-60);[-3,-2,-1,0,1].forEach(v=>h+=`<line x1="${tx(v)}" x2="${tx(v)}" y1="${pt-6}" y2="${H-28}" style="stroke:var(--rule);${v===0?'stroke:var(--ink4);stroke-dasharray:4 3':''}"/><text class="ax" x="${tx(v)}" y="${H-12}" text-anchor="middle">${v>0?'+':''}${v}%</text>`);
 const base=(S.find(s=>s.scenario==='A')||{}).median_tps;
 S.forEach((s,i)=>{const y=pt+i*rowH,sel=s.scenario===camSel;h+=`<g class="camrow" data-s="${esc(s.scenario)}" style="cursor:pointer"><rect x="2" y="${y-4}" width="${W-4}" height="${rowH-4}" rx="8" fill="${sel?'var(--accent-soft)':'transparent'}"/>`;
  h+=`<text x="12" y="${y+12}" style="font:700 12.5px var(--mono);fill:var(--ink)">${esc(s.scenario)}</text><text x="12" y="${y+26}" style="font:11px var(--font);fill:var(--ink3)">${esc(RVSC[s.scenario]||'')}</text>`;
  const run=camRun(s);if(!s.median_total_recovery_ms){h+=`<text x="${pl+6}" y="${y+18}" style="font:11.5px var(--font);fill:var(--ink3)">no recovery in this scenario</text>`}
  else if(run){let a=0;const parts={cut_ms:run.cut_ms,repair_ms:run.repair_ms,catchup_ms:run.catchup_ms};parts.other=Math.max(0,run.total_recovery_ms-run.cut_ms-run.repair_ms-run.catchup_ms);
   C.forEach(([k,n,c])=>{const v=parts[k]||0;if(v<=0)return;h+=`<rect x="${x(a)}" y="${y+2}" width="${Math.max(2,x(a+v)-x(a)-1)}" height="18" rx="3" fill="${c}"/>`;a+=v});
   h+=`<line x1="${x(s.min_total_recovery_ms)}" x2="${x(s.max_total_recovery_ms)}" y1="${y+27}" y2="${y+27}" style="stroke:var(--ink2);stroke-width:1.5"/><line x1="${x(s.min_total_recovery_ms)}" x2="${x(s.min_total_recovery_ms)}" y1="${y+23}" y2="${y+31}" style="stroke:var(--ink2)"/><line x1="${x(s.max_total_recovery_ms)}" x2="${x(s.max_total_recovery_ms)}" y1="${y+23}" y2="${y+31}" style="stroke:var(--ink2)"/>`;
   h+=`<text x="${x(a)+6}" y="${y+16}" style="font:650 11px var(--mono);fill:var(--ink)">${fmt(run.total_recovery_ms)} ms</text>`}
  const d=v=>(v/base-1)*100;h+=`<line x1="${tx(d(s.min_tps))}" x2="${tx(d(s.max_tps))}" y1="${y+11}" y2="${y+11}" style="stroke:var(--ink3);stroke-width:2"/><circle cx="${tx(d(s.median_tps))}" cy="${y+11}" r="5" fill="${K('#3987e5')}"/><text x="${W-8}" y="${y+15}" text-anchor="end" style="font:650 11px var(--mono);fill:var(--ink2)">${fmt(s.median_tps,0)}</text></g>`});
 h+='</svg>';$('#cam-svg').innerHTML=h;$('#cam-legend').innerHTML=C.map(([k,n,c])=>`<span><i class="sw" style="background:${c}"></i>${n}</span>`).join('')+'<span>“other” = detect→quarantine, drain and rebase</span><span>click a scenario for its runs</span>';
 const sc=S.find(s=>s.scenario===camSel),runs=R.attempts.filter(a=>a.scenario===camSel).sort((a,b)=>a.rep-b.rep||a.attempt-b.attempt);
 $('#cam-head').innerHTML=`<h3>${esc(camSel)} · ${esc(RVSC[camSel]||'')}</h3><p class="small">Median ${fmt(sc.median_tps,0)} TPS (${fmt(sc.min_tps,0)}–${fmt(sc.max_tps,0)}), ${sc.median_delta_vs_A_pct==null?'n/a':pct(sc.median_delta_vs_A_pct)} vs A · max completion gap ${na(sc.max_max_gap_ms,x=>x.toFixed(1))} ms · empty 100 ms bins ${na(sc.max_empty_100ms_buckets)} (worst run)</p>`;
 $('#cam-runs').innerHTML=`<table class="t"><thead><tr><th>Run</th><th>Status</th><th class="num">TPS</th><th>Detected by</th><th>Target ← donor</th><th class="num">det seq</th><th class="num">CUT / repair / catch-up ms</th><th class="num">Partitions</th><th class="num">Upserted / deleted</th><th class="num">Full copies</th><th>RECOVERY_EVENT</th></tr></thead><tbody>${runs.map(a=>{const e=a.event;return`<tr${a.primary?'':' class="hi"'}><td class="mono">r${a.rep} a${a.attempt}${a.primary?'':' · extra'}</td><td>${esc(a.status)}${a.storage_flag?` <span class="tag bad">${esc(a.storage_flag)}</span>`:''}</td><td class="num">${fmt(a.tps,0)}</td><td>${na(a.detected_by,esc)}</td><td>${a.target_node?`node ${esc(a.target_node)} ← ${esc(a.reference_node||'?')}`:'<span class="muted">—</span>'}</td><td class="num">${na(a.recovery_at_det_seq)}</td><td class="num">${a.total_recovery_ms?`${fmt(a.cut_ms)} / ${fmt(a.repair_ms)} / ${fmt(a.catchup_ms)}`:'—'}</td><td class="num">${a.total_recovery_ms?na(a.mismatched_partitions):'—'}</td><td class="num">${a.total_recovery_ms?`${na(a.rows_upserted)} / ${na(a.rows_deleted)}`:'—'}</td><td class="num">${na(a.full_copies)}</td><td class="small mono">${e?`L=${e.L} B=${fmt(e.B)} replay ${e.replay_from}→${e.replay_target}`:'—'}</td></tr>`}).join('')}</tbody></table>`}
function initCampaign(){const R=RESULTS.recovery;if(!R)return;const V=R.validation||{};
 $('#cam-tiles').innerHTML=[['Accepted attempts',`${na(V.accepted)} / ${na(V.attempts)}`,`${na(V.primary)} primary (9 × 3) + extra`,'ok'],['Quorum completions',V.all_quorum_160000?'160,000 × all':'check CSV','every accepted run'],['Phase 8 Merkle',V.all_phase8_pass?'PASS × all':'check CSV',`${(V.roots||[]).length} distinct final root`],['Full table copies',na(V.full_copies_total),'all repairs were sparse'],['Events parsed',na(V.events_parsed),'RECOVERY_EVENT lines read from run logs']].map(([a,b,c,k])=>`<div class="tile ${k||''}"><small>${a}</small><strong>${b}</strong><em>${c}</em></div>`).join('');
 $('#cam-meta').innerHTML=`Frozen engine <code>${esc((R.frozen_commit||'').slice(0,8))}</code>, before AdHash: digests and roots in these runs are XOR aggregates. Inputs <code>${esc(R.source_dir||'')}</code>.`;
 $('#cam-svg').addEventListener('click',e=>{const g=e.target.closest('.camrow');if(!g)return;camSel=g.dataset.s;renderCampaign()});renderCampaign()}

/* ---------------- 05 MERKLE: why AdHash replaced XOR ---------------- */
const ADH={n:2,other:false};
function renderAdhDemo(){const H=h32('ARIAMRKL|1|3|7|amber|42'),O=h32('ARIAMRKL|1|3|8|birch|17');let x=0,a=0;for(let i=0;i<ADH.n;i++){x=(x^H)>>>0;a=add32(a,H)}if(ADH.other){x=(x^O)>>>0;a=add32(a,O)}const cnt=ADH.n+(ADH.other?1:0);
 const empty=ADH.other?O:0;const cell=(t,v,ok)=>`<div class="tile ${ok}"><small>${t}</small><strong class="mono">${hx8(v)}</strong></div>`;
 $('#adhd-rows').innerHTML=Array.from({length:ADH.n},()=>`<span class="tag code">(7, amber, 42) · H ${hx4(H)}…</span>`).join('')+(ADH.other?`<span class="tag">(8, birch, 17) · H ${hx4(O)}…</span>`:'')+(!cnt?'<span class="small">empty table</span>':'');
 $('#adhd-out').innerHTML=cell('XOR aggregate (format ≤ 10)',x,ADH.n%2===0&&ADH.n>0?'bad':'')+cell('AdHash: Σ mod 2^32 here, 2^256 in the engine',a,'ok')+`<div class="tile"><small>tuple_count</small><strong>${cnt}</strong></div>`;
 $('#adhd-cap').innerHTML=ADH.n>0&&ADH.n%2===0?`With ${ADH.n} identical rows XOR gives <b>${hx8(x)}</b>, the same as ${ADH.other?'the single distinct row alone':'an empty table'}: identical pairs cancel. AdHash keeps ${ADH.n}·H. Before 4921f6a5 the count check did not exist, so merkle_verify returned t for both states.`:`An odd number of copies leaves one H under XOR — the same as a single copy. AdHash distinguishes ${ADH.n} copies from 1.`;
 $('#adhd-minus').disabled=ADH.n===0}
function initAdhDemo(){$('#adhd-plus').onclick=()=>{ADH.n=Math.min(6,ADH.n+1);renderAdhDemo()};$('#adhd-minus').onclick=()=>{ADH.n=Math.max(0,ADH.n-1);renderAdhDemo()};$('#adhd-other').onchange=e=>{ADH.other=e.target.checked;renderAdhDemo()};renderAdhDemo()}

/* ---------------- 10 SOURCES & FRESHNESS ---------------- */
const SETTINGS=[
 ['Det conflict tracking','bcdb_dt_conflict_tracking','off','globals.c','on — validated by the cluster runner (reference run) and set by the TPC-C harness','code'],
 ['Serial gate wait','bcdb_serial_gate_mode','0 = poll','globals.c','1 = condition variable (cluster reference run)','code'],
 ['Gate source','bcdb_serial_gate_source','published_max','globals.c','published_max','code'],
 ['Write-set map switch','bcdb_dt_hashtab_switch_threshold','1,500 ids','globals.h','1,500','code'],
 ['Result ring','bcdb_result_ring_slots','128 slots','globals.h','2,048 (cluster reference run)','code'],
 ['Early validation','BCDB_DT_EARLY_VALIDATE','on (env)','shm_transaction.c','on (merged default; the DET_OPTIMIZATION A/B switched it off per run)','code'],
 ['Post-publish settle','BCDB_DT_POST_PUBLISH_SETTLE','on (env)','worker.c','on','code'],
 ['Request shape','deterministic admission','exactly one top-level statement','worker.c get_write_set','not configurable','code'],
 ['Merkle aggregate','MERKLE_VERSION','11 = AdHash mod 2^256','merkle.h','OOM Merkle rows: format 11 (AdHash, re-measured); recovery campaign and the XOR OOM archive: format 10','hist'],
 ['Merkle fanout','fanout reloption','4 · power of two 2–256','merkle.h / merkle.c','32 in OOM, recovery and TPC-C profiles; 4 in cluster YCSB','code'],
 ['Split / merge','split_threshold','32 / 8 legacy; 1,024 for fanout 32; merge = split ÷ 4','merkle.h','1,024 / 256 (OOM, recovery, TPC-C) · 32 / 8 (cluster YCSB)','code'],
 ['Partitions','partitions reloption','200 (1 … 32,767)','merkle.h','200; TPC-C 16,384','code'],
 ['Det window','--detWindow (gateway)','per run','gateway flags','1,024 latency campaign · 65,536 original YCSB and recovery campaigns','meas'],
 ['Completion','--kafka-completion-mode','per run','cluster_sweep_support.py','majority_async_all3 (2-of-3 completes, 3rd audited)','meas'],
 ['Kafka producer','producer config','acks=1, linger 0, lz4','kafka_console.cxx','unchanged','code'],
 ['Raft timing','raft params','heartbeat 250 ms, election 5–10 s','ariabc_pg_server.cxx','unchanged','code'],
 ['Recovery','--recovery-mode','off','cluster runner','off in throughput/latency runs; off/both/passive/active in the recovery campaign','meas'],
 ['Durability','synchronous_commit, fsync, full_page_writes','PostgreSQL defaults (on)','postgresql.conf','on, validated per replica','meas'],
 ['Buffers','shared_buffers','—','runner','32MB cluster and OOM (OOM: heap ≈ 26 GiB) · 32GB TPC-C with prewarm','meas'],
 ['Gateway tag','--txSign','—','gateway','blake3: a gateway-side self-check, never sent to replicas','code']
];
function renderSourceRows(){const F=SOURCES.files||{},R=(window.SUITE_FRESH&&SUITE_FRESH.results)||{};const st=p=>R[p]==='ok'?'<span class="tag meas">matches</span>':R[p]==='changed'?'<span class="tag bad">changed</span>':R[p]==='missing'?'<span class="tag bad">unreadable</span>':'<span class="tag">not checked</span>';
 $('#src-files').innerHTML=`<thead><tr><th>File</th><th>Why the page cites it</th><th class="num">Bytes</th><th>Reviewed sha256</th><th>Live check</th></tr></thead><tbody>${Object.entries(F).map(([p,f])=>`<tr><td class="mono"><a href="${esc(p)}" target="_blank" rel="noopener">${esc(p)}</a></td><td>${esc(f.purpose||'')}</td><td class="num">${fmt(f.bytes)}</td><td class="mono small" title="${esc(f.sha256)}">${esc(f.sha256.slice(0,12))}…</td><td>${st(p)}</td></tr>`).join('')}</tbody>`;
 const S=window.SUITE_FRESH||{};$('#src-state').innerHTML=S.state==='ok'?`<div class="note ok"><b>All ${S.total} cited files match the reviewed bytes.</b> The animations and settings on this page describe this checkout.</div>`:S.state==='drift'?`<div class="note bad"><b>${S.changed.length+S.missing.length} cited file(s) differ from the review.</b> The page still describes <code>${esc((SOURCES.head||'').slice(0,8))}</code>. Review the diff, update the models, then bump REVIEWED_HEAD and rebuild.</div>`:S.state==='checking'?`<div class="note"><b>Hashing the cited files…</b></div>`:`<div class="note warn"><b>Not checked.</b> Serve the checkout with <code>./open_websites.sh</code> so the browser can fetch and hash the cited files. Opening the file directly cannot compare them.</div>`}
function initSources(){const P=SOURCES.provenance||{},prev=SOURCES.previous_review||{};
 $('#src-id').innerHTML=`<dl class="kv"><dt>Reviewed head</dt><dd><code>${esc(SOURCES.head||'—')}</code></dd><dt>Reviewed on</dt><dd>${esc(SOURCES.reviewed_date||'—')}</dd><dt>Previous review</dt><dd><code>${esc((prev.head||'').slice(0,8))}</code> · ${esc(prev.date||'')}</dd><dt>Identity mode</dt><dd>${esc(P.mode||'')}${P.mode==='snapshot'?` · manifest sha256 <code>${esc((P.manifest_sha256||'').slice(0,12))}…</code> · origin ${esc(P.origin_root||'')}`:''}</dd><dt>Scope</dt><dd>${esc(SOURCES.scope||'')}</dd><dt>Data built</dt><dd>${esc(RESULTS.generated_utc||'—')} from curated CSVs (each campaign keeps its own frozen commit)</dd></dl>`;
 $('#src-settings').innerHTML=`<thead><tr><th>Setting</th><th>Knob</th><th>Source default</th><th>Defined in</th><th>Selected profiles / recorded runs</th></tr></thead><tbody>${SETTINGS.map(([a,k,d,w,p,t])=>`<tr><td>${esc(a)}</td><td class="mono small">${esc(k)}</td><td>${esc(d)}</td><td class="mono small">${esc(w)}</td><td><span class="tag ${t}">${t==='meas'?'recorded':t==='hist'?'differs by campaign':'source / profile'}</span> ${esc(p)}</td></tr>`).join('')}</tbody>`;
 renderSourceRows();document.addEventListener('suite:fresh',renderSourceRows)}

/* ---------------- 04 GATE: the eight fallback reasons (BCDBOpfReason) ---------------- */
const OPF_REASONS=[
 ['Trigger','BEFORE/AFTER triggers would run speculatively','nodeModifyTable.c, before statement triggers fire'],
 ['INSERT … ON CONFLICT','DO NOTHING and DO UPDATE resolve the conflict physically','nodeModifyTable.c ExecInsert'],
 ['nextval() / setval()','sequence mutation is not transactional','sequence.c, before the speculative change'],
 ['Scan after own INSERT','a pending INSERT is absent from heap and indexes','shm_transaction.c bcdb_check_pending_scan_slow'],
 ['Scan after indexed UPDATE','changed index columns or partial-index membership could hide a row','shm_transaction.c / indexam.c, before the index scan'],
 ['DELETE … RETURNING','a deferred DELETE has no rows to return','nodeModifyTable.c ExecDelete (new)'],
 ['PL/pgSQL EXCEPTION block','requested at block entry, before any handler can trap BC010','pl_exec.c exec_stmt_block (new)'],
 ['Exclusion constraint','deferred apply cannot check exclusion; INSERT and UPDATE fall back','execIndexing.c flag → nodeModifyTable.c (new)']];
function renderOpfReasons(){$('#opf-reasons').innerHTML=OPF_REASONS.map(([a,b,c])=>`<li><b>${esc(a)}</b> — ${esc(b)}<small>${esc(c)}</small></li>`).join('')}
function fillGeometry(){const O=RESULTS.oom;if(!O||!O.settings)return;const f=k=>(O.settings.find(s=>s[2]&&s[2].endsWith(':'+k))||[])[1];
 const p=f('baseline_merkle_partitions'),fo=f('baseline_merkle_fanout'),n=(O.settings.find(s=>s[0]==='Merkle nodes / leaves')||[])[1],sm=(O.settings.find(s=>s[0]==='Split / merge')||[])[1];
 if(p)$('#geo-oom-p').textContent=fmt(+p);if(fo)$('#geo-oom-f').textContent=fo;if(sm)$('#geo-oom-s').textContent=sm.replace(' / ',' / ');if(n)$('#geo-oom-n').textContent=n+' nodes / leaves'}
{const fr=$('#foot-review');if(fr)fr.textContent=`${SOURCES.reviewed_date||'—'} against ${(SOURCES.head||'—').slice(0,8)} (previous ${(SOURCES.previous_review&&SOURCES.previous_review.head||'').slice(0,8)})`}
