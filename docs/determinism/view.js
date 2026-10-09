const evidence=JSON.parse(document.getElementById('source-evidence').textContent);
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const ns='http://www.w3.org/2000/svg';
const colors=['var(--token0)','var(--token1)','var(--token2)'];
const phases={queued:55,simulate:235,simerror:330,conflict:350,fallback:350,wait:410,physical:470,publish:570,apply:715,settle:715,commit:870,error:870,result:1020};
const labels=[[55,'Input'],[235,'Private SQL'],[410,'Ordering gate'],[570,'Publish P'],[715,'Apply'],[870,'Commit / C'],[1020,'Result']];
const reduced=matchMedia('(prefers-reduced-motion: reduce)');
const state={chapter:0,t:0,playing:false,speed:1,variant:'trigger'};
let lastTick=0,lastRaf=performance.now(),renderKey='',currentModel=null,previousModel=null,eventIndex=0,panNext=false;
function currentScene(){return scenarios[state.chapter];}
function currentFrame(){const s=currentScene();let k=0;for(let i=0;i<s.frames.length;i++)if(s.frames[i].t<=state.t)k=i;return {frame:s.frames[k],index:k};}
function eventRef(s,f,k){return s.id==='fallback'&&k===1?fallbackReasons[state.variant].ref:f.ref;}
function svgNode(name,attrs={},text=''){const node=document.createElementNS(ns,name);for(const [k,v]of Object.entries(attrs))node.setAttribute(k,v);if(text)node.textContent=text;return node;}
const PHYS=30; // vertical offset of the ordered-physical track
let laneY=[],opfLanes=new Set();
function layoutLanes(s){
 opfLanes=new Set(s.frames.flatMap(f=>f.ops.filter(op=>op[0]==='fallback').map(op=>op[1])));
 laneY=[];let y=85;s.base.ids.forEach(id=>{laneY.push(y);y+=opfLanes.has(id)?140:83;});
 const last=s.base.ids.length-1;return laneY[last]+(opfLanes.has(s.base.ids[last])?100:41)+22;
}
function tokenPosition(tx,i){return {x:phases[tx.phase]||55,y:laneY[i]+((tx.mode==='physical'||tx.opf)&&!tx.done?PHYS:0)};}
function drawRoutes(){
 const s=currentScene(),height=layoutLanes(s);$('flow').setAttribute('viewBox','0 0 1100 '+height);
 const routes=$('routes');routes.replaceChildren();
 for(const [x,label]of labels)routes.append(svgNode('text',{x,y:25,'text-anchor':'middle',class:'diagram-label'},label));
 s.base.ids.forEach((id,i)=>{
  const y=laneY[i],opf=opfLanes.has(id);
  routes.append(svgNode('rect',{x:12,y:y-31,width:1076,height:opf?131:72,rx:13,class:'lane-bg'}));
  routes.append(svgNode('path',{d:'M55 '+y+' H1004',class:'flow-line','marker-end':'url(#arrow)'}));
  if(opf){
   // BC010 leaves the optimistic line, waits for C ≥ s−1, runs physical SQL, rejoins at publication.
   const yp=y+PHYS;
   routes.append(svgNode('path',{d:'M235 '+y+' C285 '+y+' 300 '+yp+' 350 '+yp+' H545 C560 '+yp+' 560 '+y+' 570 '+y,class:'flow-physical'}));
   routes.append(svgNode('text',{x:455,y:yp+64,'text-anchor':'middle',class:'phys-label'},'BC010 → wait for turn and C ≥ s−1 → physical SQL → publish'));
   for(const x of [350,410,470])routes.append(svgNode('circle',{cx:x,cy:yp,r:4,class:'flow-node physical'}));
  }
  for(const [x]of labels)routes.append(svgNode('circle',{cx:x,cy:y,r:5,class:'flow-node'}));
 });
 const tokens=$('tokens');tokens.replaceChildren();
 s.base.ids.forEach((id,i)=>{
  const g=svgNode('g',{class:'token','data-tx':id,style:'--token-color:'+colors[i%3]});
  g.append(svgNode('circle',{r:28,class:'token-glow'}),svgNode('circle',{r:19}),svgNode('text',{},'T'+id),svgNode('title',{},'Transaction '+id));
  const note=svgNode('text',{class:'token-note',x:0,y:36});g.append(note);tokens.append(g);
 });
}
function showSource(ref){
 const e=evidence.excerpts[ref];if(!e)throw Error('Missing evidence '+ref);
 $('source-select').value=ref;
 if(e.paper){$('source-code').textContent=e.text;$('source-link').href='ProtectDB_arxiv-3.pdf#page=7';$('source-link').textContent='Open paper · page 7 ↗';}
 else{$('source-code').textContent=e.text;$('source-link').href=e.path;$('source-link').textContent='Open '+e.path.split('/').pop()+' ↗';}
 const file=evidence.files[e.path];$('source-hash').textContent=e.path+(e.paper?' · printed pages 6–7':' · lines '+e.start+'–'+e.end)+' · SHA-256 '+file.sha256;
}
// One-shot visual cues for the events of the current frame.
function renderEffects(s,f,st,prev){
 const fx=$('effects');fx.replaceChildren();
 const at=id=>{const i=s.base.ids.indexOf(id);return {i,now:tokenPosition(st.tx[id],i),before:tokenPosition(prev.tx[id],i)};};
 const ring=(x,y,cls,label,dy=-34)=>{const g=svgNode('g',{class:'fx '+cls,transform:'translate('+x+','+y+')'});g.append(svgNode('circle',{r:26,class:'fx-ring'}));if(label)g.append(svgNode('text',{y:dy,class:'fx-label'},label));fx.append(g);};
 for(const op of f.ops){
  const [type,id,arg,extra]=op;
  if(type==='publish'){const p=at(id);ring(570,laneY[p.i],'fx-publish','P → '+id);}
  else if(type==='pready'){const p=at(id);ring(570,laneY[p.i],'fx-pready','slot ready, no footprint');}
  else if(type==='commit'){const p=at(id);ring(870,laneY[p.i],'fx-commit','committed');}
  else if(type==='terminal'){const p=at(id);ring(870,laneY[p.i],'fx-error','ERROR '+arg);}
  else if(type==='fallback'){const p=at(id);ring(350,laneY[p.i]+PHYS,'fx-fallback',f.ops.some(o=>o[0]==='abort'&&o[1]===id)?'':'BC010',-32);}
  else if(type==='sqlerror'){const p=at(id);ring(p.now.x,p.now.y,'fx-error','✗ '+arg);}
  else if(type==='abort'){
   // The attempt is discarded where it was rejected: the token's position after this event.
   const p=at(id),tx=st.tx[id],x=p.now.x,y=p.now.y,back=!tx.opf&&x>260;
   const g=svgNode('g',{class:'fx fx-abort'});
   if(back)g.append(svgNode('path',{d:'M'+(x-14)+' '+(y-24)+' C'+(x-40)+' '+(y-60)+' 270 '+(y-60)+' 245 '+(y-28),class:'fx-retry','marker-end':'url(#arrow-danger)'}));
   g.append(svgNode('text',{x:x+24,y:y-14,class:'fx-x'},'✗'));
   g.append(svgNode('text',{x:back?(x+245)/2:x,y:back?y-54:y-40,class:'fx-label danger'},back?'attempt discarded → SQL runs again':tx.opf?'BC010: speculative attempt discarded':'attempt discarded'));
   fx.append(g);
  }
  else if(type==='dep'){
   const a=at(id).now,b=at(arg).now,near=Math.abs(a.x-b.x)<40;
   const cx=(a.x+b.x)/2+(near?90:0),cy=(a.y+b.y)/2+(near?0:28);
   const edge=(p,r)=>{const dx=cx-p.x,dy=cy-p.y,l=Math.hypot(dx,dy)||1;return [p.x+dx/l*r,p.y+dy/l*r];};
   const [x0,y0]=edge(a,24),[x2,y2]=edge(b,27);
   const g=svgNode('g',{class:'fx fx-dep'});
   g.append(svgNode('path',{d:'M'+x0+' '+y0+' Q'+cx+' '+cy+' '+x2+' '+y2,class:'fx-dep-line','marker-end':'url(#arrow-danger)'}));
   // label beside the writer, above its own note
   g.append(svgNode('text',{x:a.x+30,y:a.y-20,class:'fx-label danger','text-anchor':'start'},extra));fx.append(g);
  }
 }
}
function renderLens(st){
 const l=st.lens;if(!l)return '';
 const text=(x,y,value,size=11)=>'<text x="'+x+'" y="'+y+'" text-anchor="middle" fill="var(--text)" font-size="'+size+'">'+esc(value)+'</text>';
 const circle=(x,y,value,color)=>'<circle cx="'+x+'" cy="'+y+'" r="23" fill="var(--bg)" stroke="'+color+'" stroke-width="2"/>'+text(x,y+5,value,15);
 let content='',height=132;
 if(l.type==='tags'){
  content='<circle cx="111" cy="57" r="48" fill="var(--accent-soft)" stroke="var(--token1)" stroke-width="2"/><circle cx="189" cy="57" r="48" fill="var(--accent-soft)" stroke="'+(l.shared?'var(--token0)':'var(--line)')+'" stroke-width="2"/>';
  content+=text(94,50,'READ')+text(94,66,l.read,10)+text(206,50,l.shared?'SHARED WRITE':'PRIVATE WRITE',9)+text(206,66,l.write,10);
  content+='<circle cx="150" cy="57" r="10" fill="'+(l.match?'var(--danger)':l.covered?'var(--ok)':'var(--line)')+'"/>';
  content+=text(150,124,l.match?'Missing predecessor writer → restart':l.covered?'Writer is visible in the retry snapshot':l.shared?'Published tag is available for validation':'Read exists even before writer publication',10);
 }else if(l.type==='overlay'){
  content='<path d="M78 63 H122 M178 63 H222" stroke="var(--line)" stroke-width="3" marker-end="url(#arrow)"/>';
  content+=text(50,23,'Physical heap')+text(150,23,'Pending TID')+text(250,23,'SQL last read');
  content+=circle(50,63,l.heap,'var(--ok)')+circle(150,63,l.pending,'var(--token1)')+circle(250,63,l.visible,'var(--accent)');
  content+=text(150,117,'Command '+l.command+' · earlier-command overlay only',10);
 }else if(l.type==='random'){
  content+=text(150,16,'ID 1 · attempt '+l.attempt);
  for(let row=0;row<2;row++){
   const y=49+row*57;content+=text(27,y+4,row===0?'α':'β',16);
   content+='<path d="M72 '+y+' H262" stroke="var(--line)" stroke-width="2"/>';
   for(let j=0;j<3;j++)content+=circle(85+j*77,y,'r₁['+j+']',row===0?'var(--accent)':'var(--token1)');
  }height=139;
 }else if(l.type==='merkle'){
  content+=text(105,17,'Hash XOR')+text(235,17,'Count sum');
  content+=text(30,53,'INSERT',9)+circle(105,49,l.ih,'var(--accent)')+circle(235,49,(l.ic>0?'+':'')+l.ic,'var(--accent)');
  content+=text(30,113,'DELETE',9)+circle(105,108,l.dh,'var(--token1)')+circle(235,108,l.dc,'var(--token1)');height=145;
 }
 return '<svg class="lens" viewBox="0 0 300 '+height+'" role="img" aria-label="'+esc(l.type)+' dependency detail">'+content+'</svg>';
}
function flash(el,on){el.classList.remove('bump');if(on&&!reduced.matches){void el.offsetWidth;el.classList.add('bump');}}
function renderEvent(){
 const s=currentScene(),{frame:f,index:k}=currentFrame();eventIndex=k;currentModel=modelAt(s,state.t);previousModel=k>0?modelAt(s,s.frames[k-1].t):currentModel;
 const st=currentModel,pv=previousModel;$('P').textContent=st.P;$('C').textContent=st.C;
 flash($('P').parentNode,k>0&&pv.P!==st.P);flash($('C').parentNode,k>0&&pv.C!==st.C);
 const changes=[];
 if(k>0){
  if(pv.P!==st.P)changes.push(['pub','P '+pv.P+' → '+st.P]);
  if(pv.C!==st.C)changes.push(['ok','C '+pv.C+' → '+st.C]);
  for(const id of s.base.ids){const a=pv.tx[id],b=st.tx[id];
   if(b.aborts>a.aborts)changes.push(['bad','T'+id+' attempt discarded']);
   if(b.error&&!a.error)changes.push(['bad','T'+id+' SQL error '+b.error]);
   if(b.opf&&!a.opf)changes.push(['warn','T'+id+' → physical fallback']);
   if(b.published&&!a.published)changes.push(['pub','T'+id+' published']);
   if(b.pready&&!a.pready)changes.push(['warn','T'+id+' slot ready (no footprint)']);
   if(b.done&&!a.done)changes.push([b.outcome==='committed'?'ok':'bad','T'+id+' '+(b.outcome==='committed'?'committed':b.outcome)]);
   if(b.sqlRuns>a.sqlRuns&&a.sqlRuns>0)changes.push(['','T'+id+' SQL attempt '+b.sqlRuns+(b.mode==='physical'?' (physical)':'')]);
  }
  for(const [key,value]of Object.entries(st.db))if(pv.db[key]!==value)changes.push(['ok',key+': '+pv.db[key]+' → '+value]);
 }
 $('changes').innerHTML='<span class="chg-head">This event:</span>'+(changes.length?changes.map(([c,t])=>'<span class="chg '+c+'">'+esc(t)+'</span>').join(''):'<span class="chg none">'+(k===0?'starting state':'no frontier or data change')+'</span>');
 const max=Math.max(...s.base.ids),min=Math.max(0,Math.min(...s.base.ids)-1);
 $('slots').innerHTML=Array.from({length:max-min+1},(_,n)=>n+min).map(id=>{
  const ready=id<=st.startC||st.ready.includes(id),published=id<=st.startP||st.published.includes(id)||st.pready.includes(id),hole=id===st.C+1&&st.ready.some(n=>n>id);
  return '<span class="slot '+(ready?'ready':published?'published':'')+(hole?' hole':'')+'" title="Item '+id+': '+(ready?'finalized':published?'published, not finalized':'unpublished')+'">'+id+'</span>';
 }).join('');
 $('private').innerHTML=s.base.ids.map((id,i)=>{const tx=st.tx[id];return '<div class="tx-private" style="--token-color:'+colors[i%3]+'"><span class="tx-name">T'+id+'</span> · '+esc(tx.mode==='physical'?'physical SQL':tx.opf?'fallback selected':'optimistic SQL')+'<div class="queue '+(tx.published?'frozen':'')+(k>0&&JSON.stringify(pv.tx[id].queue)!==JSON.stringify(tx.queue)?' changed':'')+'">'+(tx.queue.length?(tx.done?'Accepted queue history:<br>':'')+tx.queue.map(esc).join('<br>'):tx.done?esc(tx.outcome):'no deferred writes')+'</div><div class="metrics">SQL attempts '+tx.sqlRuns+' · discarded '+tx.aborts+' · '+(tx.error?'SQL error '+tx.error+' (rolled back) · ':'')+(tx.published?'publication released':tx.pready?'slot ready, no footprint':'private')+(tx.done?' · finalized; backend cleaned up':'')+'</div></div>';}).join('');
 const entries=Object.entries(st.db);$('database').innerHTML=entries.length?entries.map(([key,value])=>{const was=k>0&&pv.db[key]!==value;return '<div class="db-row'+(was?' changed':'')+'"><span>'+esc(key)+'</span>'+(was?'<em class="was">'+esc(pv.db[key])+' →</em>':'')+'<strong>'+esc(value)+'</strong></div>';}).join(''):'<div class="panel-item">No data change in this scene</div>';
 $('panel-title').textContent=st.panel.title.toUpperCase();$('panel-items').innerHTML=renderLens(st)+st.panel.items.map(item=>'<div class="panel-item">'+esc(item)+'</div>').join('');
 $('event-step').textContent='Event '+(k+1)+' of '+s.frames.length;$('event-title').textContent=f.title;renderEffects(s,f,st,pv);$('event-body').textContent=f.body;$('take').textContent=s.take;
 $('events').querySelectorAll('button').forEach((b,i)=>{b.classList.toggle('current',i===k);b.setAttribute('aria-current',i===k?'step':'false');});
 $('live-status').textContent=f.title+'. P '+st.P+', C '+st.C;
 $('svg-title').textContent=s.title+': '+f.title;
 $('source-select').replaceChildren(...[...new Set([eventRef(s,f,k),...s.refs,...s.frames.map((f,i)=>eventRef(s,f,i))])].map(ref=>{const e=evidence.excerpts[ref];if(!e)throw Error('Missing reference '+ref);const o=document.createElement('option');o.value=ref;o.textContent=e.label||ref;return o;}));
 showSource(eventRef(s,f,k));
 $('prev').disabled=k===0&&state.t===0;$('next').disabled=state.t>=s.duration;
 panNext=true;renderKey=state.chapter+':'+k+':'+state.variant;
}
function renderMotion(){
 const s=currentScene(),f=s.frames[eventIndex];let fraction=Math.min(1,Math.max(0,(state.t-f.t)/.95));
 if(!state.playing||reduced.matches)fraction=1;
 const eased=1-Math.pow(1-fraction,3);
 s.base.ids.forEach((id,i)=>{
  const tx=currentModel.tx[id],old=previousModel.tx[id],from=tokenPosition(old,i),to=tokenPosition(tx,i),x=from.x+(to.x-from.x)*eased,y=from.y+(to.y-from.y)*eased;
  const g=$('tokens').children[i];g.setAttribute('transform','translate('+x.toFixed(2)+','+y.toFixed(2)+')');g.setAttribute('class','token '+tx.phase);
  const status=tx.done?tx.outcome+(id<=currentModel.C?' · in prefix C':' · behind prefix hole'):tx.note;
  const note=g.querySelector('.token-note');note.textContent=status;note.setAttribute('text-anchor',x<135?'start':x>955?'end':'middle');
  g.querySelector('title').textContent='T'+id+' · '+status;
 });
 const wrap=$('diagram-wrap');
 if(innerWidth<760&&!$('fit').checked&&(panNext||state.playing)){
  const moved=[...f.ops].reverse().find(op=>op[0]==='move');
  if(moved){const i=s.base.ids.indexOf(moved[1]),g=$('tokens').children[i],matrix=g.transform.baseVal.consolidate().matrix;
   const scale=$('flow').getBoundingClientRect().width/1100;wrap.scrollLeft=Math.max(0,matrix.e*scale-wrap.clientWidth/2);}
 }
 panNext=false;
 $('progress').value=state.t;$('time').textContent=(eventIndex+1)+' / '+s.frames.length+' events · abstract time';
 $('play').textContent=state.playing?'Ⅱ Pause':'▶ Play';$('play').setAttribute('aria-pressed',String(state.playing));
}
function render(){const {index}=currentFrame();if(renderKey!==state.chapter+':'+index+':'+state.variant)renderEvent();renderMotion();}
function pause(){state.playing=false;lastTick=0;render();}
function seek(t){state.playing=false;state.t=Math.max(0,Math.min(currentScene().duration,Number(t)));lastTick=0;render();}
function choose(index,writeHash=true){
 state.chapter=Number(index);state.t=0;state.playing=false;lastTick=0;renderKey='';const s=currentScene();
 $('title').textContent=s.title;$('question').textContent=s.question;$('group').textContent=s.group;$('chapter-count').textContent=(state.chapter+1)+' / '+scenarios.length;
 document.querySelectorAll('[data-chapter]').forEach(b=>b.setAttribute('aria-current',String(Number(b.dataset.chapter)===state.chapter)));
 $('progress').max=s.duration;$('variant-wrap').hidden=s.id!=='fallback';
 $('events').replaceChildren(...s.frames.map((f,i)=>{const b=document.createElement('button');b.className='event-dot';b.textContent=i+1;b.title=f.title;b.setAttribute('aria-label','Event '+(i+1)+': '+f.title);b.onclick=()=>seek(f.t);return b;}));
 if(writeHash)history.replaceState(null,'','#'+s.id);
 drawRoutes();render();
}
function next(){const s=currentScene();const f=s.frames.find(f=>f.t>state.t+.01);seek(f?f.t:s.duration);}
function prev(){const frames=currentScene().frames.filter(f=>f.t<state.t-.01);seek(frames.length?frames[frames.length-1].t:0);}
function toggle(){if(state.playing){pause();return;}if(state.t>=currentScene().duration)state.t=0;state.playing=true;lastTick=0;render();}
function advanceClock(now){
 if(state.playing){if(lastTick){state.t=Math.min(currentScene().duration,state.t+Math.max(0,Math.min(.1,(now-lastTick)/1000))*state.speed);if(state.t>=currentScene().duration)state.playing=false;}lastTick=Math.max(lastTick,now);render();}
 else lastTick=0;
}
function tick(now){lastRaf=now;advanceClock(now);requestAnimationFrame(tick);}
// Firefox may withhold RAF for an unfocused/minimized window without marking
// its document hidden. Keep one shared clock; RAF and this watchdog cannot
// count the same elapsed interval twice. Hidden documents still pause below.
setInterval(()=>{const now=performance.now();if(state.playing&&!document.hidden&&now-lastRaf>250)advanceClock(now);},75);
const validation=validateModel();
let group='';scenarios.forEach((s,i)=>{if(s.group!==group){const h=document.createElement('div');h.className='group';h.textContent=s.group;$('nav').append(h);group=s.group;}const b=document.createElement('button');b.className='scene-link';b.dataset.chapter=i;b.innerHTML='<span class="num">'+String(i+1).padStart(2,'0')+'</span><span class="label">'+esc(s.title)+'</span>';b.onclick=()=>choose(i);$('nav').append(b);});
Object.entries(fallbackReasons).forEach(([key,reason])=>{const o=document.createElement('option');o.value=key;o.textContent=reason.name;$('variant').append(o);});
$('variant-why').textContent=fallbackReasons[state.variant].why;
$('variant').onchange=()=>{state.variant=$('variant').value;$('variant-why').textContent=fallbackReasons[state.variant].why;renderKey='';render();};
$('play').onclick=toggle;$('restart').onclick=()=>seek(0);$('next').onclick=next;$('prev').onclick=prev;
$('progress').oninput=()=>seek($('progress').value);$('speed').onchange=()=>state.speed=Number($('speed').value);
$('explain').onclick=()=>{const on=document.body.classList.toggle('visual-only');$('explain').setAttribute('aria-pressed',String(on));$('explain').textContent=on?'Show explanation':'Animation only';};
$('fit').onchange=()=>{$('diagram-wrap').classList.toggle('fit',$('fit').checked);};
$('source-select').onchange=()=>showSource($('source-select').value);
$('jump-source').onclick=()=>{$('source-details').open=true;$('source-details').scrollIntoView({block:'nearest',behavior:reduced.matches?'auto':'smooth'});};
document.addEventListener('keydown',e=>{if(['INPUT','SELECT','TEXTAREA','BUTTON'].includes(e.target.tagName)||e.ctrlKey||e.metaKey||e.altKey)return;if(e.code==='Space'){e.preventDefault();toggle();}else if(e.key==='ArrowRight'){e.preventDefault();next();}else if(e.key==='ArrowLeft'){e.preventDefault();prev();}});
document.addEventListener('visibilitychange',()=>{if(document.hidden&&state.playing)pause();});
function reducedChange(){$('motion-notice').hidden=!reduced.matches;render();}reduced.addEventListener('change',reducedChange);
$('head').textContent=evidence.head.slice(0,8);$('date').textContent=evidence.reviewed_date;
function chapterFromHash(){const i=scenarios.findIndex(s=>s.id===location.hash.slice(1));return i<0?0:i;}
addEventListener('hashchange',()=>choose(chapterFromHash(),false));
choose(chapterFromHash(),false);reducedChange();requestAnimationFrame(tick);
window.determinismExplorer={state,chapters:scenarios,choose,seek,modelAt,validateModel,validation,get model(){return currentModel;},showSource};
