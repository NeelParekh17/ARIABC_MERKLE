/* Shared suite behaviour: review identity chip, theme toggle and the live source-freshness check.
   The page embeds the reviewed manifest (path → sha256) it was built from. When the page is served
   from the checkout (scripts/open_architecture_explorer.py), each cited file is fetched and hashed
   in the browser; any difference means the source changed after the review that this page models.
   A rebuilt page does not count as fresh: only matching file bytes do. */
(function(){
 'use strict';
 const $=s=>document.querySelector(s);
 const id=document.body.dataset.manifest;const el=id&&document.getElementById(id);let man=null;
 try{man=el?JSON.parse(el.textContent):null}catch(e){man=null}
 const head=man&&man.head?man.head:'';
 if($('#review-head'))$('#review-head').textContent=head?head.slice(0,8):'—';
 if($('#review-date'))$('#review-date').textContent=man&&man.reviewed_date?man.reviewed_date:'';
 const tb=$('#theme-btn');
 if(tb){const dark=window.SUITE_THEME==='dark';tb.textContent=dark?'☀':'☾';tb.title=dark?'Switch to the light theme':'Switch to the dark theme';tb.setAttribute('aria-label',tb.title);
  tb.onclick=()=>{try{localStorage.setItem('ariabc_theme',dark?'light':'dark')}catch(e){}location.reload()}}
 const fresh=$('#fresh');
 const state={state:'idle',results:{},checked:0,changed:[],missing:[],total:man&&man.files?Object.keys(man.files).length:0};
 window.SUITE_FRESH=state;
 function show(s,label,title){state.state=s;if(!fresh)return;fresh.dataset.state=s;fresh.querySelector('span').textContent=label;fresh.title=title;
  document.dispatchEvent(new CustomEvent('suite:fresh',{detail:state}))}
 async function sha(buf){const d=await crypto.subtle.digest('SHA-256',buf);return[...new Uint8Array(d)].map(b=>b.toString(16).padStart(2,'0')).join('')}
 async function check(){
  if(!man||!man.files){show('na','no manifest','This page carries no source manifest.');return}
  if(!/^https?:$/.test(location.protocol)||!(window.crypto&&crypto.subtle)){show('na','not checked','Open the page through scripts/open_architecture_explorer.py (http://127.0.0.1) to compare the cited files with the reviewed hashes.');return}
  show('checking','checking sources…','Hashing the cited files in the checkout');
  const paths=Object.keys(man.files);let i=0;
  async function worker(){while(i<paths.length){const p=paths[i++];let r='missing';
   try{const res=await fetch(p.split('/').map(encodeURIComponent).join('/'),{cache:'no-store'});if(res.ok)r=(await sha(await res.arrayBuffer()))===man.files[p].sha256?'ok':'changed'}catch(e){r='missing'}
   state.results[p]=r;state.checked++;if(r==='changed')state.changed.push(p);if(r==='missing')state.missing.push(p)}}
  await Promise.all([0,1,2,3,4,5].map(worker));
  const n=state.changed.length+state.missing.length;
  if(!n)show('ok',`sources match · ${paths.length}`,`All ${paths.length} cited files hash to the reviewed bytes of ${head.slice(0,8)}.`);
  else show('drift',`${n} changed since review`,`${state.changed.length} changed, ${state.missing.length} unreadable:\n`+[...state.changed,...state.missing].join('\n')+'\nThe animations describe the reviewed source, not these edits.');
 }
 if(fresh){fresh.addEventListener('click',()=>{const t=document.querySelector('[data-tab="src"]');if(t)t.click();else{const d=document.getElementById('source-details');if(d){d.open=true;d.scrollIntoView({behavior:'smooth'})}}});check()}
})();
