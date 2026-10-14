/* ---------------- navigation and motion ---------------- */
const REDUCED=matchMedia('(prefers-reduced-motion: reduce)').matches;
/* keep the active section visible in the rail (vertical on desktop, a scrolling strip on narrow screens) */
function fxTab(k){document.body.dataset.section=k;const nav=$('.tabs'),b=nav.querySelector(`[data-tab="${k}"]`);if(!b)return;
 if(nav.scrollWidth>nav.clientWidth+2){const l=b.offsetLeft-40,r=b.offsetLeft+b.offsetWidth+40;if(l<nav.scrollLeft)nav.scrollLeft=Math.max(0,l);else if(r>nav.scrollLeft+nav.clientWidth)nav.scrollLeft=r-nav.clientWidth}
 else{const t=b.offsetTop-80,bt=b.offsetTop+b.offsetHeight+40;if(t<nav.scrollTop)nav.scrollTop=Math.max(0,t);else if(bt>nav.scrollTop+nav.clientHeight)nav.scrollTop=bt-nav.clientHeight}
 const hero=$('.hero');if(hero&&fxTab.ready&&window.scrollY>hero.offsetTop+hero.offsetHeight)window.scrollTo({top:hero.offsetTop+hero.offsetHeight-8,behavior:REDUCED?'auto':'smooth'});fxTab.ready=true}
/* numbers in stat tiles ease from their previous value to the new one */
const prevNums=new WeakMap();
function countUp(root){const els=[...root.querySelectorAll('strong')],prev=prevNums.get(root)||[],now=[];
 els.forEach((el,i)=>{const txt=el.textContent,m=txt.match(/-?\d[\d,]*(\.\d+)?/);if(!m){now.push(null);return}
  const to=parseFloat(m[0].replace(/,/g,'')),dec=m[1]?m[1].length-1:0,from=prev[i]!=null?prev[i]:0;now.push(to);if(REDUCED||from===to)return;
  const pre=txt.slice(0,m.index),post=txt.slice(m.index+m[0].length),t0=performance.now(),D=700;
  const step=n=>{const q=Math.min(1,(n-t0)/D),e=1-Math.pow(1-q,3);el.textContent=pre+fmt(from+(to-from)*e,dec)+post;if(q<1)requestAnimationFrame(step)};el.textContent=pre+fmt(from,dec)+post;requestAnimationFrame(step)});
 prevNums.set(root,now)}
function initMotion(){const mt=$('#meas-tiles');new MutationObserver(()=>countUp(mt)).observe(mt,{childList:true})}
function initResultsTab(){initBench();initOom();initAdhash()}
function initRecovTab(){initRecov();initCampaign()}
function initMerkleTab(){initMerkle2();initAdhDemo();fillGeometry()}
function initPgTab(){initSim();renderOpfReasons()}
const INIT={topo:initTopo,threads:initThreads3,seq:initSeq,pg:initPgTab,merkle:initMerkleTab,recov:initRecovTab,meas:initMeas,bench:initResultsTab,find:initFind2,src:initSources};
let start='topo';try{const s=localStorage.getItem('ariabc_tab');if(s&&INIT[s])start=s}catch(e){}
const hashTab=location.hash.slice(1);if(INIT[hashTab])start=hashTab;
window.addEventListener('hashchange',()=>{const k=location.hash.slice(1);if(INIT[k])showTab(k)});
initGlobal();
initMotion();
showTab(start);
/* QA hook for scripts/check_architecture_explorer.py: WebDriver runs in a sandbox that sees window
   properties but not top-level let/const bindings, so the model state is exposed explicitly. */
window.archQA={get M2(){return M2},get RR(){return RR},get SEQ(){return SEQ},get simT(){return simT},get ADH(){return ADH},
 get simNow(){return simNow},set simNow(v){simNow=v},get simPlay(){return simPlay},set simPlay(v){simPlay=v},
 get trOn(){return trOn},get rS(){return rS},set rDisp(v){rDisp=v}};
