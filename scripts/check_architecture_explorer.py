#!/usr/bin/env python3
"""Firefox UI QA for ARCHITECTURE_EXPLORER.html. Run on the lab host only (10.129.148.247):

  python3 scripts/check_architecture_explorer.py <checkout>

Starts an isolated headless Firefox profile and a loopback HTTP server over <checkout>; starts no
database, gateway or cluster process. Artifacts (screenshots, results.json) go to
<checkout>/.bench_tmp/architecture_ui_<date>_*/.
"""
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from explorer_webdriver import Session  # noqa: E402

TABS = ['topo', 'threads', 'seq', 'pg', 'merkle', 'recov', 'meas', 'bench', 'find', 'src']
PAGE = 'ARCHITECTURE_EXPLORER.html'
DRAIN = 'function drain(){let g=0;while((archQA.M2.cur||archQA.M2.q.length)&&g<20000){m2StepOnce();g++}return g};'


def main():
    source = Path(sys.argv[1] if len(sys.argv) > 1 else '.').resolve()
    s = Session(source, 'architecture_ui_')
    js, check = s.js, s.check
    try:
        s.size(1440, 1100)
        s.open(PAGE + '#topo')
        time.sleep(.6)
        check('page initialized without errors', js('return uiErrors.length===0 && typeof showTab==="function"'))
        sources = json.loads((source / 'docs/explorers/architecture_sources.json').read_text())
        results = json.loads((source / 'docs/explorers/results_data.json').read_text())
        check('suite bar shows the reviewed head', js("return document.getElementById('review-head').textContent") == sources['head'][:8])
        embedded = js("return JSON.parse(document.getElementById('explorer-results').textContent)")
        check('embedded results equal docs/explorers/results_data.json', embedded == results)
        check('embedded source manifest equals architecture_sources.json',
              js("return JSON.parse(document.getElementById('explorer-sources').textContent)") == sources)
        for _ in range(150):
            if js('return window.SUITE_FRESH && ["ok","drift","na"].includes(SUITE_FRESH.state)'):
                break
            time.sleep(.1)
        check('freshness: every cited file matches the reviewed hash',
              js("return SUITE_FRESH.state==='ok' && SUITE_FRESH.checked===SUITE_FRESH.total && SUITE_FRESH.total>30"))
        for name, meta in sources['files'].items():
            with urllib.request.urlopen(f'{s.site}/{name}') as r:
                check('served source hash ' + name, hashlib.sha256(r.read()).hexdigest() == meta['sha256'])
        for name, digest in sources['assets'].items():
            check('asset hash ' + name, hashlib.sha256((source / name).read_bytes()).hexdigest() == digest)
        check('every speed selector offers 0.125× and 0.25×', js(
            "const s=[...document.querySelectorAll('select')].filter(x=>/speed/.test(x.id));"
            "return s.length>=8 && s.every(x=>['0.125','0.25'].every(v=>[...x.options].some(o=>o.value===v)))"))

        # every section, desktop and narrow, light theme
        for width in (1440, 390, 360):
            s.size(width, 1100)
            for t in TABS:
                js(f"showTab('{t}');window.scrollTo(0,0)")
                time.sleep(.5)
                check(f'{t} visible at {width}', js(f"return document.getElementById('v-{t}').classList.contains('on') && "
                                                    f"document.querySelector('[data-tab=\"{t}\"]').getAttribute('aria-selected')==='true'"))
                check(f'{t} containment at {width}', js('return document.documentElement.scrollWidth<=innerWidth+1'))
                s.shot(f'{t}_{width}')
        s.size(1440, 1100)

        # 01 topology: every process and connection opens its inside view, both deployments
        js("showTab('topo')")
        for dep in ('cluster', 'single'):
            js(f"document.querySelector('[data-dep=\"{dep}\"]').click()")
            keys = js("return [...document.querySelectorAll('#topo-svg [data-proc],#topo-svg [data-conn]')].map(g=>g.dataset.proc||g.dataset.conn)")
            check(f'topology {dep} has processes and connections', len(keys) >= 6)
            for k in keys:
                js(f"document.querySelector('#topo-svg [data-proc=\"{k}\"],#topo-svg [data-conn=\"{k}\"]').dispatchEvent(new MouseEvent('click',{{bubbles:true}}))")
                check(f'topology {dep} {k} inside view', js("return document.getElementById('zoom-svg').querySelector('svg')!==null && document.getElementById('zoom-title').textContent.length>3"))
        js("document.querySelector('[data-dep=\"cluster\"]').click();document.getElementById('trace-btn').click()")
        time.sleep(.8)
        check('trace animates over the topology', js("return !!document.querySelector('#trace-g') || document.getElementById('trace-btn').textContent.includes('Stop')"))
        js("if(archQA.trOn)traceToggle()")

        # 03 sequence: step through every step
        js("showTab('seq')")
        n = js('return archQA.SEQ.length')
        for i in range(n):
            js(f'seqJump({i})')
            check(f'sequence step {i}', js(f"return document.getElementById('seq-count').textContent.startsWith('{i + 1} /')"))

        # 04 gate model: every scripted case, including the BC010 route
        js("showTab('pg')")
        for case in ('random', 'samekey', 'twokeys', 'outoforder', 'postpub', 'opf'):
            js(f"const e=document.getElementById('sim-sc');e.value='{case}';e.dispatchEvent(new Event('change'));archQA.simNow=archQA.simT.end;archQA.simPlay=false;renderSim()")
            check(f'gate case {case} completes', js('return archQA.simT.tx.every(x=>x.done<=archQA.simT.end) && document.querySelectorAll("#sim-svg rect").length>20'))
        check('BC010 case uses the ordered physical route', js("return archQA.simT.tx[10].segs.some(s=>s[0]==='bc010') && archQA.simT.tx[10].segs.some(s=>s[0]==='phys')"))
        check('eight fallback reasons listed', js("return document.querySelectorAll('#opf-reasons li').length===8"))

        # 05 Merkle: AdHash model and the three verify checks, each failing distinctly
        js("showTab('merkle')")
        js(DRAIN + 'm2Reset();opVerify();drain()')
        check('clean index verifies', js('return archQA.M2.verify.ok===true && archQA.M2.verify.hashOk && archQA.M2.verify.countOk'))
        js(DRAIN + 'm2Reset();archQA.M2.sel=[...archQA.M2.rows.keys()][3];opCorrupt();drain();opVerify();drain()')
        check('heap fault fails check ① hash', js("return archQA.M2.verify.ok===false && archQA.M2.verify.fail==='hash'"))
        js(DRAIN + 'm2Reset();opCorruptCount();drain();opVerify();drain()')
        check('count drift passes ① and fails ② count', js("return archQA.M2.verify.ok===false && archQA.M2.verify.hashOk===true && archQA.M2.verify.fail==='count'"))
        js(DRAIN + 'm2Reset();opCorruptNode();drain();opVerify();drain()')
        check('node-row fault passes ① ② and fails ③ structure', js("return archQA.M2.verify.ok===false && archQA.M2.verify.hashOk && archQA.M2.verify.countOk && archQA.M2.verify.fail==='structure'"))
        js(DRAIN + 'opReindex();drain();opVerify();drain()')
        check('REINDEX repairs the node fault', js('return archQA.M2.verify.ok===true'))
        js(DRAIN + 'm2Reset();demoReins();drain();demoCoal();drain();demoSplit();drain();demoMerge();drain();demoSp();drain();opVerify();drain()')
        check('all Merkle demos keep the index consistent', js('return archQA.M2.verify.ok===true && uiErrors.length===0'))
        js(DRAIN + 'm2Reset();archQA.M2.sel=[...archQA.M2.rows.keys()][5];opDelete();drain();opVerify();drain()')
        check('DELETE subtracts its hash (AdHash) and verifies', js('return archQA.M2.verify.ok===true'))
        check('AdHash demo: identical pair cancels under XOR only', js(
            "archQA.ADH.n=2;archQA.ADH.other=false;renderAdhDemo();const t=[...document.querySelectorAll('#adhd-out strong')].map(x=>x.textContent);return t[0]==='00000000' && t[1]!=='00000000' && t[2]==='2'"))
        js('m2Reset()')
        s.shot('merkle_verify_states')

        # 06 recovery: campaign matrix, repair model incl. REINDEX-first full copy
        js("showTab('recov')")
        check('nine scenarios drawn', js("return document.querySelectorAll('#cam-svg .camrow').length===9"))
        js("document.querySelector('#cam-svg .camrow[data-s=\"M_active\"]').dispatchEvent(new MouseEvent('click',{bubbles:true}))")
        check('M_active lists 3 primary runs and the extra', js("return document.querySelectorAll('#cam-runs tbody tr').length===4"))
        for sc in ('mixed', 'upd', 'heap'):
            js(f"document.querySelector('#rr-sc [data-sc=\"{sc}\"]').click();for(let g=0;g<4000&&archQA.RR.i<archQA.RR.steps.length;g++)rrAdvance(true);rrRender()")
            phases = js('return archQA.RR.steps.map(s=>s.ph)')
            check(f'repair {sc} reaches LIVE', 'live' in phases and js('return archQA.RR.live===true'))
            if sc == 'heap':
                check('heap-only fault: REINDEX before full copy before verify',
                      phases.index('heap verify') < phases.index('reindex') < phases.index('full copy') < phases.index('verify'))
                check('heap-only fault copies the table once', js('return archQA.RR.st.full===1'))
            else:
                check(f'repair {sc} is sparse', js('return archQA.RR.st.full===0 && archQA.RR.st.parts>0'))
        js("rSetSc('L');archQA.rDisp=archQA.rS.dispTotal*0.6;rRender()")
        check('historical replay renders', js("return document.getElementById('rv-stage').querySelector('svg')!==null && document.getElementById('rv-log').textContent.length>20"))
        js("cutSeek(20)")
        check('CUT model renders', js("return document.getElementById('cut-svg').querySelector('svg')!==null"))

        # 08 results: data integrity against the curated CSV-derived JSON
        js("showTab('bench')")
        audit = results['oom']['audit']
        check('OOM: 84 configuration points, 252 cases (84 per mode), all Merkle PASS', len(results['oom']['points']) == 84 and
              audit['cases'] == 252 and audit['by_mode'] == {'pg': 84, 'bcdb_det': 84, 'bcdb_merkle': 84} and audit['merkle_pass'] == 84)
        check('OOM Merkle rows come from the AdHash campaign, PG/det from the paper campaign',
              all(('adhash' in p['source']) == (p['mode'] == 'bcdb_merkle') for p in results['oom']['points']))
        check('OOM: three trials per point', all(p['n'] == 3 for p in results['oom']['points']))
        check('recovery: 9 scenarios, 28 accepted attempts', results['recovery']['validation']['scenarios'] == 9 and
              results['recovery']['validation']['accepted'] == 28)
        check('AdHash: 28 points, 112 accepted cases', len(results['adhash']['points']) == 28 and results['adhash']['summary']['accepted'] == 112)
        check('det counters shown as unavailable, not zero', js("return document.getElementById('oom-tiles').textContent.includes('unavailable')")
              == (results['oom']['det_counters']['trials_with_values'] == 0))
        for i in range(7):
            for view in ('tps', 'ratio'):
                js(f"document.querySelector('#oom-view [data-v=\"{view}\"]').click();document.querySelector('#oom-cfg [data-i=\"{i}\"]').click()")
                check(f'OOM chart cfg {i} {view}', js("return document.querySelectorAll('#oom-svg path').length>=3"))
        js("document.querySelector('#oom-view [data-v=\"tps\"]').click();document.getElementById('oom-v2').click()")
        js("document.getElementById('oom-xor').click()")
        check('XOR Merkle overlay is dashed and labelled historical', js("return document.querySelectorAll('#oom-svg path[stroke-dasharray]').length===4"))
        js("document.getElementById('oom-xor').click()")
        check('v2 overlay is dashed and labelled historical', js("return document.querySelectorAll('#oom-svg path[stroke-dasharray]').length===3 && document.getElementById('oom-legend').textContent.includes('historical')"))
        check('AdHash chart has 28 bars', js("return document.querySelectorAll('#adh-svg .adhb').length===28"))
        check('result-family table', js("return document.querySelectorAll('#bench-table tbody tr').length>=8"))

        # 09 findings and 10 sources
        js("showTab('find')")
        check('findings render with filters', js("return document.querySelectorAll('#findgrid .find').length>=25 && document.querySelectorAll('#find-f button').length>3"))
        check('no stale working-tree claim', js("return !document.getElementById('findgrid').textContent.includes('not yet committed')"))
        js("showTab('src')")
        check('every cited file listed and matching', js(f"return document.querySelectorAll('#src-files tbody tr').length==={len(sources['files'])} && "
                                                         "document.querySelectorAll('#src-files .tag.meas').length===" + str(len(sources['files']))))
        check('settings matrix', js("return document.querySelectorAll('#src-settings tbody tr').length>=15"))

        # search palette and keyboard section switching
        js("document.getElementById('search-btn').click();const i=document.getElementById('pal-in');i.value='quarantine';i.dispatchEvent(new Event('input'))")
        check('search finds recovery content', js("return document.querySelectorAll('#pal-list li').length>0"))
        js("palClose();document.dispatchEvent(new KeyboardEvent('keydown',{key:'0'}))")
        check('key 0 opens section 10', js("return document.getElementById('v-src').classList.contains('on')"))

        # playback at 0.125× / 0.25× really advances and pauses
        js("showTab('pg');const e=document.getElementById('sim-sc');e.value='random';e.dispatchEvent(new Event('change'))")
        for speed in ('0.125', '0.25'):
            js(f"const sp=document.getElementById('sim-speed');sp.value='{speed}';sp.dispatchEvent(new Event('change'));archQA.simNow=0;archQA.simPlay=true;simKick()")
            t0 = js('return archQA.simNow')
            time.sleep(1.2)
            t1 = js('archQA.simPlay=false;return archQA.simNow')
            check(f'gate model advances at {speed}×', t1 > t0)

        # dark theme from the OS preference and from the toggle, then mobile
        s.chrome_pref('Services.prefs.setIntPref("ui.systemUsesDarkTheme", 1);')
        js("try{localStorage.removeItem('ariabc_theme')}catch(e){};location.hash='merkle';location.reload()")
        time.sleep(1.5)
        check('dark theme applied at load', js("return document.documentElement.dataset.theme==='dark' && uiErrors.length===0"))
        for t in ('topo', 'pg', 'merkle', 'recov', 'bench'):
            js(f"showTab('{t}');window.scrollTo(0,0)")
            s.shot(f'dark_{t}')
        js("document.getElementById('theme-btn').click()")
        time.sleep(1.5)
        check('theme toggle switches to light and persists', js("return document.documentElement.dataset.theme==='light' && localStorage.getItem('ariabc_theme')==='light' && uiErrors.length===0"))
        js("localStorage.removeItem('ariabc_theme')")
        with urllib.request.urlopen(f'{s.site}/DETERMINISM_EXPLORER.html') as r:
            check('page switch target exists', r.status == 200)
        check('zero captured JS errors', js('return uiErrors.length===0'))
        (s.out / 'results.json').write_text(json.dumps({
            'status': 'PASS', 'host': '10.129.148.247', 'checks': s.results, 'reviewed_head': sources['head'],
            'html_sha256': hashlib.sha256((source / PAGE).read_bytes()).hexdigest()}, indent=2))
        print(json.dumps({'status': 'PASS', 'checks': len(s.results), 'artifacts': str(s.out)}), flush=True)
    except Exception:
        (s.out / 'results.json').write_text(json.dumps({'status': 'FAIL', 'passed_before_failure': s.results}, indent=2))
        try:
            s.shot('failure', settle=0)
        except Exception:
            pass
        raise
    finally:
        s.close()


if __name__ == '__main__':
    main()
