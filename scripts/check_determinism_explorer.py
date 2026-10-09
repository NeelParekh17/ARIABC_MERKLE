#!/usr/bin/env python3
"""Run Firefox UI QA on the lab host, with an isolated profile and HTTP server."""
import base64
import functools
import hashlib
import http.server
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

def main():
    if not Path('/home/neel/Desktop/ariabc_cluster').is_dir() or not Path('/snap/bin/geckodriver').exists():
        raise SystemExit('Run on lab 10.129.148.247; Firefox/geckodriver required.')
    source = Path(sys.argv[1]).resolve()
    (source/'.bench_tmp').mkdir(exist_ok=True)  # never /tmp on lab hosts
    out = Path(tempfile.mkdtemp(prefix='determinism_ui_'+time.strftime('%Y%m%d_'), dir=source/'.bench_tmp'))
    web = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(source)))
    threading.Thread(target=web.serve_forever, daemon=True).start()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    log = (out / 'geckodriver.log').open('w')
    driver = subprocess.Popen(['/snap/bin/geckodriver', '--allow-system-access', '--host', '127.0.0.1', '--port', str(port)], stdout=log, stderr=subprocess.STDOUT)
    base = f'http://127.0.0.1:{port}'
    sid = None
    results = []

    def call(path, data=None, method=None):
        req = urllib.request.Request(base+path, data=json.dumps(data).encode() if data is not None else None,
                                     headers={'Content-Type': 'application/json'}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=45) as response: result = json.load(response)
        except urllib.error.HTTPError as e: raise RuntimeError(e.read().decode()) from e
        value = result.get('value')
        if isinstance(value, dict) and 'error' in value: raise RuntimeError(value)
        return value

    def js(code):
        return call(f'/session/{sid}/execute/sync', {'script': code, 'args': []})

    def check(name, condition):
        if not condition: raise AssertionError(name)
        results.append(name)

    def shot(name):
        time.sleep(.25)  # Capture the settled layout after the chapter transition.
        (out/(name+'.png')).write_bytes(base64.b64decode(call(f'/session/{sid}/screenshot')))

    try:
        for _ in range(100):
            try: call('/status'); break
            except (OSError, RuntimeError): time.sleep(.1)
        session = call('/session', {'capabilities': {'alwaysMatch': {'browserName': 'firefox', 'moz:firefoxOptions': {
            'binary': '/snap/firefox/current/usr/lib/firefox/firefox', 'args': ['-headless'],
            'prefs': {'browser.shell.checkDefaultBrowser': False}}}}})
        sid = session['sessionId']
        call(f'/session/{sid}/window/rect', {'width': 1440, 'height': 1200})
        call(f'/session/{sid}/url', {'url': f'http://127.0.0.1:{web.server_port}/DETERMINISM_EXPLORER.html'})
        check('page initialized without errors', js("return typeof determinismExplorer==='object' && uiErrors.length===0"))
        n=js('return determinismExplorer.chapters.length')
        check('all 17 chapters available', n==17)
        check('model validates all schedules', js('return determinismExplorer.validateModel().scenes===17'))
        check('paused on load', js('return !determinismExplorer.state.playing'))
        event_count=0
        for width in (1440,736):
            call(f'/session/{sid}/window/rect', {'width':width,'height':1200})
            for i in range(n):
                js(f'determinismExplorer.choose({i})')
                frames=js('return determinismExplorer.chapters[determinismExplorer.state.chapter].frames.map(f=>f.t)')
                event_count+=len(frames) if width==1440 else 0
                for j,t in enumerate(frames):
                    js(f'determinismExplorer.seek({t})')
                    check(f'scene {i} event {j} synchronized at {width}',js("""const d=determinismExplorer,m=d.model;
                        const rows=[...document.querySelectorAll('.db-row')];
                        return document.getElementById('P').textContent===String(m.P)
                        && document.getElementById('C').textContent===String(m.C)
                        && rows.every(r=>r.querySelector('strong').textContent===String(m.db[r.querySelector('span').textContent]))
                        && document.getElementById('source-code').textContent.length>30
                        && document.getElementById('event-title').textContent.length>5"""))
                saved=js('return JSON.stringify(determinismExplorer.model)')
                js('determinismExplorer.seek(0);determinismExplorer.seek(determinismExplorer.chapters[determinismExplorer.state.chapter].duration)')
                check(f'seeking reconstructs scene {i} at {width}',js('return JSON.stringify(determinismExplorer.model)')==saved)
                check(f'scene {i} containment at {width}',js('return document.documentElement.scrollWidth<=innerWidth+1'))
                check(f'scene {i} reverse/next at {width}',js("document.getElementById('prev').click();document.getElementById('next').click();return !determinismExplorer.state.playing"))
            js('determinismExplorer.choose(2);determinismExplorer.seek(15)')
            shot(f'overlap_{width}')
        # Independently specified expected outcomes for the illustrated schedules.
        for scene_index,expected in [(2,{'P':2,'C':2,'db':{'A':1,'B':1,'D':1}}),
                (4,{'P':1,'C':1,'db':{'matches':1,'B':1}}),
                (5,{'P':1,'C':1,'db':{'row key':8,'found key 7':False}}),
                (6,{'P':0,'C':0,'db':{'value':'absent'}}),
                (9,{'P':2,'C':2,'db':{'A':1,'B':0,'D':1}}),
                (12,{'P':1,'C':1,'db':{'value':11}}),
                (13,{'P':1,'C':1,'db':{'v':0}}),
                (14,{'P':0,'C':0,'db':{'rows at leaf':1}})]:
            got=js(f"determinismExplorer.choose({scene_index});determinismExplorer.seek(determinismExplorer.chapters[{scene_index}].duration);const m=determinismExplorer.model;return {{P:m.P,C:m.C,db:m.db}}")
            check(f'specified final outcome scene {scene_index}',got==expected)
        check('out-of-order completion leaves a hole',js('determinismExplorer.choose(2);determinismExplorer.seek(15);return determinismExplorer.model.C===-1 && determinismExplorer.model.ready.length===2 && document.querySelectorAll(".slot.hole").length===1'))
        check('P release does not expose DB writes',js('determinismExplorer.choose(2);determinismExplorer.seek(9);return determinismExplorer.model.P===2 && determinismExplorer.model.db.A===0 && determinismExplorer.model.db.B===0'))
        check('stale-snapshot error re-runs before it is finalized',js('determinismExplorer.choose(12);determinismExplorer.seek(3);const a=determinismExplorer.model;determinismExplorer.seek(9);const b=determinismExplorer.model;return a.tx[1].opf && !a.tx[1].done && b.P===1 && b.C===1 && b.tx[1].outcome==="ERROR 23514" && b.tx[1].mode==="physical"'))
        check('stale error re-run matches serial order',js('determinismExplorer.choose(13);determinismExplorer.seek(determinismExplorer.chapters[13].duration);const m=determinismExplorer.model;return m.tx[1].outcome==="committed" && m.tx[1].sqlRuns===2 && m.db.v===0'))
        check('conflict frame draws a dependency arrow',js('determinismExplorer.choose(3);determinismExplorer.seek(9);return document.querySelectorAll("#effects .fx-dep").length===1 && document.querySelectorAll("#effects .fx-abort").length===1'))
        check('event change summary lists frontier moves',js('determinismExplorer.choose(2);determinismExplorer.seek(18);return document.getElementById("changes").textContent.includes("C -1 → 2")'))
        check('settle keeps SQL and queue frozen',js('determinismExplorer.choose(9);determinismExplorer.seek(15);const tx=determinismExplorer.model.tx[1];return tx.sqlRuns===1 && tx.queue[0]==="B ← 0"'))
        for reason,ref in [('trigger','trigger'),('upsert','upsert'),('sequence','sequence'),('insert','scanpending'),('indexed','indexpending')]:
            js(f"determinismExplorer.choose(7);document.getElementById('variant').value='{reason}';document.getElementById('variant').dispatchEvent(new Event('change'));determinismExplorer.seek(3)")
            check('fallback reason source '+reason,js(f"return document.getElementById('source-select').value==='{ref}' && determinismExplorer.model.tx[1].opf"))
            js('determinismExplorer.seek(12)')
            check('fallback physical route '+reason,js('return determinismExplorer.model.C===0 && determinismExplorer.model.P===0 && determinismExplorer.model.tx[1].mode==="physical"'))
            js('determinismExplorer.seek(15)')
            check('fallback releases after SQL before commit '+reason,js('return determinismExplorer.model.P===1 && !determinismExplorer.model.tx[1].done && determinismExplorer.model.db.effect===0'))
        # Check actual SVG movement during requestAnimationFrame playback.
        js("determinismExplorer.choose(2);determinismExplorer.seek(3);document.getElementById('play').click()")
        time.sleep(.12)
        pos1=js("return document.querySelector('[data-tx=\"0\"]').getAttribute('transform')")
        time.sleep(.35)
        pos2=js("return document.querySelector('[data-tx=\"0\"]').getAttribute('transform')")
        js("document.getElementById('play').click()")
        check('transaction SVG visibly moves during playback',pos1!=pos2)
        for speed in (.125,.25,.5,1,2):
            js(f"determinismExplorer.choose(2);document.getElementById('speed').value='{speed}';document.getElementById('speed').dispatchEvent(new Event('change'));document.getElementById('play').click()")
            time.sleep(1.1)
            elapsed=js("document.getElementById('play').click();return determinismExplorer.state.t")
            check(f'real playback at {speed}x',.4*speed<elapsed<1.8*speed)
            time.sleep(.15)
            check(f'pause at {speed}x',js('return determinismExplorer.state.t')==elapsed)
        # A real Firefox desktop window can stop RAF while document.hidden is false.
        js("window.qaOriginalRaf=requestAnimationFrame;window.requestAnimationFrame=()=>0;determinismExplorer.choose(2);determinismExplorer.seek(3);document.getElementById('speed').value='1';document.getElementById('speed').dispatchEvent(new Event('change'));document.getElementById('play').click()")
        time.sleep(.9)
        check('playback survives withheld animation frames',js("return determinismExplorer.state.t>3.3 && !document.hidden"))
        js("document.getElementById('play').click();window.requestAnimationFrame=window.qaOriginalRaf;requestAnimationFrame(tick)")
        check('scrubbing pauses',js("document.getElementById('progress').value=10;document.getElementById('progress').dispatchEvent(new Event('input'));return determinismExplorer.state.t===10 && !determinismExplorer.state.playing"))
        check('animation-only toggle',js("document.getElementById('explain').click();const hidden=getComputedStyle(document.getElementById('event-body')).display==='none'&&getComputedStyle(document.getElementById('event-title')).display!=='none';document.getElementById('explain').click();return hidden&&getComputedStyle(document.getElementById('event-body')).display!=='none'"))
        check('fit diagram control',js("document.getElementById('fit').click();const fit=document.getElementById('diagram-wrap').classList.contains('fit');document.getElementById('fit').click();return fit&&!document.getElementById('diagram-wrap').classList.contains('fit')"))
        check('source jump',js("document.getElementById('jump-source').click();return document.getElementById('source-details').open"))
        call(f'/session/{sid}/url',{'url':f'http://127.0.0.1:{web.server_port}/DETERMINISM_EXPLORER.html#overlay'})
        check('chapter deep link',js('return determinismExplorer.chapters[determinismExplorer.state.chapter].id==="overlay"'))
        check('dependency lens follows command counter',js('determinismExplorer.seek(6);return determinismExplorer.model.lens.visible==="11" && document.querySelector(".lens").textContent.includes("Command 1")'))
        check('Merkle lens preserves +2 with zero hash',js('determinismExplorer.choose(14);determinismExplorer.seek(9);const l=determinismExplorer.model.lens;return l.ih==="0" && l.ic===2 && document.querySelector(".lens").textContent.includes("+2")'))
        # Independently verify served bytes and the exact line text behind each excerpt.
        manifest=json.loads((source/'docs/determinism/source_manifest.json').read_text())
        for name,meta in manifest['files'].items():
            with urllib.request.urlopen(f'http://127.0.0.1:{web.server_port}/{name}') as response:raw=response.read()
            check('served source hash '+name,hashlib.sha256(raw).hexdigest()==meta['sha256'])
        for name,excerpt in manifest['excerpts'].items():
            if excerpt.get('paper'):continue
            lines=(source/excerpt['path']).read_text().splitlines()
            text='\n'.join(f'{i}: {lines[i-1]}' for i in range(excerpt['start'],excerpt['end']+1))
            check('exact anchored excerpt '+name,text==excerpt['text'] and excerpt['anchor'] in text)
        for name in ['DETERMINISM_GUIDE.md','ARCHITECTURE_EXPLORER.html']:
            with urllib.request.urlopen(f'http://127.0.0.1:{web.server_port}/{name}') as response:
                response.read()
                check('related link '+name,response.status==200)
        call(f'/session/{sid}/window/rect',{'width':1440,'height':1200})
        js('window.scrollTo(0,0);document.getElementById("source-details").open=false')
        for i,t,name in [(7,12,'physical_fallback'),(6,9,'own_write_overlay'),(4,6,'relation_coverage'),(9,12,'frozen_settle'),(12,9,'terminal_error'),(13,12,'stale_error_rerun'),(14,9,'merkle_net_count'),(3,9,'early_conflict_arrow')]:
            js(f'determinismExplorer.choose({i});determinismExplorer.seek({t});window.scrollTo(0,0)')
            shot(name)
        check('zero captured JS errors',js('return uiErrors.length===0'))
        # Firefox can emulate the theme preference without modifying the page CSS.
        call(f'/session/{sid}/moz/context', {'context': 'chrome'})
        call(f'/session/{sid}/execute/sync', {'script': 'Services.prefs.setIntPref("ui.systemUsesDarkTheme", 1);', 'args': []})
        call(f'/session/{sid}/moz/context', {'context': 'content'})
        time.sleep(.2)
        check('dark theme preference', js('return matchMedia("(prefers-color-scheme: dark)").matches'))
        shot('dark')
        call(f'/session/{sid}/window/rect', {'width':360,'height':1000})
        check('dark mobile containment', js('return document.documentElement.scrollWidth<=innerWidth+1'))
        shot('dark_mobile')
        call(f'/session/{sid}/moz/context', {'context': 'chrome'})
        call(f'/session/{sid}/execute/sync', {'script': 'Services.prefs.setIntPref("ui.prefersReducedMotion", 1);', 'args': []})
        call(f'/session/{sid}/moz/context', {'context': 'content'})
        time.sleep(.2)
        check('reduced motion preference is respected', js('return matchMedia("(prefers-reduced-motion: reduce)").matches && !document.getElementById("motion-notice").hidden'))
        # Firefox desktop may clamp outer windows above the requested mobile
        # width. An actual iframe viewport verifies CSS at exactly 360/320px.
        for width in (360, 320):
            iframe = js(f"const frame=document.createElement('iframe');frame.src=location.href;frame.style.cssText='position:fixed;left:0;top:0;width:{width}px;height:1000px;border:0';document.body.replaceChildren(frame);return frame;")
            time.sleep(.3)
            call(f'/session/{sid}/frame', {'id':iframe})
            check(f'exact viewport {width}', js(f'return innerWidth==={width}'))
            for i in range(n):
                js(f"document.querySelector('[data-chapter=\"{i}\"]').click();")
                check(f'exact {width}px chapter {i} containment', js('return document.documentElement.scrollWidth<=innerWidth+1'))
                check(f'exact {width}px chapter {i} event controls', js("const d=determinismExplorer;for(let n=0;n<d.chapters[d.state.chapter].frames.length+1;n++)document.getElementById('next').click();return d.state.t===d.chapters[d.state.chapter].duration"))
            js('determinismExplorer.choose(2);determinismExplorer.seek(7)')
            time.sleep(.25)
            call(f'/session/{sid}/frame', {'id':None})
            element_id = iframe['element-6066-11e4-a52e-4f735466cecf']
            (out/f'exact_mobile_{width}.png').write_bytes(base64.b64decode(call(f'/session/{sid}/element/{element_id}/screenshot')))
        (out/'results.json').write_text(json.dumps({'status':'PASS','host':'10.129.148.247','checks':results,
            'scenes':n,'events':event_count,'html_sha256':hashlib.sha256((source/'DETERMINISM_EXPLORER.html').read_bytes()).hexdigest()},indent=2))
        print(json.dumps({'status':'PASS','checks':len(results),'artifacts':str(out)}),flush=True)
    finally:
        if sid:
            try: call(f'/session/{sid}', method='DELETE')
            except Exception: pass
        driver.terminate()
        try: driver.wait(timeout=10)
        except subprocess.TimeoutExpired: driver.kill(); driver.wait()
        web.shutdown();web.server_close();log.close()

if __name__ == '__main__':
    main()
