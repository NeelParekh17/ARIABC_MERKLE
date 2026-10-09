# Updated source-backed determinism animations

Open http://127.0.0.1:8765/DETERMINISM_EXPLORER.html. Use the existing repository loopback server; if needed, run python3 scripts/open_architecture_explorer.py --no-open. Firefox Snap may not open repository files directly from /work.

The standalone HTML embeds all assets and source excerpts. Its 17 event-driven chapters derive transaction positions, deferred queues, publication/finalization prefixes, completion holes, dependency diagrams and visible data from one model. Play, pause, step, scrub, choose 0.125×/0.25×, select fallback reasons, hide explanatory paragraphs or fit the diagram. Narrow screens follow the active transaction and also allow manual panning. Each chapter has a URL fragment, such as #fallback or #overlay.

Asset sources:
- model.js: reviewed illustrative schedules and state invariants.
- view.js: SVG rendering, controls, source inspector and animation.
- style.css and template.html: responsive document and themes.
- source_manifest.json: exact reviewed source/PDF hashes and anchored excerpts.

After reviewing source changes, rebuild with python3 scripts/build_determinism_evidence.py. This regenerates the portable page and manifest; it does not automatically certify changed semantics.

Run scripts/check_determinism_explorer.py on the lab machine only. It uses an isolated Firefox/WebDriver profile and temporary HTTP server, starts no database or gateway, and checks motion, event consistency, source fidelity, responsive layouts and controls. Screenshots and results go to <checkout>/.bench_tmp/determinism_ui_<date>_*/ on the lab host (never /tmp).

The existing architecture explorer and paper remain unchanged. See DETERMINISM_GUIDE.md for the detailed paper/source comparison.
