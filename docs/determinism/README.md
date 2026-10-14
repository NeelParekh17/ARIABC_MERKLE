# Source-backed determinism animations

Open both explorers with `./open_websites.sh` (it serves the checkout on http://127.0.0.1:8765 and opens the pages). Firefox Snap may not open repository files directly from /work.

The standalone `DETERMINISM_EXPLORER.html` embeds all assets and source excerpts. Its 18 event-driven chapters (addressable by URL fragment, such as `#admission`, `#fallback` or `#merkle`) derive transaction positions, deferred queues, publication/finalization prefixes, completion holes, dependency diagrams and visible data from one model. Play, pause, step, scrub, choose 0.125×/0.25×, select any of the eight fallback reasons, hide explanatory paragraphs or fit the diagram. Narrow screens follow the active transaction and allow manual panning.

Asset sources:
- `model.js`: reviewed illustrative schedules and state invariants.
- `view.js`: SVG rendering, controls, source inspector and animation.
- `style.css` and `template.html`: page layout on top of the shared design system in `docs/explorers/` (suite bar, light/dark themes, live source-freshness badge).
- `source_manifest.json`: exact reviewed source/PDF hashes, anchored excerpts and asset hashes.

After reviewing source changes, update the models, bump `REVIEWED_HEAD` in `scripts/explorer_common.py`, and rebuild with `python3 scripts/build_determinism_evidence.py` (or `--snapshot SOURCE_SNAPSHOT.json` in a copied tree without `.git`). `--check` compares without writing. The builder refuses any other head or dirty cited files; rebuilding never certifies changed semantics.

Run `scripts/check_determinism_explorer.py <checkout>` on the lab machine only. It uses an isolated Firefox/WebDriver profile and a temporary HTTP server, starts no database or gateway, and checks motion, event consistency, source fidelity, freshness, responsive layouts and controls. Screenshots and results go to `<checkout>/.bench_tmp/determinism_ui_<date>_*/` on the lab host (never /tmp).

See `DETERMINISM_GUIDE.md` for the detailed paper/source comparison and `docs/explorers/README.md` for the shared build and review workflow.
