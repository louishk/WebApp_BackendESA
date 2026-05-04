#!/usr/bin/env python3
"""
Render docs/api/recommendation_engine_public.md → recommendation_engine_public.html

Run from the repo root after editing the .md:
    python3 scripts/build_integration_guide_html.py

Requires: pip install --user markdown  (only on the dev machine).
The VM serves the resulting .html as a static file — no markdown dep needed
in production.
"""
from __future__ import annotations

import sys
from pathlib import Path

try:
    import markdown
except ImportError:
    print("ERROR: pip install --user markdown", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]
SRC      = ROOT / 'docs' / 'api' / 'recommendation_engine_public.md'
DST_HTML = ROOT / 'docs' / 'api' / 'recommendation_engine_public.html'
DST_PDF  = ROOT / 'docs' / 'api' / 'recommendation_engine_public.pdf'

CSS = """
:root {
  --fg: #1f2937; --bg: #ffffff; --muted: #6b7280;
  --code-bg: #f3f4f6; --code-fg: #111827;
  --pre-bg: #1f2937; --pre-fg: #f9fafb;
  --link: #1d4ed8; --link-hover: #1e40af;
  --rule: #e5e7eb; --table-header: #f9fafb;
  --note-bg: #fffbeb; --note-border: #fcd34d; --note-fg: #78350f;
  --quick-bg: #eff6ff; --quick-border: #bfdbfe; --quick-fg: #1e3a8a;
}
@media (prefers-color-scheme: dark) {
  :root {
    --fg: #f3f4f6; --bg: #0f172a; --muted: #94a3b8;
    --code-bg: #1e293b; --code-fg: #e2e8f0;
    --pre-bg: #0b1220; --pre-fg: #e2e8f0;
    --link: #60a5fa; --link-hover: #93c5fd;
    --rule: #334155; --table-header: #1e293b;
    --note-bg: #422006; --note-border: #b45309; --note-fg: #fde68a;
    --quick-bg: #0c1f3d; --quick-border: #1e40af; --quick-fg: #bfdbfe;
  }
}
* { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
               "Helvetica Neue", Arial, sans-serif;
  font-size: 15px; line-height: 1.6;
  color: var(--fg); background: var(--bg);
  max-width: 920px; margin: 0 auto; padding: 2rem 1.5rem 4rem;
}
h1 { font-size: 1.85rem; margin: 0 0 0.4rem; }
h2 { font-size: 1.4rem; margin: 2.2rem 0 0.8rem; padding-top: 1rem;
     border-top: 1px solid var(--rule); }
h3 { font-size: 1.08rem; margin: 1.4rem 0 0.6rem; }
h4 { font-size: 0.98rem; margin: 1rem 0 0.4rem; }
p, ul, ol { margin: 0.6rem 0; }
ul, ol { padding-left: 1.5rem; }
li { margin: 0.15rem 0; }
a { color: var(--link); text-decoration: none; }
a:hover { color: var(--link-hover); text-decoration: underline; }
hr { border: 0; border-top: 1px solid var(--rule); margin: 1.6rem 0; }

code {
  background: var(--code-bg); color: var(--code-fg);
  padding: 0.1em 0.35em; border-radius: 3px;
  font-family: "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 0.88em;
}
pre {
  background: var(--pre-bg); color: var(--pre-fg);
  padding: 0.85rem 1rem; border-radius: 6px;
  overflow-x: auto;
  font-family: "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 0.82em; line-height: 1.5;
}
pre code { background: none; color: inherit; padding: 0; font-size: inherit; }

table {
  border-collapse: collapse; width: 100%;
  margin: 0.7rem 0; font-size: 0.9em;
}
th, td {
  text-align: left; padding: 0.45rem 0.65rem;
  border: 1px solid var(--rule); vertical-align: top;
}
th { background: var(--table-header); font-weight: 600; }

blockquote {
  margin: 0.7rem 0; padding: 0.7rem 1rem;
  background: var(--note-bg); border: 1px solid var(--note-border);
  border-left-width: 4px; border-radius: 4px; color: var(--note-fg);
}
blockquote p { margin: 0.2rem 0; }

/* Header band */
.doc-header {
  margin: 0 0 1.6rem; padding-bottom: 1rem;
  border-bottom: 1px solid var(--rule);
}
.doc-header .crumbs {
  font-size: 0.82rem; color: var(--muted);
  margin-bottom: 0.5rem; letter-spacing: 0.01em;
}

/* Anchor offset for in-page links */
:target { scroll-margin-top: 1.5rem; }

/* Mobile */
@media (max-width: 600px) {
  body { padding: 1rem; font-size: 14px; }
  h1 { font-size: 1.5rem; }
  h2 { font-size: 1.2rem; }
  pre { font-size: 0.78em; }
}
"""

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Extra Space Asia — Recommendation & Booking API</title>
  <meta name="description" content="Public integration guide for the Extra Space Asia recommendation and booking API.">
  <style>{css}</style>
</head>
<body>
  <div class="doc-header">
    <div class="crumbs">Extra Space Asia · API Integration Guide · v1</div>
  </div>
  {body}
</body>
</html>
"""


def main() -> int:
    if not SRC.is_file():
        print(f"ERROR: source not found: {SRC}", file=sys.stderr)
        return 1

    src_text = SRC.read_text(encoding='utf-8')
    body_html = markdown.markdown(
        src_text,
        extensions=['extra', 'sane_lists', 'toc', 'codehilite'],
        extension_configs={
            'codehilite': {'css_class': 'codehilite', 'guess_lang': False},
        },
        output_format='html5',
    )

    html = HTML_TEMPLATE.format(css=CSS.strip(), body=body_html)
    DST_HTML.write_text(html, encoding='utf-8')

    src_kb  = SRC.stat().st_size / 1024
    html_kb = DST_HTML.stat().st_size / 1024
    print(f"Wrote {DST_HTML.relative_to(ROOT)}  ({src_kb:.1f} KB md → {html_kb:.1f} KB html)")

    # PDF — uses weasyprint if available; otherwise skip gracefully.
    try:
        from weasyprint import HTML as WeasyHTML  # type: ignore
    except ImportError:
        print("(skipping PDF — pip install --user weasyprint to enable)")
        return 0

    # Print-flavoured stylesheet — light theme, larger margins, page numbers.
    print_css = """
    @page { size: A4; margin: 18mm 16mm; }
    body { max-width: none; margin: 0; padding: 0; font-size: 10.5pt;
           color: #1f2937; background: white; }
    h1 { font-size: 22pt; }
    h2 { font-size: 15pt; page-break-before: auto; page-break-after: avoid; }
    h3 { font-size: 12pt; page-break-after: avoid; }
    pre { background: #f3f4f6; color: #111827; font-size: 8.5pt;
          padding: 0.55rem 0.75rem; border: 1px solid #e5e7eb; }
    code { background: #f3f4f6; color: #111827; }
    table { font-size: 9pt; }
    th { background: #f9fafb; }
    blockquote { background: #fffbeb; border-color: #fcd34d; color: #78350f; }
    .doc-header { border-bottom: 1px solid #e5e7eb; }
    .doc-header .crumbs { color: #6b7280; }
    /* Avoid splitting common code blocks across pages */
    pre, table { page-break-inside: avoid; }
    """
    pdf_html = HTML_TEMPLATE.format(css=print_css.strip(), body=body_html)
    WeasyHTML(string=pdf_html, base_url=str(ROOT)).write_pdf(str(DST_PDF))
    pdf_kb = DST_PDF.stat().st_size / 1024
    print(f"Wrote {DST_PDF.relative_to(ROOT)}  ({pdf_kb:.1f} KB pdf)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
