#!/usr/bin/env bash
set -euo pipefail
SRC_DIR="${1:-/home/admin/report/summary}"
DST_DIR="${2:-/home/admin/report/dashboard}"
TEMPLATE_FILE="${3:-/home/admin/soilslips-report/sliper_app_dashboard_template.html}"
python3 - "$SRC_DIR" "$DST_DIR" "$TEMPLATE_FILE" <<'PY'
import json, pathlib, sys, re, datetime, tempfile, os
src, dst, template_path = map(pathlib.Path, sys.argv[1:])
if not src.is_dir():
    sys.exit('ERROR: Input directory not found: ' + str(src))
if not template_path.is_file():
    sys.exit('ERROR: HTML template not found: ' + str(template_path))
reports = []
for path in src.rglob('soil_slips_status_????????.json'):
    try:
        with path.open(encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get('run'), dict) and data.get('date'):
            reports.append((data.get('checked_at', ''), path, data))
    except (OSError, ValueError) as exc:
        print('WARNING: skipping {}: {}'.format(path, exc), file=sys.stderr)
if not reports:
    sys.exit('ERROR: No valid reports under ' + str(src))
reports.sort(key=lambda item: (item[0], str(item[1])))
_, source_file, snapshot = reports[-1]
html = template_path.read_text(encoding='utf-8')
# Replace only the embedded JSON; preserve original layout, CSS and JavaScript.
pattern = r'(<script\s+id=["\']snapshot["\']\s+type=["\']application/json["\']\s*>)(.*?)(</script>)'
encoded = json.dumps(snapshot, ensure_ascii=False, separators=(',', ':')).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
html, n = re.subn(pattern, lambda m: m.group(1) + encoded + m.group(3), html, count=1, flags=re.S | re.I)
if n != 1:
    sys.exit('ERROR: Template must contain <script id="snapshot" type="application/json">...</script>')
now = datetime.datetime.now().astimezone()
archive_dir = dst / 'by_time' / now.strftime('%Y/%m/%d')
latest_dir = dst / 'latest'
archive_dir.mkdir(parents=True, exist_ok=True)
latest_dir.mkdir(parents=True, exist_ok=True)
archive_file = archive_dir / (now.strftime('%Y%m%d_%H') + '.html')
latest_file = latest_dir / 'index.html'
# Only latest auto-refreshes; hourly snapshots are static.
archive_html = re.sub(r'<meta\s+http-equiv=["\']refresh["\'][^>]*>', '', html, flags=re.I)
latest_html = archive_html.replace('</head>', '<meta http-equiv="refresh" content="120"></head>', 1)
def write_atomic(path, content):
    fd, tmp = tempfile.mkstemp(prefix='.soilslips-', suffix='.html', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
write_atomic(archive_file, archive_html)
write_atomic(latest_file, latest_html)
print('Source: ', source_file)
print('Archive:', archive_file)
print('Latest: ', latest_file)
PY
