"""
update_data.py — Fetch latest LCA disclosure files from DOL and rebuild data.js
Usage: python3 update_data.py
"""
import urllib.request, urllib.error
import os, sys, json, time
from datetime import datetime

BASE_URL   = 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs'
CACHE_DIR  = os.path.join(os.path.dirname(__file__), 'lca_cache')
OUTPUT     = os.path.join(os.path.dirname(__file__), 'data.js')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
}

# All quarters to probe: FY2021 Q1 → current
QUARTERS = [
    (fy, q)
    for fy in range(2021, 2027)
    for q  in range(1, 5)
]

COLS = {  # column index in the xlsx → key in output
    'CASE_NUMBER':            0,
    'CASE_STATUS':            1,
    'RECEIVED_DATE':          2,
    'VISA_CLASS':             5,
    'JOB_TITLE':              6,
    'SOC_TITLE':              8,
    'EMPLOYER_NAME':         19,
    'WORKSITE_STATE':        70,
    'WAGE_RATE_OF_PAY_FROM': 72,
    'WAGE_UNIT_OF_PAY':      74,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_date(v):
    if v is None: return None
    from datetime import datetime as dt, date
    if isinstance(v, (dt, date)): return v.strftime('%Y-%m-%d')
    s = str(v).strip()[:10]
    return s if s else None

def head(url):
    """Returns file size in bytes, or None if not found."""
    try:
        h = {**HEADERS, 'Range': 'bytes=0-0'}
        req = urllib.request.Request(url, headers=h)
        res = urllib.request.urlopen(req, timeout=10)
        # Content-Range: bytes 0-0/TOTAL
        cr = res.headers.get('Content-Range', '')
        total = int(cr.split('/')[-1]) if '/' in cr else 0
        res.close()
        return total or 1  # 1 = exists but size unknown
    except urllib.error.HTTPError as e:
        if e.code == 404: return None
        return None
    except Exception:
        return None

def download(url, dest, label):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    req = urllib.request.Request(url, headers={**HEADERS, 'Accept': 'application/octet-stream'})
    with urllib.request.urlopen(req, timeout=120) as res:
        total = int(res.headers.get('Content-Length', 0))
        done  = 0
        with open(dest, 'wb') as f:
            while True:
                chunk = res.read(65536)
                if not chunk: break
                f.write(chunk)
                done += len(chunk)
                if total:
                    pct = done / total * 100
                    bar = '█' * int(pct / 5) + '░' * (20 - int(pct / 5))
                    print(f'\r  [{bar}] {pct:5.1f}%  {done//1024//1024}MB / {total//1024//1024}MB  ', end='', flush=True)
    print()

def process_xlsx(path):
    import openpyxl
    print(f'  Parsing {os.path.basename(path)}…', flush=True)
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0: continue  # skip header
        r0 = row[0]
        if r0 is None: continue  # skip blank rows
        wage = row[72]
        try:    wage = float(str(wage).replace(',','')) if wage else None
        except: wage = None
        rows.append([
            str(r0) if r0 else None,  # case_number
            row[1],                    # case_status
            row[5],                    # visa_class
            row[6],                    # job_title
            row[19],                   # employer_name
            row[70],                   # worksite_state
            wage,                      # wage_from
            row[74],                   # wage_unit
            fmt_date(row[2]),          # received_date
            row[8],                    # soc_title
        ])
        if i % 100000 == 0 and i > 0:
            print(f'    …{i:,} rows', flush=True)
    wb.close()
    print(f'  → {len(rows):,} valid rows')
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print('═' * 60)
    print('  LCA Data Updater — DOL Disclosure Files')
    print('═' * 60)

    # ── Step 1: probe available files ─────────────────────────────────────────
    print('\n▸ Scanning for available files…\n')
    available = []
    for fy, q in QUARTERS:
        fname = f'LCA_Disclosure_Data_FY{fy}_Q{q}.xlsx'
        url   = f'{BASE_URL}/{fname}'
        size  = head(url)
        if size is not None:
            cached = os.path.exists(os.path.join(CACHE_DIR, fname))
            mb_str = f'{size//1024//1024}MB' if size > 1 else '?MB'
            tag = '  ✓ cached' if cached else ''
            print(f'  [{len(available)+1:2d}]  FY{fy} Q{q}  {mb_str}{tag}')
            available.append({'fy': fy, 'q': q, 'fname': fname, 'url': url, 'mb': size//1024//1024, 'cached': cached})

    if not available:
        print('  No files found. Check your internet connection.')
        sys.exit(1)

    # ── Step 2: pick files ────────────────────────────────────────────────────
    print(f'\n  Found {len(available)} files.')
    print('  Enter numbers to select (e.g. "1 2 3"), "all" for all, or "latest" for the most recent:')
    raw = input('  › ').strip().lower()

    if raw == 'all':
        chosen = available
    elif raw == 'latest':
        chosen = [available[-1]]
    else:
        idxs   = [int(x)-1 for x in raw.split() if x.isdigit()]
        chosen = [available[i] for i in idxs if 0 <= i < len(available)]

    if not chosen:
        print('  Nothing selected. Exiting.')
        sys.exit(0)

    print(f'\n  Selected: {", ".join(f"FY{c[\"fy\"]} Q{c[\"q\"]}" for c in chosen)}')

    # ── Step 3: download missing files ───────────────────────────────────────
    print('\n▸ Downloading…\n')
    os.makedirs(CACHE_DIR, exist_ok=True)
    for c in chosen:
        dest = os.path.join(CACHE_DIR, c['fname'])
        if os.path.exists(dest):
            print(f'  Skipping {c["fname"]} (already cached)')
        else:
            print(f'  Downloading {c["fname"]}  ({c["mb"]}MB)…')
            download(c['url'], dest, c['fname'])

    # ── Step 4: process ───────────────────────────────────────────────────────
    print('\n▸ Processing…\n')
    all_rows = []
    for c in chosen:
        dest = os.path.join(CACHE_DIR, c['fname'])
        rows = process_xlsx(dest)
        all_rows.extend(rows)

    # Deduplicate by case_number
    print(f'\n  Total rows before dedup: {len(all_rows):,}')
    seen = set()
    deduped = []
    for r in all_rows:
        k = r[0]
        if k not in seen:
            seen.add(k)
            deduped.append(r)
    print(f'  Total rows after dedup:  {len(deduped):,}')

    # Sort newest first
    deduped.sort(key=lambda r: r[8] or '', reverse=True)

    # ── Step 5: write data.js ─────────────────────────────────────────────────
    print(f'\n▸ Writing data.js…')
    with open(OUTPUT, 'w') as f:
        f.write('const D=' + json.dumps(deduped, separators=(',', ':')) + ';')
    mb = os.path.getsize(OUTPUT) / 1024 / 1024
    print(f'  ✓ {OUTPUT}')
    print(f'  {len(deduped):,} rows  ·  {mb:.1f}MB')

    print(f'\n▸ Done! Upload data.js to your server to update the dashboard.\n')

if __name__ == '__main__':
    main()
