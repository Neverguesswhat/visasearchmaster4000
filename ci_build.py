"""
ci_build.py — non-interactive version of update_data.py for GitHub Actions / CI.
Reads QUARTERS_BACK env var (default 8 = 2 years) to decide how many quarters to include.
Always picks the most recent N available quarters.
"""
import urllib.request, urllib.error
import os, sys, json
from datetime import datetime, date

BASE_URL  = 'https://www.dol.gov/sites/dolgov/files/ETA/oflc/pdfs'
OUTPUT    = os.path.join(os.path.dirname(__file__), 'data.js')
CACHE_DIR = os.path.join(os.path.dirname(__file__), 'lca_cache')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
}

QUARTERS_BACK = int(os.environ.get('QUARTERS_BACK', '8'))

def fmt_date(v):
    if v is None: return None
    if isinstance(v, (datetime, date)): return v.strftime('%Y-%m-%d')
    return str(v).strip()[:10] or None

def file_exists(url):
    try:
        req = urllib.request.Request(url, headers={**HEADERS, 'Range': 'bytes=0-0'})
        res = urllib.request.urlopen(req, timeout=10)
        res.close()
        return True
    except Exception:
        return False

def download_file(url, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    req = urllib.request.Request(url, headers={**HEADERS, 'Accept': 'application/octet-stream'})
    with urllib.request.urlopen(req, timeout=300) as res:
        total = int(res.headers.get('Content-Length', 0))
        done = 0
        with open(dest, 'wb') as f:
            while True:
                chunk = res.read(131072)
                if not chunk: break
                f.write(chunk)
                done += len(chunk)
                if total:
                    pct = done / total * 100
                    print(f'\r    {pct:.1f}%  ({done//1024//1024}MB / {total//1024//1024}MB)', end='', flush=True)
    print()

def process_xlsx(path):
    import openpyxl
    print(f'  Parsing {os.path.basename(path)}…', flush=True)
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0: continue
        if row[0] is None: continue
        wage = row[72]
        try:    wage = float(str(wage).replace(',', '')) if wage is not None else None
        except: wage = None
        rows.append([
            str(row[0]),  # case_number
            row[1],       # case_status
            row[5],       # visa_class
            row[6],       # job_title
            row[19],      # employer_name
            row[70],      # worksite_state
            wage,         # wage_from
            row[74],      # wage_unit
            fmt_date(row[2]),  # received_date
            row[8],       # soc_title
        ])
    wb.close()
    print(f'    → {len(rows):,} rows')
    return rows

def main():
    print(f'QUARTERS_BACK = {QUARTERS_BACK}')

    # Build list of all possible quarters newest-first
    all_quarters = [
        (fy, q)
        for fy in range(2026, 2020, -1)
        for q  in range(4, 0, -1)
    ]

    # Find the most recent N available ones
    print('\n▸ Scanning for available quarters…')
    chosen = []
    for fy, q in all_quarters:
        if len(chosen) >= QUARTERS_BACK:
            break
        fname = f'LCA_Disclosure_Data_FY{fy}_Q{q}.xlsx'
        url   = f'{BASE_URL}/{fname}'
        dest  = os.path.join(CACHE_DIR, fname)
        if os.path.exists(dest):
            print(f'  FY{fy} Q{q}  [cached]')
            chosen.append((fy, q, url, dest))
        elif file_exists(url):
            print(f'  FY{fy} Q{q}  [will download]')
            chosen.append((fy, q, url, dest))
        else:
            print(f'  FY{fy} Q{q}  [not found, skipping]')

    if not chosen:
        print('ERROR: No files found.')
        sys.exit(1)

    print(f'\n▸ Downloading {len(chosen)} quarters…')
    os.makedirs(CACHE_DIR, exist_ok=True)
    for fy, q, url, dest in chosen:
        if os.path.exists(dest):
            print(f'  FY{fy} Q{q}: already cached, skipping')
        else:
            print(f'  FY{fy} Q{q}: downloading…')
            download_file(url, dest)
            print(f'  FY{fy} Q{q}: done')

    print(f'\n▸ Processing {len(chosen)} files…')
    all_rows = []
    for fy, q, url, dest in chosen:
        print(f'\n  FY{fy} Q{q}:')
        all_rows.extend(process_xlsx(dest))

    # Deduplicate by case_number, keep newest
    print(f'\n▸ Deduplicating {len(all_rows):,} rows…')
    seen = {}
    for r in all_rows:
        k = r[0]
        if k not in seen or (r[8] or '') > (seen[k][8] or ''):
            seen[k] = r
    deduped = sorted(seen.values(), key=lambda r: r[8] or '', reverse=True)
    print(f'  → {len(deduped):,} unique cases')

    print(f'\n▸ Writing data.js…')
    with open(OUTPUT, 'w') as f:
        f.write('const D=' + json.dumps(deduped, separators=(',', ':')) + ';')
    mb = os.path.getsize(OUTPUT) / 1024 / 1024
    print(f'  ✓ {len(deduped):,} rows  ·  {mb:.1f}MB')

if __name__ == '__main__':
    main()
