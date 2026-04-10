"""
LCA Visa Dashboard — Flask server
Local:  python3 server.py          → http://localhost:5050
Server: set BASE_PATH env var to mount under a subfolder
        e.g. BASE_PATH=/visasearchmaster4000 gunicorn server:app
"""
import sqlite3
import json
import os
from flask import Flask, jsonify, request, send_from_directory

DB        = os.path.join(os.path.dirname(__file__), 'lca.db')
BASE_PATH = os.environ.get('BASE_PATH', '').rstrip('/')   # e.g. '/visasearchmaster4000'

app = Flask(__name__, static_folder='.', static_url_path='')
app.config['APPLICATION_ROOT'] = BASE_PATH or '/'


def get_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con


# ── Static files ──────────────────────────────────────────────────────────────
@app.route(BASE_PATH + '/')
@app.route(BASE_PATH + '/index.html')
def index():
    return send_from_directory('.', 'index.html')


# ── Dropdown options ──────────────────────────────────────────────────────────
@app.route(BASE_PATH + '/api/options')
def options():
    con = get_db()
    cur = con.cursor()
    result = {}
    for key, table in [('visa', 'meta_visa'), ('state', 'meta_state'),
                       ('soc', 'meta_soc'), ('status', 'meta_status')]:
        cur.execute(f'SELECT val FROM {table} WHERE val IS NOT NULL AND val != "" ORDER BY val')
        result[key] = [r['val'] for r in cur.fetchall()]
    con.close()
    return jsonify(result)


# ── Stats (for stat cards) ────────────────────────────────────────────────────
@app.route(BASE_PATH + '/api/stats')
def stats():
    con = get_db()
    cur = con.cursor()
    where, params = build_where(request.args)
    cur.execute(f'SELECT case_status, COUNT(*) as n FROM lca {where} GROUP BY case_status', params)
    rows = cur.fetchall()
    total = sum(r['n'] for r in rows)
    breakdown = {(r['case_status'] or 'Unknown'): r['n'] for r in rows}
    con.close()
    return jsonify({'total': total, 'breakdown': breakdown})


# ── Charts data ───────────────────────────────────────────────────────────────
@app.route(BASE_PATH + '/api/charts')
def charts():
    con = get_db()
    cur = con.cursor()
    where, params = build_where(request.args)

    # By visa class
    cur.execute(f'SELECT visa_class, COUNT(*) n FROM lca {where} GROUP BY visa_class ORDER BY n DESC', params)
    by_visa = {r['visa_class']: r['n'] for r in cur.fetchall() if r['visa_class']}

    # By status
    cur.execute(f'SELECT case_status, COUNT(*) n FROM lca {where} GROUP BY case_status ORDER BY n DESC', params)
    by_status = {r['case_status']: r['n'] for r in cur.fetchall() if r['case_status']}

    # Monthly trend (received_date YYYY-MM-DD)
    and_or_where = 'AND' if where else 'WHERE'
    cur.execute(f'''SELECT substr(received_date,1,7) AS month, COUNT(*) n
                    FROM lca {where} {and_or_where} received_date IS NOT NULL
                    GROUP BY month ORDER BY month''', params)
    by_month = {r['month']: r['n'] for r in cur.fetchall() if r['month']}

    # Top 15 employers
    cur.execute(f'''SELECT employer_name, COUNT(*) n FROM lca {where} {and_or_where} employer_name IS NOT NULL
                    GROUP BY employer_name ORDER BY n DESC LIMIT 15''', params)
    top_employers = [[r['employer_name'], r['n']] for r in cur.fetchall()]

    # Top 10 SOC titles
    cur.execute(f'''SELECT soc_title, COUNT(*) n FROM lca {where} {and_or_where} soc_title IS NOT NULL
                    GROUP BY soc_title ORDER BY n DESC LIMIT 10''', params)
    top_soc = [[r['soc_title'], r['n']] for r in cur.fetchall()]

    con.close()
    return jsonify({'by_visa': by_visa, 'by_status': by_status,
                    'by_month': by_month, 'top_employers': top_employers,
                    'top_soc': top_soc})


# ── Table rows (paginated) ────────────────────────────────────────────────────
@app.route(BASE_PATH + '/api/rows')
def rows():
    con = get_db()
    cur = con.cursor()
    where, params = build_where(request.args)

    # Count
    cur.execute(f'SELECT COUNT(*) n FROM lca {where}', params)
    total = cur.fetchone()['n']

    # Sort
    sort_col = request.args.get('sort', 'received_date')
    ALLOWED = {'case_number','case_status','visa_class','job_title','employer_name',
               'employer_state','worksite_state','received_date','wage_from','soc_title'}
    if sort_col not in ALLOWED:
        sort_col = 'received_date'
    sort_dir = 'ASC' if request.args.get('dir','desc') == 'asc' else 'DESC'

    page = max(1, int(request.args.get('page', 1)))
    per  = min(100, max(10, int(request.args.get('per', 50))))
    offset = (page - 1) * per

    cur.execute(f'''SELECT case_number, case_status, visa_class, job_title,
                           employer_name, employer_city, employer_state,
                           worksite_city, worksite_state,
                           wage_from, wage_unit, pw_wage_level,
                           received_date, decision_date, full_time, total_workers
                    FROM lca {where}
                    ORDER BY {sort_col} {sort_dir} NULLS LAST
                    LIMIT {per} OFFSET {offset}''', params)
    data = [dict(r) for r in cur.fetchall()]
    con.close()
    return jsonify({'total': total, 'page': page, 'per': per, 'rows': data})


# ── WHERE builder ─────────────────────────────────────────────────────────────
def build_where(args):
    clauses, params = [], []

    search = args.get('search', '').strip()
    if search:
        clauses.append('(employer_name LIKE ? OR job_title LIKE ? OR case_number LIKE ?)')
        like = f'%{search}%'
        params += [like, like, like]

    for arg, col in [('visa', 'visa_class'), ('status', 'case_status'),
                     ('state', 'worksite_state'), ('soc', 'soc_title')]:
        val = args.get(arg, '').strip()
        if val:
            clauses.append(f'{col} = ?')
            params.append(val)

    date_from = args.get('date_from', '').strip()
    date_to   = args.get('date_to', '').strip()
    if date_from:
        clauses.append('received_date >= ?')
        params.append(date_from)
    if date_to:
        clauses.append('received_date <= ?')
        params.append(date_to)

    wage_min = args.get('wage_min', '').strip()
    wage_max = args.get('wage_max', '').strip()
    if wage_min:
        clauses.append("(wage_unit = 'Year' AND wage_from >= ?)")
        params.append(float(wage_min))
    if wage_max:
        clauses.append("(wage_unit = 'Year' AND wage_from <= ?)")
        params.append(float(wage_max))

    if args.get('me') == '1':
        ME_KEYWORDS = [
            'UX Designer', 'UX/UI Designer', 'UI/UX Designer',
            'User Experience Designer', 'User Interface Designer',
            'Product Designer', 'Graphic Designer', 'Visual Designer',
            'Interaction Designer', 'Experience Designer',
            'Digital Designer', 'Brand Designer', 'Motion Designer',
            'Web Designer', 'UI Designer', 'Human Interface Designer',
            'UX Researcher', 'Design Lead', 'Design Manager',
            'Senior Designer', 'Staff Designer', 'Principal Designer',
        ]
        me_clauses = ' OR '.join(['job_title LIKE ?' for _ in ME_KEYWORDS])
        clauses.append(f'({me_clauses})')
        params += [f'%{kw}%' for kw in ME_KEYWORDS]

    where = ('WHERE ' + ' AND '.join(clauses)) if clauses else ''
    return where, params


if __name__ == '__main__':
    if not os.path.exists(DB):
        print('ERROR: lca.db not found. Run "python3 build_db.py" first.')
        exit(1)
    print('Dashboard running at http://localhost:5050')
    app.run(port=5050, debug=False)
