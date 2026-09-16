#!/usr/bin/env python3
"""
Hot Streaks — daily data pipeline for hotstreaks.ymlux.shop
===========================================================

Fetches finished matches for the 28 tracked competitions from FotMob, keeps a
growing match-level history in data/history/*.jsonl, evaluates all 78 run
("streak") types, and writes data/streaks.json which index.html loads.

Standard library only — no pip installs, runs anywhere with Python 3.9+.

Modes
-----
  full      (default) nightly maintenance: refresh recent days, rebuild
            fixtures, extend the backfill by one chunk, rebuild streaks.json
  daily     refresh recent days + fixtures + streaks (no backfill)
  backfill  walk backwards filling history (3 chunks per run)
  rebuild   offline: recompute streaks.json from history only

Example (what the GitHub Action runs)
-------------------------------------
  python3 scripts/hotstreaks.py --mode full \
      --days 4 --chunk-days 21 --max-details 6000 --max-minutes 90
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

BASE = "https://www.fotmob.com"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
HIST = os.path.join(DATA, "history")
OUT_JSON = os.path.join(DATA, "streaks.json")
STATE_JSON = os.path.join(DATA, "state.json")
VENUE_CACHE = os.path.join(DATA, "venue_cache.json")

WAT = timezone(timedelta(hours=1))      # dashboard shows kickoffs + "as of" in WAT
DEFAULT_HORIZON = "2024-08-01"          # oldest date the backfill may reach
MIN_STREAK = 3                          # matches — aligns with the dashboard's default filter
RECENT_N = 6                            # run-detail lines per card
FORM_N = 5                              # form dots
FIXTURE_DAYS = 16                       # forward window for "next match"
WORKERS_DETAIL = 6
WORKERS_TREE = 4

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.fotmob.com/",
}

# dashboard label, country, FotMob primary league ids.
# Three labels deliberately merge two competitions, matching the current site.
LEAGUES = [
    ("Premier League",       "England",        [47]),
    ("Championship",         "England",        [48]),
    ("League One",           "England",        [108]),
    ("La Liga",              "Spain",          [87]),
    ("Segunda División",     "Spain",          [140]),
    ("Serie A",              "Italy",          [55, 268]),   # + Brazil Serie A
    ("Serie B",              "Italy",          [86]),
    ("Bundesliga",           "Germany",        [54, 38]),    # + Austria Bundesliga
    ("2. Bundesliga",        "Germany",        [146]),
    ("Ligue 1",              "France",         [53]),
    ("Ligue 2",              "France",         [110]),
    ("Eredivisie",           "Netherlands",    [57]),
    ("Belgian Pro League",   "Belgium",        [40]),
    ("Primeira Liga",        "Portugal",       [61]),
    ("Süper Lig",            "Turkey",         [71]),
    ("Super League Greece",  "Greece",         [135]),
    ("Superliga",            "Romania",        [189]),
    ("Ekstraklasa",          "Poland",         [196]),
    ("Eliteserien",          "Norway",         [59]),
    ("Allsvenskan",          "Sweden",         [67]),
    ("Veikkausliiga",        "Finland",        [51]),
    ("Scottish Premiership", "Scotland",       [64]),
    ("Premier Division",     "Ireland",        [126]),
    ("Super League",         "Switzerland",    [69, 120]),   # + China Super League
    ("MLS",                  "USA",            [130]),
    ("Liga MX",              "Mexico",         [230]),
    ("Liga Profesional",     "Argentina",      [112]),
    ("J1 League",            "Japan",          [223]),
]

LEAGUE_LABELS = [l for l, _c, _i in LEAGUES]
TRACKED_IDS = {i for _l, _c, ids in LEAGUES for i in ids}
ID_MAP = {i: (l, c) for l, c, ids in LEAGUES for i in ids}

# Competitions that never contribute history but may supply a "next match"
# (European nights, cups).  Anything else outside the 28 also shows up when a
# tracked team plays in it, labelled with FotMob's own competition name.
EXTRA_FIXTURE_IDS = {
    42: "UEFA Champions League", 73: "UEFA Europa League",
    10216: "UEFA Conference League", 44: "UEFA Europa Conference League",
    45: "FA Cup", 133: "EFL Cup", 132: "Coupe de France",
    136: "Coppa Italia", 141: "Copa del Rey", 73_1: "Taça de Portugal",
}

MARKETS = [
    {"id": "goals", "label": "Goals / 1X2"},
    {"id": "corners", "label": "Corners"},
    {"id": "cards", "label": "Bookings"},
    {"id": "fouls", "label": "Fouls"},
    {"id": "shots", "label": "Shots / xG"},
    {"id": "throws", "label": "Throw-ins"},
    {"id": "tackles", "label": "Tackles"},
    {"id": "other", "label": "Other"},
]

# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

_print_lock = threading.Lock()
_throttle_lock = threading.Lock()
_last_call = [0.0]
MIN_GAP = float(os.environ.get("HOTSTREAKS_MIN_GAP", "0.06"))   # polite request spacing


def log(msg: str) -> None:
    with _print_lock:
        print(f"[{datetime.now(WAT).strftime('%H:%M:%S')}] {msg}", flush=True)


def _throttle() -> None:
    with _throttle_lock:
        gap = time.time() - _last_call[0]
        if gap < MIN_GAP:
            time.sleep(MIN_GAP - gap)
        _last_call[0] = time.time()


def http_json(url: str, tries: int = 4, timeout: int = 25):
    """GET JSON with retries/backoff. Returns None on 404 or after giving up."""
    backoff = 1.5
    for attempt in range(tries):
        _throttle()
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (429, 500, 502, 503, 504, 520, 522, 524):
                time.sleep(backoff)
                backoff *= 2
                continue
            log(f"  HTTP {e.code} on {url}")
            return None
        except Exception as e:                    # timeouts, resets, bad JSON
            if attempt == tries - 1:
                log(f"  fetch failed ({type(e).__name__}) {url}")
                return None
            time.sleep(backoff)
            backoff *= 2
    return None


def num(value):
    """'385 (83%)' / '1.88' / 12 / None -> int | float | None"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    m = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", ""))
    if not m:
        return None
    f = float(m.group(0))
    return int(f) if f.is_integer() else f


# --------------------------------------------------------------------------
# FotMob parsing
# --------------------------------------------------------------------------

STAT_KEYS = {
    "Ball possession": "poss",
    "Expected goals (xG)": "xg",
    "Total shots": "shots",
    "Shots on target": "sot",
    "Shots off target": "soff",
    "Blocked shots": "blocked",
    "Corners": "corners",
    "Yellow cards": "yellows",
    "Red cards": "reds",
    "Fouls committed": "fouls",
    "Offsides": "offsides",
    "Throws": "throws",
    "Tackles": "tackles",
    "Interceptions": "ints",
    "Keeper saves": "saves",
}


def to_wat_date(utc_iso: str) -> str:
    if not utc_iso:
        return ""
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
    except ValueError:
        return utc_iso[:10]
    return dt.astimezone(WAT).strftime("%Y-%m-%d")


def team_obj(t: dict) -> dict:
    name = t.get("name") or t.get("longName") or ""
    return {
        "id": t.get("id"),
        "n": name,
        "s": t.get("shortName") or name,
        "l": t.get("longName") or name,
    }


def parse_day(date_str: str) -> dict:
    return http_json(f"{BASE}/api/data/matches?date={date_str.replace('-', '')}") or {}


def tree_fixtures(payload: dict, only_ids: set | None = None) -> list[dict]:
    """Normalise every match in a date-tree payload."""
    out: list[dict] = []
    for lg in payload.get("leagues") or []:
        pid = lg.get("primaryId") if lg.get("primaryId") is not None else lg.get("id")
        if only_ids is not None and pid not in only_ids:
            continue
        if pid in ID_MAP:
            label, country = ID_MAP[pid]
        elif pid in EXTRA_FIXTURE_IDS:
            label, country = EXTRA_FIXTURE_IDS[pid], None
        else:
            label, country = (lg.get("name") or ""), None
        for m in lg.get("matches") or []:
            st = m.get("status") or {}
            if st.get("cancelled") or st.get("awarded"):
                continue
            home, away = m.get("home") or {}, m.get("away") or {}
            hs, as_ = home.get("score"), away.get("score")
            finished = bool(st.get("finished")) and hs is not None and as_ is not None
            utc = st.get("utcTime") or ""
            out.append({
                "id": m.get("id"),
                "lg": label,
                "ctry": country,
                "utc": utc,
                "date": to_wat_date(utc),
                "h": team_obj(home),
                "a": team_obj(away),
                "sc": [hs, as_] if finished else None,
                "fin": finished,
            })
    return out


def parse_details(match_id: int) -> dict | None:
    """Match stats -> {'ht': [h,a]|None, 'stt': {'h': {...}, 'a': {...}}}"""
    d = http_json(f"{BASE}/api/data/matchDetails?matchId={match_id}")
    if not d:
        return None
    content = d.get("content") or {}
    groups = (((content.get("stats") or {}).get("Periods") or {}).get("All") or {}).get("stats") or []
    if not groups:
        return None

    flat: dict[str, list] = {}
    for g in groups:
        for s in g.get("stats") or []:
            key = STAT_KEYS.get(s.get("title") or "")
            vals = s.get("stats")
            if key and isinstance(vals, list) and len(vals) == 2:
                flat[key] = vals

    def side(k: str, idx: int):
        v = flat.get(k)
        if not v:
            return None
        got = num(v[idx])
        if k == "poss" and isinstance(got, float):
            got = round(got, 1)
        return got

    h_stats = {k: side(k, 0) for k in STAT_KEYS.values()}
    a_stats = {k: side(k, 1) for k in STAT_KEYS.values()}

    # half-time score = goals before the 'Half' marker in the event feed
    ht = None
    events = ((content.get("matchFacts") or {}).get("events") or {}).get("events") or []
    if events:
        hg = ag = 0
        saw_half = False
        for e in events:
            if e.get("type") == "Half":
                saw_half = True
                break
            if e.get("type") == "Goal":
                if e.get("isHome") is True:
                    hg += 1
                elif e.get("isHome") is False:
                    ag += 1
        if saw_half:
            ht = [hg, ag]

    if ht is None and all(v is None for v in h_stats.values()):
        return None
    return {"ht": ht, "stt": {"h": h_stats, "a": a_stats}}


def parse_venue(match_id: int) -> str | None:
    d = http_json(f"{BASE}/api/data/matchDetails?matchId={match_id}", tries=2)
    if not d:
        return None
    box = ((d.get("content") or {}).get("matchFacts") or {}).get("infoBox") or {}
    station = box.get("Stadium") or {}
    return (station.get("name") or "").strip() or None


def season_of(date_str: str) -> str:
    y, m = int(date_str[:4]), int(date_str[5:7])
    return f"{y}-{str(y + 1)[-2:]}" if m >= 7 else f"{y - 1}-{str(y)[-2:]}"


# --------------------------------------------------------------------------
# History store — one JSON object per match, one file per season
# --------------------------------------------------------------------------

def load_history() -> dict[int, dict]:
    matches: dict[int, dict] = {}
    if not os.path.isdir(HIST):
        return matches
    for fn in sorted(os.listdir(HIST)):
        if not fn.endswith(".jsonl"):
            continue
        with open(os.path.join(HIST, fn), encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                matches[rec["id"]] = rec        # a later line supersedes an update
    return matches


def append_history(records: list[dict]) -> int:
    if not records:
        return 0
    os.makedirs(HIST, exist_ok=True)
    buckets: dict[str, list[dict]] = {}
    for r in records:
        buckets.setdefault(season_of(r["date"]), []).append(r)
    written = 0
    for season, recs in buckets.items():
        recs.sort(key=lambda r: (r["date"], r["id"]))
        with open(os.path.join(HIST, f"{season}.jsonl"), "a", encoding="utf-8") as fh:
            for r in recs:
                fh.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
                written += 1
    return written


def load_state() -> dict:
    if os.path.exists(STATE_JSON):
        try:
            with open(STATE_JSON, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return {}


def save_state(state: dict) -> None:
    os.makedirs(DATA, exist_ok=True)
    with open(STATE_JSON, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=1, sort_keys=True)
        fh.write("\n")


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------

def make_record(f: dict, det: dict | None) -> dict:
    det = det or {"ht": None, "stt": None}
    return {
        "id": f["id"], "lg": f["lg"], "ctry": f["ctry"], "date": f["date"],
        "utc": f["utc"], "h": f["h"], "a": f["a"], "sc": f["sc"],
        "ht": det.get("ht"), "stt": det.get("stt"),
    }


def fetch_window(start: str, end: str, have: dict[int, dict], max_details: int,
                 deadline: float, sink=None) -> tuple[int, int]:
    """
    Fetch day trees for [start..end], enrich finished matches we don't have
    stats for, and hand each finished batch to `sink` (so a long run persists
    progress as it goes).  Returns (records written, still awaiting stats).
    """
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    days = [(d0 + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((d1 - d0).days + 1)]

    raw: list[dict] = []
    with ThreadPoolExecutor(max_workers=WORKERS_TREE) as ex:
        for res in ex.map(lambda dd: tree_fixtures(parse_day(dd), TRACKED_IDS), days):
            raw.extend(res)

    seen: set[int] = set()
    finished: list[dict] = []
    for f in raw:                                  # de-dupe (late kickoffs can repeat)
        if f["id"] in seen or not f["fin"]:
            continue
        seen.add(f["id"])
        finished.append(f)
    if not finished:
        return 0, 0

    needs = [f for f in finished if not (have.get(f["id"]) or {}).get("stt")]
    fresh_results = [f for f in finished if f["id"] not in have]      # result-only, no stats yet
    budget = max(0, min(max_details, len(needs)))
    todo, deferred = needs[:budget], needs[budget:]
    log(f"  {len(finished)} finished: {len(finished) - len(needs)} cached · "
        f"{len(todo)} to enrich · {len(deferred)} deferred")
    if not todo and not fresh_results:
        return 0, len(deferred)

    written = 0
    for i in range(0, len(todo), 400):
        batch = todo[i:i + 400]

        def work(f):
            if time.time() > deadline:
                return None
            return (f["id"], parse_details(f["id"]))

        details: dict[int, dict] = {}
        with ThreadPoolExecutor(max_workers=WORKERS_DETAIL) as ex:
            for res in ex.map(work, batch):
                if res and res[1]:
                    details[res[0]] = res[1]
        recs = [make_record(f, details.get(f["id"])) for f in batch]
        recs += [make_record(f, None) for f in fresh_results if f["id"] in
                 {b["id"] for b in batch} and f["id"] not in details]
        for r in recs:                              # remember so later batches skip
            have[r["id"]] = r
        if sink and recs:
            written += sink(recs)
        log(f"    batch {i // 400 + 1}: +{len(recs)} stored ({written} this window)")

    # results for matches we chose not to enrich (over budget / past deadline)
    leftovers = [make_record(f, None) for f in deferred
                 if (f["id"] not in have)]
    if leftovers and sink:
        for r in leftovers:
            have[r["id"]] = r
        written += sink(leftovers)
        log(f"    +{len(leftovers)} result-only record(s) pending stats")
    return written, len(deferred)


def fetch_forward(days: int = FIXTURE_DAYS) -> list[dict]:
    """Every match (any competition) in the next `days` days."""
    today = datetime.now(WAT).date()
    window = [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days + 1)]
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=WORKERS_TREE) as ex:
        for res in ex.map(lambda dd: tree_fixtures(parse_day(dd), None), window):
            out.extend(res)
    return out


# --------------------------------------------------------------------------
# Streak engine
# --------------------------------------------------------------------------

def build_rows(matches: dict[int, dict]) -> dict[int, dict]:
    """team_id -> {name, league, country, rows[...]} in chronological order."""
    teams: dict[int, dict] = {}
    for m in sorted(matches.values(), key=lambda x: (x.get("date") or "", x["id"])):
        if not m.get("sc"):
            continue
        sc, stt, ht = m["sc"], m.get("stt") or {}, m.get("ht")
        for side in ("h", "a"):
            other = "a" if side == "h" else "h"
            me, opp = m[side], m[other]
            tid = me.get("id")
            if tid is None:
                continue
            gf, ga = (sc[0], sc[1]) if side == "h" else (sc[1], sc[0])
            hs, as_ = stt.get("h") or {}, stt.get("a") or {}
            t = {k: (hs.get(k) if side == "h" else as_.get(k)) for k in STAT_KEYS.values()}
            o = {k: (as_.get(k) if side == "h" else hs.get(k)) for k in STAT_KEYS.values()}
            both = {k: (None if (t[k] is None or o[k] is None) else t[k] + o[k])
                    for k in STAT_KEYS.values()}
            if ht and len(ht) == 2:
                ht_t, ht_o = (ht[0], ht[1]) if side == "h" else (ht[1], ht[0])
            else:
                ht_t = ht_o = None
            t["cards"] = None if t["yellows"] is None else (t["yellows"] or 0) + (t["reds"] or 0)
            o["cards"] = None if o["yellows"] is None else (o["yellows"] or 0) + (o["reds"] or 0)
            both["cards"] = (None if (t["cards"] is None or o["cards"] is None)
                             else t["cards"] + o["cards"])
            row = {
                "date": m.get("date") or "", "opp": opp.get("n") or "",
                "home": side == "h", "gf": gf, "ga": ga,
                "res": "W" if gf > ga else ("L" if gf < ga else "D"),
                "score": f"{gf}-{ga}", "total": gf + ga,
                "ht_total": (None if ht_t is None else ht_t + ht_o),
                "ht_score": (None if ht_t is None else f"{ht_t}-{ht_o}"),
                "t": t, "o": o, "m": both,
                "has_stats": any(v is not None for v in t.values()),
                "lg": m.get("lg"), "ctry": m.get("ctry"),
            }
            slot = teams.get(tid)
            if slot is None:
                slot = teams[tid] = {"id": tid, "name": me.get("n") or "",
                                     "short": me.get("s") or "", "league": m.get("lg"),
                                     "country": m.get("ctry"), "rows": []}
            slot["rows"].append(row)
            slot["name"] = me.get("n") or slot["name"]
            slot["short"] = me.get("s") or slot["short"]
            slot["league"], slot["country"] = m.get("lg"), m.get("ctry")

    for slot in teams.values():
        slot["rows"].sort(key=lambda r: (r["date"], 0 if r["home"] else 1))
    return teams


# ---- stat string formatters (match the conventions of the live dashboard) --

def f_score(r, _t):         return r["score"]
def f_goals(r, _t):         return f"{r['total']} goals"
def f_ht(r, _t):            return f"HT {r['ht_score']}" if r["ht_score"] else "HT —"
def f_corners(r, _t):
    v = r["m"]["corners"]
    return f"{v} corners" + (f" ({r['t']['corners']}-{r['o']['corners']})"
                             if r["t"]["corners"] is not None and r["o"]["corners"] is not None else "")
def f_corners_plain(r, _t): return f"{r['m']['corners']} corners"
def f_team_corners(r, _t):  return f"{r['t']['corners']} team corners"
def f_cards(r, _t):
    y, rd = r["m"]["yellows"], r["m"]["reds"]
    extra = f" ({y}+{rd} Y)" if (y is not None and rd) else ""
    return f"{r['m']['cards']} cards{extra}"
def f_cards_plain(r, _t):   return f"{r['m']['cards']} cards"
def f_team_cards(r, _t):    return f"{r['t']['cards']} team cards"
def f_yellows(r, _t):       return f"{r['m']['yellows']} yellows"
def f_fouls(r, _t):
    v = r["m"]["fouls"]
    return f"{v} fouls" + (f" ({r['t']['fouls']}-{r['o']['fouls']})"
                           if r["t"]["fouls"] is not None and r["o"]["fouls"] is not None else "")
def f_fouls_plain(r, _t):   return f"{r['m']['fouls']} fouls"
def f_team_fouls(r, _t):    return f"{r['t']['fouls']} fouls"
def f_shots(r, _t):         return f"{r['t']['shots']} shots"
def f_m_shots(r, _t):       return f"{r['m']['shots']} shots"
def f_sot(r, _t):           return f"{r['t']['sot']} on target"
def f_xg(r, _t):
    v = r["t"]["xg"]
    return f"{v:.2f} xG" if isinstance(v, (int, float)) else "—"
def f_m_xg(r, _t):
    v = r["m"]["xg"]
    return f"{v:.2f} xG" if isinstance(v, (int, float)) else "—"
def f_throws(r, _t):
    v = r["m"]["throws"]
    return f"{v} throws" + (f" ({r['t']['throws']}-{r['o']['throws']})"
                            if r["t"]["throws"] is not None and r["o"]["throws"] is not None else "")
def f_throws_plain(r, _t):  return f"{r['m']['throws']} throws"
def f_team_throws(r, _t):   return f"{r['t']['throws']} throw-ins"
def f_tackles(r, _t):
    v = r["m"]["tackles"]
    return f"{v} tackles" + (f" ({r['t']['tackles']}-{r['o']['tackles']})"
                             if r["t"]["tackles"] is not None and r["o"]["tackles"] is not None else "")
def f_tackles_plain(r, _t): return f"{r['m']['tackles']} tackles"
def f_team_tackles(r, _t):  return f"{r['t']['tackles']} tackles"
def f_offsides(r, _t):      return f"{r['m']['offsides']} offsides"
def f_saves(r, _t):         return f"{r['t']['saves']} saves"
def f_ints(r, _t):          return f"{r['t']['ints']} interceptions"
def f_poss(r, _t):
    v = r["t"]["poss"]
    return f"{int(round(v))}% poss" if isinstance(v, (int, float)) else "—"


# ---- declarative thresholds: every "over/under" run is built from one path --

def line_type(tid, label, market, polarity, key, line, op=">", team=False,
              scope="any", fmt=None):
    src = "t" if team else "m"
    if fmt is None:
        fmt = {
            ("corners", False): f_corners_plain, ("corners", True): f_team_corners,
            ("cards", False): f_cards_plain,     ("cards", True): f_team_cards,
            ("fouls", False): f_fouls_plain,     ("fouls", True): f_team_fouls,
            ("shots", False): f_m_shots,         ("shots", True): f_shots,
            ("throws", False): f_throws_plain,   ("throws", True): f_team_throws,
            ("tackles", False): f_tackles_plain, ("tackles", True): f_team_tackles,
            ("other", False): f_offsides,        ("other", True): f_ints,
        }.get((market, team), f_shots)

    def pred(r, src=src, key=key, line=line, op=op):
        v = r[src].get(key)
        if v is None:
            return False
        return v > line if op == ">" else v < line

    def missing(r, src=src, key=key):
        return r[src].get(key) is None

    return dict(id=tid, label=label, market=market, polarity=polarity, scope=scope,
                pred=pred, fmt=fmt, missing=missing, kind="line", src=src, key=key)


def result_type(tid, label, market, polarity, pred, fmt=f_score, scope="any", kind="score"):
    def missing(r):
        return False
    return dict(id=tid, label=label, market=market, polarity=polarity, scope=scope,
                pred=pred, fmt=fmt, missing=missing, kind=kind, src="score", key=None)


TYPES: list[dict] = [
    # ---- goals / 1X2 ------------------------------------------------------
    result_type("wins", "Straight wins", "goals", "hot", lambda r: r["res"] == "W"),
    result_type("home_wins", "Straight home wins", "goals", "hot", lambda r: r["res"] == "W", scope="home"),
    result_type("away_wins", "Straight away wins", "goals", "hot", lambda r: r["res"] == "W", scope="away"),
    result_type("unbeaten", "Unbeaten run", "goals", "hot", lambda r: r["res"] != "L"),
    result_type("home_unbeaten", "Home unbeaten", "goals", "hot", lambda r: r["res"] != "L", scope="home"),
    result_type("away_unbeaten", "Away unbeaten", "goals", "hot", lambda r: r["res"] != "L", scope="away"),
    result_type("losses", "Straight losses", "goals", "cold", lambda r: r["res"] == "L"),
    result_type("home_losses", "Straight home losses", "goals", "cold", lambda r: r["res"] == "L", scope="home"),
    result_type("away_losses", "Straight away losses", "goals", "cold", lambda r: r["res"] == "L", scope="away"),
    result_type("winless", "Winless run", "goals", "cold", lambda r: r["res"] != "W"),
    result_type("scoring", "Scoring run", "goals", "spicy", lambda r: r["gf"] > 0),
    result_type("failed_score", "Blank run", "goals", "cold", lambda r: r["gf"] == 0),
    result_type("clean_sheets", "Clean sheets", "goals", "hot", lambda r: r["ga"] == 0),
    result_type("btts", "BTTS run", "goals", "spicy", lambda r: r["gf"] > 0 and r["ga"] > 0),
    result_type("no_draw", "No-draw run", "goals", "spicy", lambda r: r["res"] != "D"),
    result_type("over25", "Over 2.5 goals", "goals", "spicy", lambda r: r["total"] > 2.5, fmt=f_goals),
    result_type("under25", "Under 2.5 goals", "goals", "spicy", lambda r: r["total"] < 2.5, fmt=f_goals),
    result_type("over15", "Over 1.5 goals", "goals", "spicy", lambda r: r["total"] > 1.5, fmt=f_goals),
    result_type("over35", "Over 3.5 goals", "goals", "spicy", lambda r: r["total"] > 3.5, fmt=f_goals),
    result_type("ht_over05", "HT over 0.5", "goals", "spicy",
                lambda r: r["ht_total"] is not None and r["ht_total"] > 0.5, fmt=f_ht,
                kind="ht"),
    result_type("ht_over15", "HT over 1.5", "goals", "spicy",
                lambda r: r["ht_total"] is not None and r["ht_total"] > 1.5, fmt=f_ht,
                kind="ht"),
    # ---- corners ----------------------------------------------------------
    line_type("corners_o85", "Over 8.5 corners", "corners", "spicy", "corners", 8.5, fmt=f_corners),
    line_type("corners_o95", "Over 9.5 corners", "corners", "spicy", "corners", 9.5, fmt=f_corners),
    line_type("corners_o105", "Over 10.5 corners", "corners", "spicy", "corners", 10.5),
    line_type("corners_u95", "Under 9.5 corners", "corners", "spicy", "corners", 9.5, op="<"),
    line_type("corners_u85", "Under 8.5 corners", "corners", "spicy", "corners", 8.5, op="<"),
    line_type("team_corners_o45", "Team over 4.5 corners", "corners", "hot", "corners", 4.5, team=True),
    line_type("team_corners_o55", "Team over 5.5 corners", "corners", "hot", "corners", 5.5, team=True),
    line_type("team_corners_o65", "Team over 6.5 corners", "corners", "hot", "corners", 6.5, team=True),
    line_type("team_corners_u45", "Team under 4.5 corners", "corners", "cold", "corners", 4.5, op="<", team=True),
    line_type("home_corners_o55", "Home over 5.5 corners", "corners", "hot", "corners", 5.5,
              team=True, scope="home", fmt=f_corners_plain),
    line_type("away_corners_o45", "Away over 4.5 corners", "corners", "hot", "corners", 4.5,
              team=True, scope="away", fmt=f_corners_plain),
    # ---- cards ------------------------------------------------------------
    line_type("cards_o35", "Over 3.5 cards", "cards", "spicy", "cards", 3.5, fmt=f_cards),
    line_type("cards_o45", "Over 4.5 cards", "cards", "spicy", "cards", 4.5),
    line_type("cards_o55", "Over 5.5 cards", "cards", "spicy", "cards", 5.5),
    line_type("cards_u35", "Under 3.5 cards", "cards", "spicy", "cards", 3.5, op="<"),
    line_type("cards_u45", "Under 4.5 cards", "cards", "spicy", "cards", 4.5, op="<"),
    line_type("team_cards_o15", "Team over 1.5 cards", "cards", "cold", "cards", 1.5, team=True),
    line_type("team_cards_o25", "Team over 2.5 cards", "cards", "cold", "cards", 2.5, team=True),
    line_type("team_cards_u15", "Team under 1.5 cards", "cards", "hot", "cards", 1.5, op="<", team=True),
    line_type("yellows_o35", "Over 3.5 yellows", "cards", "spicy", "yellows", 3.5, fmt=f_yellows),
    # ---- fouls ------------------------------------------------------------
    line_type("fouls_o205", "Over 20.5 fouls", "fouls", "spicy", "fouls", 20.5, fmt=f_fouls),
    line_type("fouls_o225", "Over 22.5 fouls", "fouls", "spicy", "fouls", 22.5),
    line_type("fouls_o245", "Over 24.5 fouls", "fouls", "spicy", "fouls", 24.5),
    line_type("fouls_u205", "Under 20.5 fouls", "fouls", "spicy", "fouls", 20.5, op="<"),
    line_type("fouls_u225", "Under 22.5 fouls", "fouls", "spicy", "fouls", 22.5, op="<"),
    line_type("team_fouls_o105", "Team over 10.5 fouls", "fouls", "cold", "fouls", 10.5, team=True),
    line_type("team_fouls_o125", "Team over 12.5 fouls", "fouls", "cold", "fouls", 12.5, team=True),
    line_type("team_fouls_u95", "Team under 9.5 fouls", "fouls", "hot", "fouls", 9.5, op="<", team=True),
    line_type("team_fouls_u105", "Team under 10.5 fouls", "fouls", "hot", "fouls", 10.5, op="<", team=True),
    # ---- shots / xG -------------------------------------------------------
    line_type("team_shots_o105", "Team over 10.5 shots", "shots", "hot", "shots", 10.5, team=True),
    line_type("team_shots_o125", "Team over 12.5 shots", "shots", "hot", "shots", 12.5, team=True),
    line_type("team_shots_o145", "Team over 14.5 shots", "shots", "hot", "shots", 14.5, team=True),
    line_type("team_shots_u85", "Team under 8.5 shots", "shots", "cold", "shots", 8.5, op="<", team=True),
    line_type("team_sot_o45", "Team over 4.5 SOT", "shots", "hot", "sot", 4.5, team=True, fmt=f_sot),
    line_type("team_sot_o35", "Team over 3.5 SOT", "shots", "hot", "sot", 3.5, team=True, fmt=f_sot),
    line_type("match_shots_o235", "Over 23.5 shots", "shots", "spicy", "shots", 23.5),
    line_type("team_xg_o15", "Team over 1.5 xG", "shots", "hot", "xg", 1.5, team=True, fmt=f_xg),
    line_type("match_xg_o25", "Over 2.5 match xG", "shots", "spicy", "xg", 2.5, fmt=f_m_xg),
    # ---- throw-ins --------------------------------------------------------
    line_type("throws_o355", "Over 35.5 throw-ins", "throws", "spicy", "throws", 35.5, fmt=f_throws),
    line_type("throws_o395", "Over 39.5 throw-ins", "throws", "spicy", "throws", 39.5),
    line_type("throws_u355", "Under 35.5 throw-ins", "throws", "spicy", "throws", 35.5, op="<"),
    line_type("team_throws_o185", "Team over 18.5 throw-ins", "throws", "hot", "throws", 18.5, team=True),
    line_type("team_throws_o205", "Team over 20.5 throw-ins", "throws", "hot", "throws", 20.5, team=True),
    line_type("team_throws_u165", "Team under 16.5 throw-ins", "throws", "cold", "throws", 16.5, op="<", team=True),
    # ---- tackles ----------------------------------------------------------
    line_type("tackles_o285", "Over 28.5 tackles", "tackles", "spicy", "tackles", 28.5, fmt=f_tackles),
    line_type("tackles_o325", "Over 32.5 tackles", "tackles", "spicy", "tackles", 32.5),
    line_type("tackles_u285", "Under 28.5 tackles", "tackles", "spicy", "tackles", 28.5, op="<"),
    line_type("team_tackles_o155", "Team over 15.5 tackles", "tackles", "hot", "tackles", 15.5, team=True),
    line_type("team_tackles_o175", "Team over 17.5 tackles", "tackles", "hot", "tackles", 17.5, team=True),
    line_type("team_tackles_u145", "Team under 14.5 tackles", "tackles", "cold", "tackles", 14.5,
              op="<", team=True),
    # ---- other ------------------------------------------------------------
    line_type("offsides_o25", "Over 2.5 offsides", "other", "spicy", "offsides", 2.5, fmt=f_offsides),
    line_type("offsides_o35", "Over 3.5 offsides", "other", "spicy", "offsides", 3.5, fmt=f_offsides),
    line_type("saves_o25", "Keeper over 2.5 saves", "other", "hot", "saves", 2.5, team=True, fmt=f_saves),
    line_type("saves_o35", "Keeper over 3.5 saves", "other", "hot", "saves", 3.5, team=True, fmt=f_saves),
    line_type("ints_o95", "Team over 9.5 interceptions", "other", "hot", "ints", 9.5, team=True, fmt=f_ints),
    line_type("poss_o55", "Possession over 55%", "other", "hot", "poss", 55, team=True, fmt=f_poss),
    line_type("poss_u45", "Possession under 45%", "other", "cold", "poss", 45, op="<", team=True,
              fmt=f_poss),
]

assert len(TYPES) == 78, f"expected 78 run types, built {len(TYPES)}"


def row_missing(row: dict, t: dict) -> bool:
    """True when this match can't be judged for this type (stat absent)."""
    if t["kind"] == "ht":
        return row["ht_total"] is None
    return t["missing"](row)


def build_streaks(matches: dict[int, dict], fixtures_by_team: dict[int, list[dict]]) -> list[dict]:
    teams = build_rows(matches)
    today = datetime.now(WAT).strftime("%Y-%m-%d")
    suffixes = load_venue_cache()
    out: list[dict] = []

    for tid, slot in teams.items():
        rows = slot["rows"]
        if not rows:
            continue
        form = "".join(r["res"] for r in rows[-FORM_N:][::-1])
        nxt = next_fixture(fixtures_by_team.get(tid, []), tid, today, suffixes)
        for t in TYPES:
            scope = t.get("scope", "any")
            length = 0
            run: list[dict] = []
            for r in reversed(rows):
                if scope == "home" and not r["home"]:
                    continue
                if scope == "away" and r["home"]:
                    continue
                if row_missing(r, t):
                    if t["kind"] == "score" or r["has_stats"]:
                        break                     # stat tracked but missing -> run ends
                    continue                      # no stats for this match at all: ignore it
                if t["pred"](r):
                    length += 1
                    if len(run) < RECENT_N:
                        run.append(r)
                else:
                    break
            if length < MIN_STREAK:
                continue
            out.append({
                "id": f"{slug(slot['name'])}-{t['id']}",
                "team": slot["name"],
                "teamShort": short_code(slot["name"], slot["short"]),
                "league": slot["league"],
                "country": slot["country"],
                "type": t["id"],
                "typeLabel": t["label"],
                "market": t["market"],
                "length": length,
                "polarity": t["polarity"],
                "form": form,
                "recent": [{
                    "date": r["date"], "opp": r["opp"], "home": r["home"],
                    "score": r["score"], "result": r["res"],
                    "stat": safe_fmt(t["fmt"], r),
                } for r in run],
                "next": nxt,
                "matches": [],
            })
    out.sort(key=lambda s: (-s["length"], s["team"], s["type"]))
    return out


def safe_fmt(fmt, row) -> str:
    try:
        v = fmt(row, None)
        return str(v) if v is not None else ""
    except Exception:
        return ""


def next_fixture(entries: list[dict], team_id: int, today: str, venues: dict) -> dict | None:
    """Earliest not-yet-played match for a team."""
    now = datetime.now(timezone.utc)
    best = None
    for f in entries:
        if f.get("fin") or f.get("sc"):
            continue
        utc = f.get("utc") or ""
        try:
            when = datetime.fromisoformat(utc.replace("Z", "+00:00"))
        except ValueError:
            continue
        if when < now - timedelta(hours=4):
            continue
        if best is None or when < best[0]:
            best = (when, f)
    if best is None:
        return None
    when, f = best
    home, away = f["h"], f["a"]
    is_home = home.get("id") == team_id
    local = when.astimezone(WAT)
    return {
        "date": f.get("date") or local.strftime("%Y-%m-%d"),
        "kickoffLabel": f"{local.strftime('%A %b')} {local.day} · {local.strftime('%H:%M')}",
        "opponent": (away if is_home else home)["n"],
        "home": is_home,
        "venue": venues.get(str(f.get("id")), ""),
        "comp": f.get("lg") or "",
        "homeTeam": home.get("l") or home["n"],
        "awayTeam": away.get("l") or away["n"],
        "headline": f"{home.get('l') or home['n']} vs {away.get('l') or away['n']}",
        "today": (f.get("date") or "") == today,
    }


def load_venue_cache() -> dict:
    if os.path.exists(VENUE_CACHE):
        try:
            with open(VENUE_CACHE, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return {}


def enrich_venues(pairs: list[tuple[int, str]], budget: int, deadline: float) -> None:
    """Fill data/venue_cache.json with stadium names (best effort, cached)."""
    cache = load_venue_cache()
    todo = [(mid, when) for mid, when in sorted(pairs, key=lambda p: p[1])
            if str(mid) not in cache][:budget]
    if not todo:
        return
    log(f"  venues: fetching {len(todo)} stadium name(s)")
    with ThreadPoolExecutor(max_workers=WORKERS_DETAIL) as ex:
        for (mid, _when), name in zip(todo, ex.map(
                lambda p: parse_venue(p[0]) if time.time() < deadline else None, todo)):
            cache[str(mid)] = name or ""
    with open(VENUE_CACHE, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, ensure_ascii=False, sort_keys=True)
        fh.write("\n")


def slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", name.lower().replace("&", "and"))
    return s.strip("-")


_STOP = {"fc", "cf", "ac", "as", "sc", "sv", "sk", "fk", "nk", "bk", "if", "ik", "cd", "ca",
         "cs", "sd", "se", "ec", "ad", "ud", "rc", "us", "ss", "afc", "kk", "kv", "krc", "kaa",
         "csd", "rcd", "cfc", "sv", "tsv", "vfb", "vfl", "tsg", "bsc"}


def short_code(name: str, fallback: str = "") -> str:
    """'Bayern Munich' -> BM, 'Cordoba' -> COR, 'FC Cincinnati' -> CIN."""
    toks = [t for t in re.split(r"[\s\-]+", name.strip()) if t]
    while len(toks) > 1 and toks[0].lower() in _STOP:
        toks = toks[1:]
    if not toks:
        toks = [name]
    if len(toks) == 1:
        letters = re.sub(r"[^A-Za-z]", "", toks[0])
        return (letters[:3] or fallback[:3] or name[:3]).upper()
    initials = "".join(re.sub(r"[^A-Za-z0-9']", "", tok)[:1] for tok in toks if tok)
    return (initials[:3] or fallback[:3]).upper()


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Hot Streaks pipeline")
    ap.add_argument("--mode", choices=["full", "daily", "backfill", "rebuild"], default="full")
    ap.add_argument("--days", type=int, default=4, help="days of recent results to refresh")
    ap.add_argument("--chunk-days", type=int, default=21, help="backfill chunk size")
    ap.add_argument("--max-details", type=int, default=6000, help="stats fetches per run")
    ap.add_argument("--max-minutes", type=float, default=90.0, help="wall-clock budget")
    ap.add_argument("--venue-budget", type=int, default=60,
                    help="stadium lookups per run (only fixtures within 5 days)")
    ap.add_argument("--horizon", default=DEFAULT_HORIZON, help="oldest date backfill may reach")
    args = ap.parse_args()

    t0 = time.time()
    deadline = t0 + args.max_minutes * 60
    today = datetime.now(WAT).date()
    os.makedirs(DATA, exist_ok=True)      # fresh clones have no data/ yet
    log(f"HotStreaks pipeline — mode={args.mode}, root={ROOT}")

    have = load_history()
    log(f"history on file: {len(have)} matches")
    state = load_state()
    added = 0

    if args.mode != "rebuild":
        start = (today - timedelta(days=args.days)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        log(f"refresh {start} .. {end}")
        written, pending = fetch_window(start, end, have, args.max_details, deadline,
                                        sink=append_history)
        added += written
        log(f"  refresh stored {written} record(s); {pending} awaiting stats")
        state["pendingStats"] = pending

        if args.mode in ("full", "backfill"):
            # Keep walking backwards while the time and request budgets allow, so a
            # single nightly run can deepen history substantially (and the loop
            # simply stops once the horizon is reached).
            cursor = state.get("cursor") or start
            for _ in range(120):
                if (cursor <= args.horizon or time.time() > deadline
                        or added >= args.max_details):
                    break
                c_end = datetime.strptime(cursor, "%Y-%m-%d").date() - timedelta(days=1)
                c_start = max(c_end - timedelta(days=args.chunk_days - 1),
                              datetime.strptime(args.horizon, "%Y-%m-%d").date())
                if c_end < c_start:
                    break
                log(f"backfill {c_start} .. {c_end} (cursor was {cursor})")
                written, pending = fetch_window(c_start.strftime("%Y-%m-%d"),
                                                c_end.strftime("%Y-%m-%d"), load_history(),
                                                max(0, args.max_details - added), deadline,
                                                sink=append_history)
                added += written
                log(f"  stored {written} record(s)")
                cursor = c_start.strftime("%Y-%m-%d")
                state["cursor"] = cursor
                state["backfillDone"] = cursor <= args.horizon
                save_state(state)
        if "cursor" not in state:
            state["cursor"] = start
        state["lastRun"] = datetime.now(WAT).isoformat(timespec="seconds")
        save_state(state)

    # ---------------- rebuild ----------------
    have = load_history()
    log(f"building from {len(have)} matches")
    fixtures_by_team: dict[int, list[dict]] = {}
    if args.mode != "rebuild":
        fx = fetch_forward()
        team_ids = {t for m in have.values() for t in (m["h"].get("id"), m["a"].get("id")) if t}
        for f in fx:
            for side in ("h", "a"):
                if f[side].get("id") in team_ids:
                    fixtures_by_team.setdefault(f[side]["id"], []).append(f)
        log(f"  {len(fx)} upcoming fixtures, {len(fixtures_by_team)} teams have a next match")
        horizon_utc = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
        enrich_venues([(f["id"], f.get("utc") or "") for f in fx
                       if f.get("id") and f.get("utc") and f["utc"] < horizon_utc],
                      args.venue_budget, deadline)

    streaks = build_streaks(have, fixtures_by_team)
    as_of = max((m.get("date") or "" for m in have.values()), default=today.strftime("%Y-%m-%d"))
    teams_count = len({t for m in have.values() for t in (m["h"].get("id"), m["a"].get("id")) if t})
    payload = {
        "generatedAt": datetime.now(WAT).isoformat(timespec="seconds"),
        "asOf": as_of,
        "source": "FotMob (fotmob.com)",
        "eventCount": len(have),
        "teamCount": teams_count,
        "streakCount": len(streaks),
        "leagues": LEAGUE_LABELS,
        "types": [{k: t[k] for k in ("id", "label", "market", "polarity")} for t in TYPES],
        "markets": MARKETS,
        "streaks": streaks,
    }
    os.makedirs(DATA, exist_ok=True)
    tmp = OUT_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
        fh.write("\n")
    os.replace(tmp, OUT_JSON)
    log(f"wrote {OUT_JSON}: {len(streaks)} runs · {teams_count} teams · "
        f"{len(have)} matches · as of {as_of}")
    log(f"finished in {time.time() - t0:.0f}s ({added} new match records)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
