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
STATS_LOOKBACK = 10                     # the consistency section looks at the last 10 games
STATS_MIN_GAMES = 8                     # ...needs at least 8 of them played
STATS_MIN_HITS = 8                      # ...and the stat landed in at least 8
STATS_CAP = 900                         # entries shipped for football
STATS_PER_TEAM = 8                      # most entries one team may contribute
FORM_N = 5                              # form dots
FIXTURE_DAYS = 25                       # forward window for "next match"
                                        # (European matchdays are ~3 weeks apart)
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
    # European club competitions. League phase and qualifying rounds share a
    # stable primaryId per competition, so both are merged under one label.
    # They are tracked as their own competitions: European matches never bleed
    # into a team's domestic runs.
    ("UEFA Champions League",  "Europe",       [42, 10611]),
    ("UEFA Europa League",     "Europe",       [73, 10613]),
    ("UEFA Conference League", "Europe",       [10216, 10615]),
    ("Premier League",       "England",        [47]),
    ("Championship",         "England",        [48]),
    ("League One",           "England",        [108]),
    ("La Liga",              "Spain",          [87]),
    ("Segunda División",     "Spain",          [140]),
    ("Serie A",              "Italy",          [55]),
    ("Brasileirão",          "Brazil",         [268]),
    ("Serie B",              "Italy",          [86]),
    ("Bundesliga",           "Germany",        [54]),
    ("Austrian Bundesliga",  "Austria",        [38]),
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
    ("Super League",         "Switzerland",    [69]),
    ("Chinese Super League", "China",          [120]),
    ("MLS",                  "USA",            [130]),
    ("Liga MX",              "Mexico",         [230]),
    ("Liga Profesional",     "Argentina",      [112]),
    ("J1 League",            "Japan",          [223]),
    # Second wave of domestic leagues (Sept 2026). Labels are country-qualified
    # where FotMob's own name would collide with a league already tracked above
    # ("Premier League" x4, "Ligue 1", "Premiership"), because the dashboard keys
    # every competition by this label.
    ("Slovak Super Liga",         "Slovakia",               [176]),
    ("Prva Liga",                 "Slovenia",               [173]),
    ("Nemzeti Bajnokság I",       "Hungary",                [212]),
    ("First Professional League", "Bulgaria",               [270]),
    ("Ukrainian Premier League",  "Ukraine",                [441]),
    ("Russian Premier League",    "Russia",                 [63]),
    ("Eerste Divisie",            "Netherlands",            [111]),
    ("HNL",                       "Croatia",                [252]),
    ("Cypriot First Division",    "Cyprus",                 [136]),
    ("Belarusian Premier League", "Belarus",                [263]),
    ("Algerian Ligue 1",          "Algeria",                [516]),
    ("Egyptian Premier League",   "Egypt",                  [519]),
    ("Bosnian Premier League",    "Bosnia and Herzegovina", [267]),
    ("Cymru Premier",             "Wales",                  [116]),
    ("NIFL Premiership",          "Northern Ireland",       [129]),
    # Third wave (Sept 2026). Kosovo is not covered by FotMob at all - their
    # league index lists 94 countries and Kosovo is not one of them - so it is
    # the only requested league that could not be added.
    ("Serbian Super Liga",        "Serbia",                 [182]),
    ("Danish Superliga",          "Denmark",                [46]),
    ("Czech First League",        "Czechia",                [122]),
    ("Saudi Pro League",          "Saudi Arabia",           [536]),
    ("Israeli Premier League",    "Israel",                 [127]),
    ("Persian Gulf Pro League",   "Iran",                   [523]),
    ("Colombian Primera A",       "Colombia",               [274]),
    ("Premier Soccer League",     "South Africa",           [537]),
    ("UAE Pro League",            "United Arab Emirates",   [538]),
    ("Besta deildin",             "Iceland",                [215]),
    ("Erovnuli Liga",             "Georgia",                [439]),
    ("Kategoria Superiore",       "Albania",                [260]),
    ("Macedonian Prva Liga",      "North Macedonia",        [249]),
    ("Virsliga",                  "Latvia",                 [226]),
    ("Premium liiga",             "Estonia",                [248]),
    ("A Lyga",                    "Lithuania",              [228]),
    ("Iraqi Stars League",        "Iraq",                   [524]),
    ("Armenian Premier League",   "Armenia",                [118]),
    ("Faroese Premier League",    "Faroe Islands",          [250]),
]

LEAGUE_LABELS = [l for l, _c, _i in LEAGUES]
TRACKED_IDS = {i for _l, _c, ids in LEAGUES for i in ids}
ID_MAP = {i: (l, c) for l, c, ids in LEAGUES for i in ids}

# Competitions that never contribute history but may supply a "next match"
# (European nights, cups).  Anything else outside the 28 also shows up when a
# tracked team plays in it, labelled with FotMob's own competition name.
EXTRA_FIXTURE_IDS = {
    # Cosmetic labels for competitions that supply fixtures only (never history).
    # Anything else falls back to FotMob's own competition name.
    74: "UEFA Super Cup",
    45: "Copa Libertadores",
    299: "Copa Sudamericana",
    297: "CONCACAF Champions Cup",
    525: "AFC Champions League Elite",
}

LABEL_COUNTRY = {label: country for label, country, _ids in LEAGUES}

# order the consistency section prefers: the markets that matter most on a slip
# first, the stat markets SportyBet blocks on Flexi last
MARKET_RANK = {"goals": 0, "handicap": 1, "corners": 2, "cards": 3, "shots": 4,
               "fouls": 5, "throws": 6, "tackles": 7, "other": 8}

MARKETS = [
    {"id": "goals", "label": "Goals / 1X2"},
    {"id": "corners", "label": "Corners"},
    {"id": "cards", "label": "Bookings"},
    {"id": "fouls", "label": "Fouls"},
    {"id": "shots", "label": "Shots / xG"},
    {"id": "throws", "label": "Throw-ins"},
    {"id": "tackles", "label": "Tackles"},
    {"id": "handicap", "label": "Handicap"},
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
        comps: list[str] = []
        for r in slot["rows"]:
            if r["lg"] not in comps:
                comps.append(r["lg"])
        slot["competitions"] = comps
    return teams


# ---- stat string formatters (match the conventions of the live dashboard) --

def f_score(r, _t):         return r["score"]
def f_margin(r, _t):        return f"won by {r['gf'] - r['ga']}" if r["gf"] > r["ga"] else (
                                   f"lost by {r['ga'] - r['gf']}" if r["ga"] > r["gf"] else "drew")
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
    # ---- handicap: the margin, straight off the stored score --------------
    result_type("handicap_m15", "Covers -1.5 (wins by 2+)", "handicap", "hot",
                lambda r: (r["gf"] - r["ga"]) >= 2, fmt=f_margin),
    result_type("handicap_p15", "Covers +1.5 (not beaten by 2+)", "handicap", "hot",
                lambda r: (r["ga"] - r["gf"]) <= 1, fmt=f_margin),
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

assert len(TYPES) == 80, f"expected 80 run types, built {len(TYPES)}"


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
        nxt = next_fixture(fixtures_by_team.get(tid, []), tid, today, suffixes)

        by_competition: dict[str, list[dict]] = {}
        for r in rows:
            by_competition.setdefault(r["lg"], []).append(r)

        for league, comp_rows in by_competition.items():
            form = "".join(r["res"] for r in comp_rows[-FORM_N:][::-1])
            for t in TYPES:
                scope = t.get("scope", "any")
                length = 0
                run: list[dict] = []
                for r in reversed(comp_rows):
                    if scope == "home" and not r["home"]:
                        continue
                    if scope == "away" and r["home"]:
                        continue
                    if row_missing(r, t):
                        if t["kind"] == "score" or r["has_stats"]:
                            break                 # stat tracked but missing -> run ends
                        continue                  # no stats for this match at all: ignore it
                    if t["pred"](r):
                        length += 1
                        if len(run) < RECENT_N:
                            run.append(r)
                    else:
                        break
                if length < MIN_STREAK:
                    continue
                name = slot["name"]
                out.append({
                    "id": f"{slug(name)}-{slug(league)}-{t['id']}",
                    "teamId": tid,
                    "team": name,
                    "teamShort": short_code(name, slot["short"]),
                    "league": league,
                    "country": comp_rows[-1].get("ctry") or LABEL_COUNTRY.get(league),
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


def build_stats(teams: dict[int, dict], fixtures_by_team: dict[int, list[dict]],
                streak_keys: set, market_rank: dict[str, int]) -> list[dict]:
    """The consistency section: a stat that landed in 8+ of the last 10 games.

    A run in `build_streaks` is *consecutive* - three straight wins qualifies, a
    win-loss-win pattern does not. This is the other half of the picture: 8 wins
    in the last 10 games qualifies even if the last two were lost, and a stat that
    has landed 9 times in 10 qualifies however the sequence is ordered.

    `streak_keys` are the (team, competition, type) triples that also hold a live
    streak; those entries (and the matching streaks) carry a star in the UI.
    """
    today = datetime.now(WAT).strftime("%Y-%m-%d")
    suffixes = load_venue_cache()
    out: list[dict] = []

    for tid, slot in teams.items():
        rows = slot["rows"]
        if len(rows) < STATS_MIN_GAMES:
            continue
        nxt = next_fixture(fixtures_by_team.get(tid, []), tid, today, suffixes)
        by_competition: dict[str, list[dict]] = {}
        for r in rows:
            by_competition.setdefault(r["lg"], []).append(r)
        picked: list[dict] = []

        for league, comp_rows in by_competition.items():
            if len(comp_rows) < STATS_MIN_GAMES:
                continue
            form = "".join(r["res"] for r in comp_rows[-FORM_N:][::-1])
            for t in TYPES:
                scope = t.get("scope", "any")
                hits = seen = 0
                run: list[dict] = []          # newest first, one entry per game counted
                for r in reversed(comp_rows):
                    if scope == "home" and not r["home"]:
                        continue
                    if scope == "away" and r["home"]:
                        continue
                    if row_missing(r, t):
                        continue
                    seen += 1
                    hit = bool(t["pred"](r))
                    if hit:
                        hits += 1
                    if len(run) < STATS_LOOKBACK:
                        run.append((r, hit))
                    if seen >= STATS_LOOKBACK:
                        break
                if seen < STATS_MIN_GAMES or hits < STATS_MIN_HITS:
                    continue
                # the live streak of the same type, so the card can show how the
                # current sequence looks as well as the ten-game hit rate
                streak_len = 0
                for r in reversed(comp_rows):
                    if scope == "home" and not r["home"]:
                        continue
                    if scope == "away" and r["home"]:
                        continue
                    if row_missing(r, t):
                        break
                    if t["pred"](r):
                        streak_len += 1
                    else:
                        break
                name = slot["name"]
                picked.append({
                    "id": f"{slug(name)}-{slug(league)}-{t['id']}-stats",
                    "teamId": tid, "team": name,
                    "teamShort": short_code(name, slot["short"]),
                    "league": league,
                    "country": comp_rows[-1].get("ctry") or LABEL_COUNTRY.get(league),
                    "type": t["id"], "typeLabel": t["label"], "market": t["market"],
                    "polarity": t["polarity"],
                    "hits": hits, "games": seen,
                    "pct": round(hits * 100.0 / seen, 1),
                    "streak": streak_len,
                    "alsoStreak": (name, league, t["id"]) in streak_keys,
                    "form": form,
                    "recent": [{
                        "date": r["date"], "opp": r["opp"], "home": r["home"],
                        "score": r["score"], "result": r["res"], "hit": hit,
                        "stat": safe_fmt(t["fmt"], r),
                    } for r, hit in run],
                    "next": nxt,
                    "matches": [],
                })
        # keep each team's strongest entries, so no one team floods the section
        picked.sort(key=lambda e: (-e["hits"], -e["pct"], market_rank.get(e["market"], 9),
                                   e["typeLabel"]))
        out.extend(picked[:STATS_PER_TEAM])

    out.sort(key=lambda e: (-e["hits"], -e["pct"],
                            market_rank.get(e["market"], 9), e["team"]))
    # show both flavours: teams whose current run also counts as a streak (starred)
    # and teams that are consistent but whose run was just broken - the case the
    # streaks section cannot show
    starred = [e for e in out if e["alsoStreak"]]
    plain = [e for e in out if not e["alsoStreak"]]
    keep = starred[: int(STATS_CAP * 0.7)] + plain[: STATS_CAP - int(STATS_CAP * 0.7)]
    keep.sort(key=lambda e: (-e["hits"], -e["pct"], 0 if e["alsoStreak"] else 1,
                             market_rank.get(e["market"], 9), e["team"]))
    return keep


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
        "ts": int(when.timestamp()),                      # UTC epoch, for time windows
        "hours": round((when - now).total_seconds() / 3600.0, 2),
        # "Thu 17 Sep · 20:00" - same shape as the other sports and the tickets,
        # so a kickoff reads the same wherever it appears on the page
        "kickoffLabel": f"{local.strftime('%a %d %b')} · {local.strftime('%H:%M')}",
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



# ==========================================================================
# Analysis: model odds, power rankings, accumulators
# ==========================================================================
#
# No free source publishes live bookmaker prices for every market we track, so
# the engine prices each upcoming selection from its own measured base rate:
#
#   p_hat  = probability the run continues (shrunk towards the competition mean)
#   fair   = 1 / p_hat
#   market = fair * (1 - MARGIN)      <- what a book is likely to offer
#
# When a real price is available (see ODDS_API_KEY / odds_reference()) the
# measured market price is used instead of the estimate.

MARGIN = 0.055                 # typical bookmaker overround on these markets
P_MAX = 0.90                   # a model never "knows" a selection is certain:
                               # prices shorter than this would be fantasy
ODDS_FLOOR = 1.02              # no real book prices a tracked market below this
P_MIN = 0.02                   # nor is a tracked run ever a write-off: a competition
                               # whose base rate measured zero must not divide by zero
SHRINK_K = 6.0                 # pseudo-matches pulling a team's rate to the mean
LOOKBACK = 20                  # appearances used to estimate a team's rate
TOP_LEAGUES = {
    "Premier League": 1.00, "La Liga": 0.98, "Serie A": 0.98,
    "Bundesliga": 0.97, "Ligue 1": 0.95,
    "Primeira Liga": 0.88, "Eredivisie": 0.88, "Championship": 0.90,
    "Belgian Pro League": 0.85, "Süper Lig": 0.84, "Scottish Premiership": 0.84,
    "UEFA Champions League": 0.95, "UEFA Europa League": 0.90,
    "UEFA Conference League": 0.86,
    # Second wave: solid top flights, weighted below the big five so the power
    # rankings keep their top-5-Europe bias, and above the 0.74 default.
    "Russian Premier League": 0.76, "Ukrainian Premier League": 0.75,
    "HNL": 0.73, "Nemzeti Bajnokság I": 0.72, "Eerste Divisie": 0.70,
    "Slovak Super Liga": 0.70, "Prva Liga": 0.68, "First Professional League": 0.68,
    "Cypriot First Division": 0.68, "Egyptian Premier League": 0.67,
    "Algerian Ligue 1": 0.66, "Bosnian Premier League": 0.65,
    "Belarusian Premier League": 0.62, "Cymru Premier": 0.62,
    "NIFL Premiership": 0.62,
    # Third wave: mid-tier European and the stronger non-European top flights,
    # still well below the big five, so the rankings keep their European bias.
    "Serbian Super Liga": 0.70, "Czech First League": 0.70, "Danish Superliga": 0.72,
    "Saudi Pro League": 0.72, "Israeli Premier League": 0.66, "Colombian Primera A": 0.68,
    "Persian Gulf Pro League": 0.66, "Premier Soccer League": 0.66,
    "UAE Pro League": 0.64, "Besta deildin": 0.60, "Erovnuli Liga": 0.62,
    "Kategoria Superiore": 0.60, "Macedonian Prva Liga": 0.58, "Virsliga": 0.60,
    "Premium liiga": 0.58, "A Lyga": 0.58, "Iraqi Stars League": 0.56,
    "Armenian Premier League": 0.56, "Faroese Premier League": 0.55,
}


def league_weight(league: str) -> float:
    return TOP_LEAGUES.get(league, 0.74)


def hit_stats(rows: list[dict], t: dict, scope: str = "any") -> tuple[int, int]:
    """(hits, considered) for one type over the most recent LOOKBACK appearances."""
    hits = considered = 0
    for r in reversed(rows):
        if scope == "home" and not r["home"]:
            continue
        if scope == "away" and r["home"]:
            continue
        if row_missing(r, t):
            if t["kind"] == "score" or r["has_stats"]:
                continue
            continue
        considered += 1
        if t["pred"](r):
            hits += 1
        if considered >= LOOKBACK:
            break
    return hits, considered


def build_baselines(matches: dict[int, dict]) -> dict:
    """Competition + type -> base rate, measured across every stored match."""
    agg: dict[tuple[str, str], list[int]] = {}
    teams = build_rows(matches)
    for slot in teams.values():
        for r in slot["rows"]:
            for t in TYPES:
                if row_missing(r, t):
                    continue
                k = (r["lg"], t["id"])
                a = agg.setdefault(k, [0, 0])
                a[0] += 1
                if t["pred"](r):
                    a[1] += 1
    # keep 0 and 1 out: a base rate of exactly zero would price a selection at
    # infinite odds, and it usually means the sample is too young to read
    return {k: (v[1] / v[0]) for k, v in agg.items() if v[0] >= 30 and 0 < v[1] < v[0]}


def estimate(matches: dict[int, dict], baselines: dict) -> dict:
    """
    Price every (team, active run) pair:
      {team_id: {type_id: {...}}}
    """
    out: dict[int, dict] = {}
    for tid, slot in build_rows(matches).items():
        rows = slot["rows"]
        if len(rows) < 4:
            continue
        per_type: dict[str, dict] = {}
        for t in TYPES:
            scope = t.get("scope", "any")
            hits, n = hit_stats(rows, t, scope)
            if n < 3:
                continue
            base = baselines.get((rows[-1]["lg"], t["id"]), 0.5)
            p = (hits + SHRINK_K * base) / (n + SHRINK_K)
            # clamp the estimate: unmodelled risk (rotation, injuries, red cards)
            # means no selection is ever a certainty, and a type that has never
            # landed in this competition (base rate 0) must not price at infinity
            p_capped = min(max(p, P_MIN), P_MAX)
            odds = max(ODDS_FLOOR, round((1.0 / p_capped) * (1.0 - MARGIN), 2))
            per_type[t["id"]] = {
                "hits": hits, "n": n, "base": round(base, 3),
                "p": round(p, 4), "pUsed": round(p_capped, 4),
                "fair": round(1.0 / p_capped, 2),
                "odds": odds,
                "market": odds,               # kept for compatibility
            }
        out[tid] = per_type
    return out


# market bucket -> how a selection is expressed as a bet
TYPE_SCOPE = {t["id"]: t.get("scope", "any") for t in TYPES}


def upcoming_selections(matches, fixtures_by_team, streaks, baselines, pricebook,
                        horizon_days=10):
    """One entry per (fixture, run) whose fixture kicks off inside the horizon."""
    by_team_run: dict[int, list[dict]] = {}
    for s in streaks:
        tid = None
        for t, per in pricebook.items():
            pass
        by_team_run.setdefault(s["team"], []).append(s)

    now = time.time()
    horizon = now + horizon_days * 86400
    out = []
    seen_pairs = set()
    for tid, entries in fixtures_by_team.items():
        prices = pricebook.get(tid)
        if not prices:
            continue
        for f in entries:
            if f.get("sc") or f.get("fin"):
                continue
            try:
                when = datetime.fromisoformat((f.get("utc") or "").replace("Z", "+00:00"))
            except ValueError:
                continue
            ts = when.timestamp()
            if ts < now - 3600 or ts > horizon:
                continue
            team = (f["h"] if f["h"].get("id") == tid else f["a"])
            home = f["h"].get("id") == tid
            for run in streaks:
                if run["team"] != team.get("n"):
                    continue
                if run["league"] not in (f.get("lg"),) and run["type"].startswith("match_"):
                    continue
                price = prices.get(run["type"])
                if not price or price["n"] < 8:
                    continue
                if run["length"] < 3:
                    continue
                # a home-scoped run is about home games: it can only continue if the
                # next match is at home (same for away), otherwise the price is wrong
                scope = TYPE_SCOPE.get(run["type"], "any")
                if scope == "home" and not home:
                    continue
                if scope == "away" and home:
                    continue
                key = (f["id"], run["type"], run["team"])
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                out.append({
                    "fixtureId": f["id"],
                    "ts": int(ts),
                    "date": f.get("date"),
                    "league": f.get("lg"),
                    "team": run["team"],
                    "home": home,
                    "opponent": (f["a"] if home else f["h"]).get("n"),
                    "headline": f"{f['h'].get('n')} vs {f['a'].get('n')}",
                    "run": run,
                    "price": price,
                })
    out.sort(key=lambda x: x["ts"])
    return out


def power_rankings(selections, limit=24):
    """Rank upcoming selections by confidence, weighted to strong competitions."""
    best: dict[tuple, dict] = {}
    for sel in selections:
        lw = league_weight(sel["league"])
        if lw < 0.84:
            continue                                    # top leagues (+ a few) only
        p = sel["price"]["p"]
        n = sel["price"]["n"]
        sample = min(1.0, n / 20.0)
        lenf = min(1.0, sel["run"]["length"] / 12.0)
        score = 100.0 * p * (0.55 + 0.30 * sample + 0.15 * lenf) * lw
        key = (sel["fixtureId"], sel["run"]["team"])
        prev = best.get(key)
        if prev is None or score > prev["_score"]:
            best[key] = {
                "rankScore": round(score, 1), "_score": score,
                "fixtureId": sel["fixtureId"], "headline": sel["headline"],
                "league": sel["league"], "team": sel["team"],
                "opponent": sel["opponent"], "date": sel["date"], "ts": sel["ts"],
                "run": sel["run"]["length"], "runId": sel["run"]["id"],
                "typeLabel": sel["run"]["typeLabel"], "market": sel["run"]["market"],
                "form": sel["run"]["form"],
                "confidence": round(sel["price"]["p"] * 100, 1),
                "fairOdds": sel["price"]["fair"], "estOdds": sel["price"]["market"],
                "record": f"{sel['price']['hits']}/{sel['price']['n']}",
            }
    rows = sorted(best.values(), key=lambda r: -r["_score"])[:limit]
    for i, r in enumerate(rows, 1):
        r["rank"] = i
        r.pop("_score", None)
    return rows


def high_odds(selections, minimum=2.2, limit=30):
    """Strong runs the market will price long (>minimum)."""
    rows = []
    seen = set()
    for sel in selections:
        odds = sel["price"]["market"]
        if odds < minimum or sel["price"]["n"] < 10:
            continue
        if sel["run"]["length"] < 4:
            continue
        key = (sel["run"]["team"], sel["run"]["type"])
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "team": sel["team"], "league": sel["league"], "headline": sel["headline"],
            "opponent": sel["opponent"], "date": sel["date"], "ts": sel["ts"],
            "run": sel["run"]["length"], "typeLabel": sel["run"]["typeLabel"],
            "market": sel["run"]["market"], "form": sel["run"]["form"],
            "estOdds": odds, "fairOdds": sel["price"]["fair"],
            "confidence": round(sel["price"]["p"] * 100, 1),
            "record": f"{sel['price']['hits']}/{sel['price']['n']}",
        })
    rows.sort(key=lambda r: (-r["run"], -r["estOdds"]))
    return rows[:limit]


def flexi_maths(legs: list[dict], max_losses: int) -> dict:
    """
    Arithmetic for a Flexi-style ticket: product of all legs, and the product of
    the surviving legs when exactly `max_losses` of the shortest-priced legs fail
    (the pessimistic case) versus the most expensive legs failing (the good case).
    This is arithmetic on our estimated prices, not SportyBet's internal split.
    """
    odds = sorted((l["odds"] for l in legs), reverse=True)
    total = 1.0
    for o in odds:
        total *= o
    keep_worst = odds[max_losses:] if len(odds) > max_losses else []
    keep_best = odds[: len(odds) - max_losses]
    worst = 1.0
    for o in keep_worst:
        worst *= o
    best = 1.0
    for o in keep_best:
        best *= o
    return {
        "legs": len(legs),
        "totalOdds": round(total, 2),
        "worstCaseOdds": round(worst, 2),
        "bestCaseOdds": round(best, 2),
        "minLegOdds": round(min(odds), 2) if odds else 0,
        "avgLegOdds": round(sum(odds) / len(odds), 2) if odds else 0,
        "allLegsAboveFloor": all(o >= 1.5 for o in odds),
    }


TICKET_PLANS = [
    # name,                    odds band,      leaguemax, prefers
    ("Ticket A · bankers",     (1.50, 1.62),   3, "confidence"),
    ("Ticket B · balanced",    (1.55, 1.80),   2, "spread"),
    ("Ticket C · value",       (1.65, 2.10),   2, "odds"),
]


def _ticket_legs(sel, taken, leagues, cap):
    return sel["fixtureId"] not in taken and leagues.get(sel["league"], 0) < cap


def build_accumulators(selections, size=16, tickets=3, max_losses=5, seed=None):
    """
    Three genuinely different tickets. For every fixture we keep the best leg that
    fits that ticket's price band, then take the highest-confidence fixtures and
    spread them across competitions so no single bad round sinks a ticket.
    """
    day = seed or datetime.now(WAT).strftime("%Y-%m-%d")
    tracked = set(LEAGUE_LABELS)

    by_fixture: dict[int, list[dict]] = {}
    for sel in selections:
        if sel["league"] not in tracked:
            continue
        if sel["price"]["n"] < 10 or sel["run"]["length"] < 3:
            continue
        by_fixture.setdefault(sel["fixtureId"], []).append(sel)
    if not by_fixture:
        return []

    used_pairs: set[tuple] = set()
    out = []
    for ti, (name, (lo, hi), leag_cap, _prefer) in enumerate(TICKET_PLANS[:tickets]):
        picks = []
        for group in by_fixture.values():
            band = [x for x in group
                    if lo <= x["price"]["odds"] <= hi
                    and (x["run"]["team"], x["run"]["type"]) not in used_pairs]
            if not band:
                continue
            band.sort(key=lambda x: (-x["price"]["p"], x["ts"]))
            picks.append(band[0])
        picks.sort(key=lambda x: (-x["price"]["p"], x["ts"]))

        legs, leagues = [], {}
        for sel in picks:
            if len(legs) >= size:
                break
            lg = sel["league"]
            if leagues.get(lg, 0) >= leag_cap:
                continue
            leagues[lg] = leagues.get(lg, 0) + 1
            legs.append({
                "headline": sel["headline"], "league": lg,
                "date": sel["date"], "ts": sel["ts"],
                "kickoff": datetime.fromtimestamp(sel["ts"], WAT).strftime("%a %d %b · %H:%M"),
                "team": sel["team"], "opponent": sel["opponent"],
                "typeLabel": sel["run"]["typeLabel"], "market": sel["run"]["market"],
                "run": sel["run"]["length"], "form": sel["run"]["form"],
                "confidence": round(sel["price"]["p"] * 100, 1),
                "record": f"{sel['price']['hits']}/{sel['price']['n']}",
                "odds": sel["price"]["odds"], "fairOdds": sel["price"]["fair"],
            })
        if len(legs) < 15:
            continue
        for l in legs:
            used_pairs.add((l["team"], l["typeLabel"]))
        legs.sort(key=lambda l: l["ts"])
        out.append({
            "id": f"{day}-T{ti + 1}",
            "name": name,
            "flexi": flexi_maths(legs, max_losses),
            "outcomes": ticket_outcomes(legs, max_losses),
            "leagues": sorted(leagues, key=lambda k: -leagues[k]),
            "legs": legs,
        })
    return out


def ticket_outcomes(legs: list[dict], max_losses: int) -> dict:
    """
    What the ticket actually needs, from the model's own probabilities:
      hitAll        - chance every leg lands
      anyReturn     - chance at most `max_losses` legs fail (the insurance pays)
    and the break-even odds for each leg (1/p), so the user can compare with the
    price SportyBet actually offers before staking anything.
    """
    from math import comb

    ps = [l["confidence"] / 100.0 for l in legs]
    n = len(ps)
    hit_all = 1.0
    for x in ps:
        hit_all *= x

    # P(exactly k failures) approximated by the Poisson-binomial via DP
    dist = [1.0]
    for x in ps:
        nxt = [0.0] * (len(dist) + 1)
        for i, d in enumerate(dist):
            nxt[i] += d * x              # leg lands
            nxt[i + 1] += d * (1 - x)    # leg fails
        dist = nxt
    any_return = sum(dist[: max_losses + 1])
    lose_all_or_none = dist[0]

    return {
        "n": n,
        "hitAllPct": round(hit_all * 100, 3),
        "anyReturnPct": round(any_return * 100, 1),
        "expectedLosses": round(sum(1 - x for x in ps), 1),
        "breakEvenAvgOdds": round(sum(1 / x for x in ps) / n, 2),
    }





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

        # If the set of tracked competitions changes (e.g. European competitions
        # are added), already-walked dates are missing those matches, so restart
        # the backfill from the top. Stored matches are skipped, only new ones
        # are fetched, so this is cheap.
        signature = ",".join(str(i) for i in sorted(TRACKED_IDS))
        if state.get("tracked") != signature:
            log("tracked competitions changed — re-walking history from the top")
            state["cursor"] = start
            state["backfillDone"] = False
        state["tracked"] = signature
        save_state(state)

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

    log("pricing selections (model odds)")
    baselines = build_baselines(have)
    pricebook = estimate(have, baselines)

    # the consistency section: 8+ hits in the last 10 games, per competition
    streak_keys = {(st["team"], st["league"], st["type"]) for st in streaks}
    stats = build_stats(build_rows(have), fixtures_by_team, streak_keys, MARKET_RANK)
    log(f"  {len(stats)} consistency entries (8+ of the last 10 games)")
    # how likely the model thinks each one is to land next time, for the sort menu
    for st in streaks + stats:
        pk = (pricebook.get(st.get("teamId")) or {}).get(st["type"])
        if pk:
            st["confidence"] = round(pk["p"] * 100, 1)
            st["fairOdds"] = pk["fair"]
            st["record"] = f"{pk['hits']}/{pk['n']}"
    selections = upcoming_selections(have, fixtures_by_team, streaks, baselines,
                                     pricebook, 10)
    rankings = power_rankings(selections)
    longshots = high_odds(selections)
    accas = build_accumulators(selections)

    # Hand the full priced pool to the ticket builder (cross-sport). Only the
    # next 24 hours matter: every ticket must settle inside one day so the stake
    # can roll over.
    now_ts = time.time()
    pool = []
    for sel in selections:
        hours = (sel["ts"] - now_ts) / 3600.0
        if hours < -0.5 or hours > 26:        # small grace for late kickoffs
            continue
        pool.append({
            "sport": "football", "league": sel["league"], "country": sel["league"],
            "team": sel["team"], "opponent": sel["opponent"], "home": sel["home"],
            "headline": sel["headline"], "ts": sel["ts"], "date": sel["date"],
            "type": sel["run"]["type"], "typeLabel": sel["run"]["typeLabel"],
            "market": sel["run"]["market"], "polarity": sel["run"]["polarity"],
            "run": sel["run"]["length"], "form": sel["run"]["form"],
            "confidence": round(sel["price"]["p"] * 100, 1),
            "p": sel["price"]["p"], "odds": sel["price"]["odds"],
            "fairOdds": sel["price"]["fair"],
            "record": f"{sel['price']['hits']}/{sel['price']['n']}",
            "fixtureId": sel["fixtureId"],
        })
    sel_path = os.path.join(DATA, "selections_football.json")
    with open(sel_path, "w", encoding="utf-8") as fh:
        json.dump({"generatedAt": datetime.now(WAT).isoformat(timespec="seconds"),
                   "sport": "football", "horizonHours": 24,
                   "count": len(pool), "selections": pool}, fh,
                  ensure_ascii=False, separators=(",", ":"))
    log(f"  {len(pool)} football selections inside 24h -> data/selections_football.json")
    log(f"  {len(selections)} selections priced · {len(rankings)} ranked · "
        f"{len(longshots)} longshots · {len(accas)} tickets")

    # per-league status for the strip: what is stored, and when each competition
    # next plays. Built from the archive and the fixture feed, so a newly added
    # league shows up here as soon as it has one game stored.
    games_by_league: dict[str, list] = {}
    for m in have.values():
        games_by_league.setdefault(m.get("lg") or "", []).append(m)
    runs_by_league: dict[str, int] = {}
    for st in streaks:
        lg = st.get("league") or ""
        runs_by_league[lg] = runs_by_league.get(lg, 0) + 1
    fixtures_by_league: dict[str, list] = {}
    for rows in fixtures_by_team.values():
        for f in rows:
            fixtures_by_league.setdefault(f.get("lg") or "", []).append(f)

    league_info = []
    for label in LEAGUE_LABELS:
        lg_games = games_by_league.get(label, [])
        nxt = min((f.get("utc") or "" for f in fixtures_by_league.get(label, [])
                   if f.get("utc")), default=None)
        if not lg_games and not nxt:
            continue          # neither played nor scheduled yet: nothing to show
        league_info.append({
            "key": slug(label), "label": label, "country": LABEL_COUNTRY.get(label, ""),
            "games": len(lg_games), "runs": runs_by_league.get(label, 0),
            "lastGame": max((m.get("date") or "" for m in lg_games), default=None),
            "nextGame": nxt,
        })
    league_info.sort(key=lambda lg: (0 if lg["nextGame"] else 1, lg["nextGame"] or "",
                                     -lg["runs"], -lg["games"]))

    as_of = max((m.get("date") or "" for m in have.values()), default=today.strftime("%Y-%m-%d"))
    teams_count = len({t for m in have.values() for t in (m["h"].get("id"), m["a"].get("id")) if t})
    payload = {
        "generatedAt": datetime.now(WAT).isoformat(timespec="seconds"),
        "asOf": as_of,
        "source": "FotMob (fotmob.com)",
        "eventCount": len(have),
        "teamCount": teams_count,
        "streakCount": len(streaks),
        "statsCount": len(stats),
        # the filter dropdown lists what actually has runs (in competition order);
        # the full tracked set is in leagueInfo, which drives the strip
        "leagues": [l for l in LEAGUE_LABELS if runs_by_league.get(l)],
        "stats": stats,
        "leagueInfo": league_info,
        "types": [{k: t[k] for k in ("id", "label", "market", "polarity")} for t in TYPES],
        "markets": MARKETS,
        "streaks": streaks,
        "analysis": {
            "generatedAt": datetime.now(WAT).isoformat(timespec="seconds"),
            "marginUsed": MARGIN,
            "probCap": P_MAX,
            "horizonDays": 10,
            "note": ("Prices are model estimates from each run's measured hit rate, "
                     "not live bookmaker quotes. Break-even odds = 1 / probability; "
                     "only take a leg if SportyBet's price is at or above it."),
            "powerRankings": rankings,
            "highOdds": longshots,
            "accumulators": accas,
        },
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
