#!/usr/bin/env python3
"""
Hot Streaks — multi-sport companion pipeline
===========================================

Builds data/sports.json for basketball, tennis, table tennis, ice hockey,
baseball and American football, using the same "run" (streak) shape the football
dashboard already renders.

Sources (all keyless):
  American football  nflverse games.csv   (GitHub-hosted, full history, includes
                                           closing spread and total lines)
  Baseball           statsapi.mlb.com     (official MLB, day schedules)
  Ice hockey         api-web.nhle.com     (official NHL, day schedules)
  Basketball         ESPN scoreboard      (NBA)
  Tennis             ESPN scoreboard      (ATP)
  Table tennis       ESPN scoreboard      (WTT)

The ESPN endpoints refuse some datacentre IP ranges. If they are unreachable the
sport is simply reported as unavailable in the output and the rest still builds.

Standard library only.

  python3 scripts/sports.py --mode full --days 3 --max-requests 400
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
HIST = os.path.join(DATA, "sports_history")
OUT = os.path.join(DATA, "sports.json")
STATE = os.path.join(DATA, "sports_state.json")
WAT = timezone(timedelta(hours=1))

NFLVERSE = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
NFL_SEASONS = 3            # seasons of NFL history kept (one request covers all)
MIN_STREAK = 3
RECENT_N = 6
FORM_N = 5
FIXTURE_DAYS = 14          # forward window for each sport's upcoming games

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
}

_lock = threading.Lock()
_last = [0.0]
MIN_GAP = float(os.environ.get("HOTSTREAKS_MIN_GAP", "0.06"))


def log(msg: str) -> None:
    with _lock:
        print(f"[{datetime.now(WAT).strftime('%H:%M:%S')}] {msg}", flush=True)


def _throttle():
    with _lock:
        gap = time.time() - _last[0]
        if gap < MIN_GAP:
            time.sleep(MIN_GAP - gap)
        _last[0] = time.time()


def http_text(url: str, tries: int = 3, timeout: int = 30):
    backoff = 1.5
    for attempt in range(tries):
        _throttle()
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code in (403, 401):
                raise                       # blocked - caller marks sport unavailable
            if e.code == 404:
                return None
            time.sleep(backoff)
            backoff *= 2
        except Exception:
            if attempt == tries - 1:
                return None
            time.sleep(backoff)
            backoff *= 2
    return None


def http_json(url: str, tries: int = 3, timeout: int = 30):
    t = http_text(url, tries=tries, timeout=timeout)
    if not t:
        return None
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        return None


def wat_date(ts_utc: str) -> tuple[str, int]:
    """ISO UTC string -> (WAT date, epoch seconds)"""
    try:
        dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return "", 0
    return dt.astimezone(WAT).strftime("%Y-%m-%d"), int(dt.timestamp())


def short(name: str) -> str:
    toks = [t for t in re.split(r"[\s\-]+", (name or "").strip()) if t]
    stop = {"fc", "sc", "ac", "as", "cf", "bc", "the"}
    while len(toks) > 1 and toks[0].lower() in stop:
        toks = toks[1:]
    if not toks:
        return (name or "?")[:3].upper()
    if len(toks) == 1:
        return re.sub(r"[^A-Za-z]", "", toks[0])[:3].upper()
    return "".join(t[0] for t in toks if t.isalnum())[:3].upper()


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")


# ==========================================================================
# Normalised game record
# ==========================================================================
# {
#   "id", "sport", "league", "date", "ts",
#   "h": {"n","s"}, "a": {"n","s"}, "hs", "as",
#   "x": {sport-specific numbers, e.g. nfl spread/total lines}
# }

def rec(gid, sport, league, iso, hname, aname, hs, as_, extra=None) -> dict:
    date, ts = wat_date(iso)
    return {
        "id": str(gid), "sport": sport, "league": league, "date": date, "ts": ts,
        "h": {"n": hname, "s": short(hname)}, "a": {"n": aname, "s": short(aname)},
        "hs": hs, "as": as_, "x": extra or {},
    }


# ==========================================================================
# Sources
# ==========================================================================

def fetch_nfl() -> list[dict]:
    txt = http_text(NFLVERSE, timeout=60)
    if not txt:
        return []
    rows = list(csv.DictReader(io.StringIO(txt)))
    seasons = sorted({r["season"] for r in rows if r.get("season")})[-NFL_SEASONS:]
    out = []
    for r in rows:
        if r.get("season") not in seasons:
            continue
        if r.get("game_type") not in ("REG", "WC", "DIV", "CON", "SB"):
            continue
        try:
            hs, as_ = int(r["home_score"]), int(r["away_score"])
        except (TypeError, ValueError):
            hs = as_ = None                              # scheduled, not played yet
        day = r.get("gameday") or ""
        if not day:
            continue
        clock = (r.get("gametime") or "13:00").strip()
        iso = f"{day}T{clock}:00Z" if clock else f"{day}T13:00:00Z"

        def fnum(key):
            v = (r.get(key) or "").strip()
            try:
                return float(v)
            except ValueError:
                return None

        out.append(rec(r.get("game_id") or f"{r['season']}{r['week']}{r['home_team']}{r['away_team']}",
                       "amfootball", "NFL", iso, r.get("home_team", ""), r.get("away_team", ""),
                       hs, as_, {"spread": fnum("spread_line"), "totalline": fnum("total_line"),
                                 "season": r.get("season"), "week": r.get("week")}))
    return out


def fetch_mlb_day(day: str) -> list[dict]:
    j = http_json(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={day}")
    out = []
    for d in (j or {}).get("dates", []):
        for g in d.get("games", []):
            if (g.get("status") or {}).get("detailedState") != "Final":
                continue
            t = g.get("teams") or {}
            h, a = t.get("home") or {}, t.get("away") or {}
            if h.get("score") is None or a.get("score") is None:
                continue
            out.append(rec(g.get("gamePk"), "baseball", "MLB", g.get("gameDate", ""),
                           ((h.get("team") or {}).get("name") or ""),
                           ((a.get("team") or {}).get("name") or ""),
                           int(h["score"]), int(a["score"]),
                           {"venue": (g.get("venue") or {}).get("name", "")}))
    return out


def fetch_nhl_day(day: str) -> list[dict]:
    j = http_json(f"https://api-web.nhle.com/v1/schedule/{day}")
    out = []
    for wk in (j or {}).get("gameWeek", []):
        if wk.get("date") and wk["date"] != day:
            continue                      # response spans a week - keep one day only
        for g in wk.get("games", []):
            if g.get("gameState") not in ("OFF", "FINAL"):
                continue
            h, a = g.get("homeTeam") or {}, g.get("awayTeam") or {}
            if h.get("score") is None or a.get("score") is None:
                continue
            hn = (h.get("commonName") or {}).get("default") or h.get("name") or ""
            an = (a.get("commonName") or {}).get("default") or a.get("name") or ""
            out.append(rec(g.get("id"), "icehockey", "NHL", g.get("startTimeUTC", ""),
                           hn, an, int(h["score"]), int(a["score"]),
                           {"venue": (g.get("venue") or {}).get("default", "")}))
    return out


ESPN = {
    "basketball": ("basketball/nba", "NBA", "basketball"),
    "tennis": ("tennis/atp", "ATP Tour", "tennis"),
    "tabletennis": ("table-tennis/wtt", "WTT", "tabletennis"),
}


def fetch_espn_day(key: str, day: str) -> list[dict]:
    path, league, sport = ESPN[key]
    j = http_json(f"https://site.api.espn.com/apis/site/v2/sports/{path}"
                  f"/scoreboard?dates={day.replace('-', '')}")
    out = []
    for ev in (j or {}).get("events", []):
        comp = (ev.get("competitions") or [{}])[0]
        if not ((comp.get("status") or {}).get("type") or {}).get("completed"):
            continue
        home = away = None
        for c in comp.get("competitors", []):
            if c.get("homeAway") == "home":
                home = c
            elif c.get("homeAway") == "away":
                away = c
        if not home or not away:
            continue
        try:
            hs, as_ = int(home.get("score")), int(away.get("score"))
        except (TypeError, ValueError):
            continue
        hn = (home.get("team") or {}).get("displayName") or ""
        an = (away.get("team") or {}).get("displayName") or ""
        lines = {}
        notes = (comp.get("status") or {}).get("type", {})
        out.append(rec(ev.get("id"), sport, league, ev.get("date", ""), hn, an, hs, as_,
                       {"detail": notes.get("detail", ""),
                        "sets": [c.get("linescores") for c in comp.get("competitors", [])]}))
    return out


SOURCE = {
    "amfootball": ("American football", "NFL", fetch_nfl),
    "baseball": ("Baseball", "MLB", fetch_mlb_day),
    "icehockey": ("Ice hockey", "NHL", fetch_nhl_day),
    "basketball": ("Basketball", "NBA", lambda d: fetch_espn_day("basketball", d)),
    "tennis": ("Tennis", "ATP", lambda d: fetch_espn_day("tennis", d)),
    "tabletennis": ("Table tennis", "WTT", lambda d: fetch_espn_day("tabletennis", d)),
}
DAY_SOURCES = {"baseball", "icehockey", "basketball", "tennis", "tabletennis"}


# ==========================================================================
# Run types per sport  (a handful each - these sports simply offer less)
# ==========================================================================

def _score_fmt(r, _=None):
    return f"{r['gf']}-{r['ga']}"


def _total_fmt(r, _=None):
    return f"{r['total']} total"


def _ats_fmt(r, _=None):
    line = r["x"].get("spread")
    return f"ATS {'' if line is None else ('+' if line > 0 else '')}{line:g}" if line is not None else _score_fmt(r)


def _ou_fmt(r, _=None):
    line = r["x"].get("totalline")
    return f"{r['total']} pts (line {line:g})" if line is not None else _total_fmt(r)


def line_type(tid, label, market, polarity, key, val, op=">", scope="any", fmt=None):
    def pred(r, key=key, val=val, op=op):
        v = r.get(key)
        if v is None:
            return False
        return v > val if op == ">" else v < val
    return {"id": tid, "label": label, "market": market, "polarity": polarity,
            "scope": scope, "pred": pred, "fmt": fmt or _total_fmt}


def result_type(tid, label, market, polarity, pred, scope="any", fmt=None):
    return {"id": tid, "label": label, "market": market, "polarity": polarity,
            "scope": scope, "pred": pred, "fmt": fmt or _score_fmt}


TYPES = {
    "amfootball": [
        result_type("wins", "Straight wins", "result", "hot", lambda r: r["res"] == "W"),
        result_type("losses", "Straight losses", "result", "cold", lambda r: r["res"] == "L"),
        result_type("home_wins", "Straight home wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="home"),
        result_type("away_wins", "Straight away wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="away"),
        result_type("unbeaten", "Unbeaten run", "result", "hot", lambda r: r["res"] != "L"),
        result_type("winless", "Winless run", "result", "cold", lambda r: r["res"] != "W"),
        line_type("team_20", "Team 20+ points", "scoring", "hot", "gf", 19.5),
        line_type("team_27", "Team 27+ points", "scoring", "hot", "gf", 26.5),
        line_type("team_30", "Team 30+ points", "scoring", "hot", "gf", 29.5),
        line_type("allow_u20", "Team allows under 20", "defence", "hot", "ga", 19.5, op="<"),
        line_type("total_o44", "Over 44.5 points", "total", "spicy", "total", 44.5),
        line_type("total_u44", "Under 44.5 points", "total", "spicy", "total", 44.5, op="<"),
        line_type("total_o50", "Over 50.5 points", "total", "spicy", "total", 50.5),
        result_type("covers", "Covers the spread", "spread", "hot", lambda r: bool(r["x"].get("covers"))),
        result_type("no_cover", "Fails to cover", "spread", "cold",
                    lambda r: r["x"].get("covers") is False, fmt=_ats_fmt),
        result_type("over_line", "Goes over the total line", "total", "spicy",
                    lambda r: bool(r["x"].get("over")), fmt=_ou_fmt),
        result_type("under_line", "Goes under the total line", "total", "spicy",
                    lambda r: r["x"].get("over") is False, fmt=_ou_fmt),
        result_type("btts20", "Both teams 20+", "scoring", "spicy",
                    lambda r: r["gf"] >= 20 and r["ga"] >= 20),
    ],
    "baseball": [
        result_type("wins", "Straight wins", "result", "hot", lambda r: r["res"] == "W"),
        result_type("losses", "Straight losses", "result", "cold", lambda r: r["res"] == "L"),
        result_type("home_wins", "Straight home wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="home"),
        result_type("away_wins", "Straight away wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="away"),
        result_type("unbeaten", "Unbeaten run", "result", "hot", lambda r: r["res"] != "L"),
        line_type("team_4", "Team 4+ runs", "scoring", "hot", "gf", 3.5),
        line_type("team_5", "Team 5+ runs", "scoring", "hot", "gf", 4.5),
        line_type("allow_u3", "Team allows 3 or fewer", "defence", "hot", "ga", 3.5, op="<"),
        line_type("total_o85", "Over 8.5 runs", "total", "spicy", "total", 8.5),
        line_type("total_u85", "Under 8.5 runs", "total", "spicy", "total", 8.5, op="<"),
        line_type("total_o95", "Over 9.5 runs", "total", "spicy", "total", 9.5),
    ],
    "icehockey": [
        result_type("wins", "Straight wins", "result", "hot", lambda r: r["res"] == "W"),
        result_type("losses", "Straight losses", "result", "cold", lambda r: r["res"] == "L"),
        result_type("home_wins", "Straight home wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="home"),
        result_type("away_wins", "Straight away wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="away"),
        result_type("unbeaten", "Unbeaten run (60 min)", "result", "hot", lambda r: r["res"] != "L"),
        line_type("team_3", "Team 3+ goals", "scoring", "hot", "gf", 2.5),
        line_type("team_4", "Team 4+ goals", "scoring", "hot", "gf", 3.5),
        line_type("allow_u3", "Team allows 2 or fewer", "defence", "hot", "ga", 2.5, op="<"),
        line_type("total_o55", "Over 5.5 goals", "total", "spicy", "total", 5.5),
        line_type("total_u55", "Under 5.5 goals", "total", "spicy", "total", 5.5, op="<"),
        line_type("total_o65", "Over 6.5 goals", "total", "spicy", "total", 6.5),
    ],
    "basketball": [
        result_type("wins", "Straight wins", "result", "hot", lambda r: r["res"] == "W"),
        result_type("losses", "Straight losses", "result", "cold", lambda r: r["res"] == "L"),
        result_type("home_wins", "Straight home wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="home"),
        result_type("away_wins", "Straight away wins", "result", "hot",
                    lambda r: r["res"] == "W", scope="away"),
        result_type("unbeaten", "Unbeaten run", "result", "hot", lambda r: r["res"] != "L"),
        line_type("team_100", "Team 100+ points", "scoring", "hot", "gf", 99.5),
        line_type("team_110", "Team 110+ points", "scoring", "hot", "gf", 109.5),
        line_type("team_120", "Team 120+ points", "scoring", "hot", "gf", 119.5),
        line_type("total_o210", "Over 210.5 points", "total", "spicy", "total", 210.5),
        line_type("total_o220", "Over 220.5 points", "total", "spicy", "total", 220.5),
        line_type("total_u210", "Under 210.5 points", "total", "spicy", "total", 210.5, op="<"),
    ],
    "tennis": [
        result_type("wins", "Straight wins", "result", "hot", lambda r: r["res"] == "W"),
        result_type("losses", "Straight losses", "result", "cold", lambda r: r["res"] == "L"),
        result_type("unbeaten", "Unbeaten run", "result", "hot", lambda r: r["res"] != "L"),
        line_type("straight_sets", "Won without dropping a set", "sets", "hot",
                  "sets_lost", 0.5, op="<", fmt=lambda r, _=None: f"{r.get('games')} games"),
    ],
    "tabletennis": [
        result_type("wins", "Straight wins", "result", "hot", lambda r: r["res"] == "W"),
        result_type("losses", "Straight losses", "result", "cold", lambda r: r["res"] == "L"),
        result_type("unbeaten", "Unbeaten run", "result", "hot", lambda r: r["res"] != "L"),
        line_type("team_3", "Won 3+ games", "scoring", "hot", "gf", 2.5),
    ],
}

MARKETS = {
    "amfootball": [{"id": "result", "label": "Result"}, {"id": "scoring", "label": "Scoring"},
                   {"id": "defence", "label": "Defence"}, {"id": "total", "label": "Totals"},
                   {"id": "spread", "label": "Spread"}],
    "baseball": [{"id": "result", "label": "Result"}, {"id": "scoring", "label": "Scoring"},
                 {"id": "defence", "label": "Defence"}, {"id": "total", "label": "Totals"}],
    "icehockey": [{"id": "result", "label": "Result"}, {"id": "scoring", "label": "Scoring"},
                  {"id": "defence", "label": "Defence"}, {"id": "total", "label": "Totals"}],
    "basketball": [{"id": "result", "label": "Result"}, {"id": "scoring", "label": "Scoring"},
                   {"id": "total", "label": "Totals"}],
    "tennis": [{"id": "result", "label": "Result"}, {"id": "sets", "label": "Sets"}],
    "tabletennis": [{"id": "result", "label": "Result"}, {"id": "scoring", "label": "Scoring"}],
}


# ==========================================================================
# History store (one file per sport)
# ==========================================================================

def hist_path(sport: str) -> str:
    return os.path.join(HIST, f"{sport}.jsonl")


def load_history(sport: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    p = hist_path(sport)
    if not os.path.exists(p):
        return out
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            out[r["id"]] = r
    return out


def append_history(sport: str, records: list[dict]) -> int:
    if not records:
        return 0
    os.makedirs(HIST, exist_ok=True)
    records = sorted(records, key=lambda r: (r["date"], r["id"]))
    with open(hist_path(sport), "a", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
    return len(records)


# ==========================================================================
# Streak engine (generic)
# ==========================================================================

def side_rows(games: list[dict], sport: str) -> dict[str, dict]:
    """team name -> {games[...], league, rows[...]}"""
    teams: dict[str, dict] = {}
    for g in sorted(games, key=lambda x: (x["date"], x["id"])):
        if g["hs"] is None or g["as"] is None:
            continue
        for side in ("h", "a"):
            other = "a" if side == "h" else "h"
            gf = g["hs"] if side == "h" else g["as"]
            ga = g["as"] if side == "h" else g["hs"]
            x = dict(g.get("x") or {})
            if sport == "amfootball":
                spread = x.get("spread")
                margin = gf - ga
                if spread is not None:
                    # nflverse: spread_line is the home line; positive = home receiving points
                    home_line = spread if side == "h" else -spread
                    x["covers"] = (margin + home_line) > 0
            row = {
                "date": g["date"], "ts": g["ts"], "opp": g[other]["n"],
                "opp_id": g[other]["s"], "home": side == "h", "gf": gf, "ga": ga,
                "total": gf + ga,
                "res": "W" if gf > ga else ("L" if gf < ga else "D"),
                "x": x, "league": g["league"],
            }
            slot = teams.setdefault(g[side]["n"], {"name": g[side]["n"], "short": g[side]["s"],
                                                   "league": g["league"], "rows": []})
            slot["rows"].append(row)
            slot["league"] = g["league"]

    for slot in teams.values():
        slot["rows"].sort(key=lambda r: (r["date"], 0 if r["home"] else 1))
    return teams


def build_sport_streaks(sport: str, games: list[dict], upcoming: dict[str, list[dict]]) -> list[dict]:
    teams = side_rows(games, sport)
    today = datetime.now(WAT).strftime("%Y-%m-%d")
    out = []
    for name, slot in teams.items():
        rows = slot["rows"]
        if len(rows) < MIN_STREAK:
            continue
        form = "".join(r["res"] for r in rows[-FORM_N:][::-1])
        nxt = next_game(upcoming.get(name, []), name, today)
        for t in TYPES[sport]:
            scope = t.get("scope", "any")
            length, run = 0, []
            for r in reversed(rows):
                if scope == "home" and not r["home"]:
                    continue
                if scope == "away" and r["home"]:
                    continue
                if t["pred"](r):
                    length += 1
                    if len(run) < RECENT_N:
                        run.append(r)
                else:
                    break
            if length < MIN_STREAK:
                continue
            out.append({
                "id": f"{slug(name)}-{t['id']}",
                "team": name, "teamShort": slot["short"],
                "league": slot["league"], "country": None,
                "type": t["id"], "typeLabel": t["label"],
                "market": t["market"], "length": length, "polarity": t["polarity"],
                "form": form,
                "recent": [{"date": r["date"], "opp": r["opp"], "home": r["home"],
                            "score": _score_fmt(r), "result": r["res"],
                            "stat": _safe(t["fmt"], r)} for r in run],
                "next": nxt, "matches": [],
            })
    out.sort(key=lambda s: (-s["length"], s["team"], s["type"]))
    return out


def _safe(fmt, row) -> str:
    try:
        v = fmt(row, None)
        return str(v) if v is not None else ""
    except Exception:
        return ""


def next_game(entries: list[dict], team: str, today: str) -> dict | None:
    now = datetime.now(timezone.utc)
    best = None
    for g in entries:
        try:
            when = datetime.fromisoformat((g.get("iso") or "").replace("Z", "+00:00"))
        except ValueError:
            continue
        if when < now - timedelta(hours=3):
            continue
        if best is None or when < best[0]:
            best = (when, g)
    if not best:
        return None
    when, g = best
    home = g["home"] == team
    local = when.astimezone(WAT)
    return {
        "date": local.strftime("%Y-%m-%d"), "ts": int(when.timestamp()),
        "hours": round((when - now).total_seconds() / 3600.0, 2),
        "kickoffLabel": f"{local.strftime('%a %d %b')} · {local.strftime('%H:%M')}",
        "opponent": g["away"] if home else g["home"],
        "home": home, "venue": g.get("venue") or "",
        "comp": g.get("league") or "",
        "homeTeam": g["home"], "awayTeam": g["away"],
        "headline": f"{g['home']} vs {g['away']}", "today": local.strftime("%Y-%m-%d") == today,
    }


# ==========================================================================
# Upcoming games (next fixtures for each sport)
# ==========================================================================

def upcoming_nfl(games: list[dict]) -> dict[str, list[dict]]:
    """nflverse ships the full schedule, so future rows are already in the CSV."""
    out: dict[str, list[dict]] = {}
    for g in games:
        if g.get("hs") is not None or not g.get("ts"):
            continue
        iso = ""
        if g.get("ts"):
            iso = datetime.fromtimestamp(g["ts"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for side in ("h", "a"):
            out.setdefault(g[side]["n"], []).append({
                "home": g["h"]["n"], "away": g["a"]["n"], "league": "NFL",
                "iso": iso, "venue": g["x"].get("venue", ""),
            })
    return out


def upcoming_espn(key: str) -> dict[str, list[dict]]:
    path, league, _sport = ESPN[key]
    today = datetime.now(WAT).date()
    out: dict[str, list[dict]] = {}
    for i in range(FIXTURE_DAYS):
        day = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        j = http_json(f"https://site.api.espn.com/apis/site/v2/sports/{path}"
                      f"/scoreboard?dates={day.replace('-', '')}")
        for ev in (j or {}).get("events", []):
            comp = (ev.get("competitions") or [{}])[0]
            home = away = None
            for c in comp.get("competitors", []):
                if c.get("homeAway") == "home":
                    home = c
                elif c.get("homeAway") == "away":
                    away = c
            if not home or not away:
                continue
            hn = (home.get("team") or {}).get("displayName") or ""
            an = (away.get("team") or {}).get("displayName") or ""
            for nm in (hn, an):
                out.setdefault(nm, []).append({
                    "home": hn, "away": an, "league": league, "iso": ev.get("date", ""),
                    "venue": (comp.get("venue") or {}).get("fullName", ""),
                })
    return out


def upcoming_mlb() -> dict[str, list[dict]]:
    today = datetime.now(WAT).date()
    out: dict[str, list[dict]] = {}
    for i in range(FIXTURE_DAYS):
        day = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        j = http_json(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={day}")
        for d in (j or {}).get("dates", []):
            for g in d.get("games", []):
                state = (g.get("status") or {}).get("detailedState", "")
                if state in ("Final", "Cancelled", "Postponed"):
                    continue
                t = g.get("teams") or {}
                hn = ((t.get("home") or {}).get("team") or {}).get("name", "")
                an = ((t.get("away") or {}).get("team") or {}).get("name", "")
                for nm in (hn, an):
                    out.setdefault(nm, []).append({
                        "home": hn, "away": an, "league": "MLB",
                        "iso": g.get("gameDate", ""),
                        "venue": (g.get("venue") or {}).get("name", "")})
    return out


def upcoming_nhl() -> dict[str, list[dict]]:
    today = datetime.now(WAT).date()
    out: dict[str, list[dict]] = {}
    for i in range(FIXTURE_DAYS):
        day = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        j = http_json(f"https://api-web.nhle.com/v1/schedule/{day}")
        for wk in (j or {}).get("gameWeek", []):
            for g in wk.get("games", []):
                if g.get("gameState") in ("OFF", "FINAL"):
                    continue
                hn = ((g.get("homeTeam") or {}).get("commonName") or {}).get("default", "")
                an = ((g.get("awayTeam") or {}).get("commonName") or {}).get("default", "")
                for nm in (hn, an):
                    out.setdefault(nm, []).append({
                        "home": hn, "away": an, "league": "NHL",
                        "iso": g.get("startTimeUTC", ""),
                        "venue": (g.get("venue") or {}).get("default", "")})
    return out


# ==========================================================================
# Orchestration
# ==========================================================================

def load_state() -> dict:
    if os.path.exists(STATE):
        try:
            with open(STATE, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return {}


def save_state(st: dict):
    os.makedirs(DATA, exist_ok=True)
    with open(STATE, "w", encoding="utf-8") as fh:
        json.dump(st, fh, indent=1, sort_keys=True)
        fh.write("\n")


def days_between(start: str, end: str) -> list[str]:
    a = datetime.strptime(start, "%Y-%m-%d").date()
    b = datetime.strptime(end, "%Y-%m-%d").date()
    return [(a + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((b - a).days + 1)]


def collect(sport: str, days: list[str], max_requests: int, budget: list[int]) -> tuple[int, list[dict]]:
    """Fetch day schedules for a day-based sport. Returns (new_records, games_since)."""
    fetch = SOURCE[sport][2]
    have = load_history(sport)
    new, games = [], []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for day, rows in zip(days, ex.map(fetch, days)):
            games.extend(rows)
            for r in rows:
                if r["id"] not in have:
                    new.append(r)
    return (append_history(sport, new) if new else 0), games


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full", "daily", "backfill"], default="full")
    ap.add_argument("--days", type=int, default=3, help="recent days refreshed each run")
    ap.add_argument("--chunk-days", type=int, default=30, help="backfill chunk size")
    ap.add_argument("--max-requests", type=int, default=1500)
    ap.add_argument("--max-minutes", type=float, default=60)
    ap.add_argument("--horizon-days", type=int, default=400, help="how far back to build")
    args = ap.parse_args()

    t0 = time.time()
    deadline = t0 + args.max_minutes * 60
    today = datetime.now(WAT).date()
    os.makedirs(DATA, exist_ok=True)
    state = load_state()
    log(f"multi-sport pipeline — mode={args.mode}")

    sports_out: dict[str, dict] = {}
    for sport, (label, league, _f) in SOURCE.items():
        try:
            # ---------------- American football: one request, full history ----------
            if sport == "amfootball":
                all_rows = fetch_nfl()
                games = [g for g in all_rows if g.get("hs") is not None]
                if not all_rows:
                    raise RuntimeError("no rows")
                have = load_history(sport)
                fresh = [g for g in games if g["id"] not in have]
                if fresh:
                    append_history(sport, fresh)
                allgames = list(load_history(sport).values())
                up = upcoming_nfl(all_rows)
                available, reason = True, None
            else:
                # ---------------- day-based sports ---------------------------------
                cur = state.get("cursor", {}).get(sport)
                first = (today - timedelta(days=args.days))
                if not cur:
                    cur = first.strftime("%Y-%m-%d")
                end = str(today)
                budget = [0]

                def fetch_day(day: str):
                    if budget[0] >= args.max_requests or time.time() > deadline:
                        return []
                    budget[0] += 1
                    return SOURCE[sport][2](day)

                have = load_history(sport)
                # recent window first
                recent = days_between(first.strftime("%Y-%m-%d"), end)
                games = []
                with ThreadPoolExecutor(max_workers=4) as ex:
                    for rows in ex.map(fetch_day, recent):
                        games.extend(rows or [])
                # then walk backwards while budget allows
                cursor = datetime.strptime(cur, "%Y-%m-%d").date()
                floor = today - timedelta(days=args.horizon_days)
                while budget[0] < args.max_requests and time.time() < deadline and cursor > floor:
                    c_end = cursor - timedelta(days=1)
                    c_start = max(c_end - timedelta(days=args.chunk_days - 1), floor)
                    chunk = days_between(str(c_start), str(c_end))
                    if not chunk:
                        break
                    got = []
                    with ThreadPoolExecutor(max_workers=4) as ex:
                        for rows in ex.map(fetch_day, chunk):
                            got.extend(rows or [])
                    cursor = c_start
                    if not got:
                        # off-season / lockout: step over the dead stretch with a
                        # cheap 3-day probe rather than 30 empty requests
                        while cursor > floor:
                            probe_start = max(cursor - timedelta(days=120), floor)
                            probe_days = days_between(str(probe_start), str(probe_start + timedelta(days=2)))
                            found = []
                            for d in probe_days:
                                found.extend(SOURCE[sport][2](d) or [])
                            budget[0] += len(probe_days)
                            cursor = probe_start
                            if found:
                                got.extend(found)
                                break
                        log(f"  {sport}: skipped inactive stretch to {cursor}")
                    else:
                        games.extend(got)
                    state.setdefault("cursor", {})[sport] = str(cursor)
                    save_state(state)
                    log(f"  {sport}: walked back to {cursor} ({budget[0]} requests)")
                new = [g for g in games if g["id"] not in have]
                if new:
                    append_history(sport, new)
                log(f"  {sport}: {len(games)} games this window, {len(new)} new")
                allgames = list(load_history(sport).values())
                up = (upcoming_nfl(games) if sport == "amfootball" else
                      upcoming_mlb() if sport == "baseball" else
                      upcoming_nhl() if sport == "icehockey" else
                      upcoming_espn(sport))
                available, reason = True, None

            streaks = build_sport_streaks(sport, allgames, up)
            teams = len({g[side]["n"] for g in allgames for side in ("h", "a")})
            as_of = max((g["date"] for g in allgames if g.get("date")), default=str(today))
            sports_out[sport] = {
                "key": sport, "label": label, "league": league,
                "available": available, "reason": reason,
                "asOf": as_of, "eventCount": len(allgames), "teamCount": teams,
                "streakCount": len(streaks),
                "leagues": sorted({s["league"] for s in streaks}),
                "types": [{"id": t["id"], "label": t["label"], "market": t["market"],
                           "polarity": t["polarity"]} for t in TYPES[sport]],
                "markets": MARKETS[sport],
                "streaks": streaks,
            }
            log(f"  {sport}: {len(streaks)} runs from {len(allgames)} games")
        except urllib.error.HTTPError as e:
            log(f"  {sport}: source refused ({e.code}) — reported as unavailable")
            sports_out[sport] = {"key": sport, "label": label, "league": league,
                                 "available": False,
                                 "reason": f"source returned HTTP {e.code} from this network",
                                 "streaks": [], "types": [], "markets": MARKETS[sport],
                                 "leagues": [], "asOf": None, "eventCount": 0,
                                 "teamCount": 0, "streakCount": 0}
        except Exception as e:
            log(f"  {sport}: failed ({type(e).__name__}: {str(e)[:60]})")
            sports_out[sport] = {"key": sport, "label": label, "league": league,
                                 "available": False, "reason": f"{type(e).__name__}",
                                 "streaks": [], "types": [], "markets": MARKETS[sport],
                                 "leagues": [], "asOf": None, "eventCount": 0,
                                 "teamCount": 0, "streakCount": 0}

    payload = {
        "generatedAt": datetime.now(WAT).isoformat(timespec="seconds"),
        "sports": sports_out,
        "order": ["amfootball", "basketball", "baseball", "icehockey", "tennis", "tabletennis"],
    }
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
        fh.write("\n")
    ok = [s for s, v in sports_out.items() if v["available"]]
    log(f"wrote {OUT}: {len(ok)}/{len(SOURCE)} sports available — "
        + ", ".join(f"{s}:{sports_out[s]['streakCount']}" for s in ok))
    log(f"finished in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
