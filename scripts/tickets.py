#!/usr/bin/env python3
"""
Hot Streaks — cross-sport accumulator builder
=============================================

Merges the priced selection pools written by the football and multi-sport
pipelines, keeps only what settles in the next 24 hours (so the stake can roll
over the same day), and builds three independent Flexi-style tickets.

A ticket may mix sports, or end up single-sport — it simply takes the strongest
qualifying selections available. Each leg is a different match, and the builder
spreads legs across competitions and sports so no one result can sink a ticket.

Output: data/tickets.json

  python3 scripts/tickets.py --window 24 --size 16 --tickets 3 --max-losses 5
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
WAT = timezone(timedelta(hours=1))

POOLS = {
    "football": os.path.join(DATA, "selections_football.json"),
    "sports": os.path.join(DATA, "selections_sports.json"),
}

SPORT_LABEL = {
    "football": "Football", "basketball": "Basketball", "tennis": "Tennis",
    "tabletennis": "Table tennis", "icehockey": "Ice hockey",
    "baseball": "Baseball", "amfootball": "American football",
}

# name, odds band, max legs per competition, max legs per sport, selection style
TICKET_PLANS = [
    ("Ticket A · bankers",  (1.50, 1.62), 2, 8, "confidence"),
    ("Ticket B · balanced", (1.55, 1.80), 2, 7, "mixed"),
    ("Ticket C · value",    (1.65, 2.10), 2, 6, "odds"),
]


def load_pools() -> list[dict]:
    out: list[dict] = []
    for name, path in POOLS.items():
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                payload = json.load(fh)
        except (json.JSONDecodeError, OSError):
            continue
        for sel in payload.get("selections", []):
            sel.setdefault("sport", name)
            out.append(sel)
    return out


def within_window(sel: dict, now: float, hours: float) -> bool:
    ts = sel.get("ts")
    if not ts:
        return False
    delta = (ts - now) / 3600.0
    return -0.5 <= delta <= hours


def flexi_maths(legs: list[dict], max_losses: int) -> dict:
    odds = sorted((l["odds"] for l in legs), reverse=True)
    total = 1.0
    for o in odds:
        total *= o
    keep_worst = odds[max_losses:]
    worst = 1.0
    for o in keep_worst:
        worst *= o
    return {
        "legs": len(legs),
        "totalOdds": round(total, 2),
        "worstCaseOdds": round(worst, 2),
        "minLegOdds": round(min(odds), 2) if odds else 0,
        "maxLegOdds": round(max(odds), 2) if odds else 0,
        "avgLegOdds": round(sum(odds) / len(odds), 2) if odds else 0,
        "allLegsAboveFloor": all(o >= 1.5 for o in odds),
    }


def ticket_outcomes(legs: list[dict], max_losses: int) -> dict:
    """Chance every leg lands, and chance the Flexi insurance pays out."""
    ps = [l["confidence"] / 100.0 for l in legs]
    hit_all = 1.0
    for x in ps:
        hit_all *= x
    dist = [1.0]
    for x in ps:
        nxt = [0.0] * (len(dist) + 1)
        for i, d in enumerate(dist):
            nxt[i] += d * x
            nxt[i + 1] += d * (1 - x)
        dist = nxt
    return {
        "n": len(ps),
        "hitAllPct": round(hit_all * 100, 3),
        "anyReturnPct": round(sum(dist[: max_losses + 1]) * 100, 1),
        "expectedLosses": round(sum(1 - x for x in ps), 1),
        "breakEvenAvgOdds": round(sum(1 / x for x in ps) / len(ps), 2) if ps else 0,
    }


def build(pool: list[dict], window: float, size: int, tickets: int,
          max_losses: int, now: float) -> list[dict]:
    fresh = [s for s in pool if within_window(s, now, window)]
    by_fixture: dict[str, list[dict]] = {}
    for sel in fresh:
        # key on the actual match, not the streak id: the two sides of one game
        # arrive as separate selections and must never share a ticket
        pair = "|".join(sorted([sel.get("team", ""), sel.get("opponent", "")]))
        by_fixture.setdefault(f"{sel['sport']}:{pair}:{sel.get('ts')}", []).append(sel)
    if not by_fixture:
        return []

    day = datetime.now(WAT).strftime("%Y-%m-%d")
    used: set[tuple] = set()
    used_types: set[tuple] = set()
    used_matches: set[str] = set()          # a match belongs to one ticket only
    out = []
    for ti, (name, (lo, hi), cap_comp, cap_sport, style) in enumerate(TICKET_PLANS[:tickets]):
        picks = []
        for mkey, group in by_fixture.items():
            if mkey in used_matches:
                continue                    # already on an earlier ticket
            band = [x for x in group
                    if lo <= x["odds"] <= hi
                    and (x["sport"], x["team"]) not in used
                    and (x["sport"], x["team"], x["type"]) not in used_types]
            if not band:
                continue
            if style == "odds":
                band.sort(key=lambda x: (-x["odds"], -x["confidence"]))
            else:
                band.sort(key=lambda x: (-x["confidence"], x["ts"]))
            picks.append((mkey, band[0]))
        if style == "mixed":
            # rotate sports so the ticket is genuinely diversified
            picks.sort(key=lambda x: (x[1]["sport"], -x[1]["confidence"]))
        else:
            picks.sort(key=lambda x: (-x[1]["confidence"], x[1]["ts"]))

        legs, comps, sports, seen_teams = [], {}, {}, set()
        for mkey, sel in picks:
            if len(legs) >= size:
                break
            sp, lg = sel["sport"], sel.get("league") or ""
            if comps.get(lg, 0) >= cap_comp or sports.get(sp, 0) >= cap_sport:
                continue
            if (sp, sel["team"]) in seen_teams:      # no team twice inside one ticket
                continue
            comps[lg] = comps.get(lg, 0) + 1
            sports[sp] = sports.get(sp, 0) + 1
            seen_teams.add((sp, sel["team"]))
            used_types.add((sp, sel["team"], sel["type"]))
            used_matches.add(mkey)
            when = datetime.fromtimestamp(sel["ts"], WAT)
            legs.append({
                "sport": sp, "sportLabel": SPORT_LABEL.get(sp, sp),
                "league": lg, "headline": sel.get("headline", ""),
                "team": sel.get("team", ""), "opponent": sel.get("opponent", ""),
                "typeLabel": sel.get("typeLabel", ""), "market": sel.get("market", ""),
                "run": sel.get("run", 0), "form": sel.get("form", ""),
                "confidence": sel.get("confidence"), "record": sel.get("record", ""),
                "odds": sel["odds"], "fairOdds": sel.get("fairOdds"),
                "ts": sel["ts"], "date": sel.get("date"),
                "kickoff": when.strftime("%a %d %b · %H:%M"),
            })
        if not legs:
            continue
        for l in legs:
            used.add((l["sport"], l["team"]))      # no team on two tickets either
        legs.sort(key=lambda l: l["ts"])
        out.append({
            "id": f"{day}-T{ti + 1}",
            "name": name,
            "windowHours": window,
            "flexi": flexi_maths(legs, max_losses),
            "outcomes": ticket_outcomes(legs, max_losses),
            "sports": sorted(sports, key=lambda s: -sports[s]),
            "sportBreakdown": {SPORT_LABEL.get(k, k): v for k, v in
                               sorted(sports.items(), key=lambda x: -x[1])},
            "leagues": sorted(comps, key=lambda k: -comps[k]),
            "legs": legs,
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=float, default=24.0, help="hours until the last leg kicks off")
    ap.add_argument("--size", type=int, default=16, help="maximum legs per ticket")
    ap.add_argument("--tickets", type=int, default=3)
    ap.add_argument("--max-losses", type=int, default=5)
    args = ap.parse_args()

    now = datetime.now(timezone.utc).timestamp()
    pool = load_pools()
    per_sport: dict[str, int] = {}
    for s in pool:
        per_sport[s["sport"]] = per_sport.get(s["sport"], 0) + 1
    print(f"[tickets] pool: {len(pool)} selections across {len(per_sport)} sports "
          f"({', '.join(f'{k}:{v}' for k, v in sorted(per_sport.items()))})")

    window_pool = [s for s in pool if within_window(s, now, args.window)]
    print(f"[tickets] inside the next {args.window:g}h: {len(window_pool)} selections")

    tickets = build(pool, args.window, args.size, args.tickets, args.max_losses, now)
    for t in tickets:
        f, o = t["flexi"], t["outcomes"]
        print(f"[tickets] {t['name']}: {f['legs']} legs · odds {f['minLegOdds']}-{f['maxLegOdds']} "
              f"· total {f['totalOdds']:,} · return chance {o['anyReturnPct']}% "
              f"· {', '.join(f'{k} {v}' for k, v in t['sportBreakdown'].items())}")

    payload = {
        "generatedAt": datetime.now(WAT).isoformat(timespec="seconds"),
        "windowHours": args.window,
        "maxLosses": args.max_losses,
        "poolSize": len(pool),
        "poolBySport": per_sport,
        "poolInWindow": len(window_pool),
        "tickets": tickets,
        "note": ("Every leg kicks off inside the next 24 hours so the stake can roll over "
                 "the same day. Prices are model estimates: only take a leg if the book's "
                 "price is at or above its break-even, otherwise skip it."),
    }
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, "tickets.json"), "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
        fh.write("\n")
    print(f"[tickets] wrote data/tickets.json ({len(tickets)} tickets)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
