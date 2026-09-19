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

# name, odds band, max legs per competition, selection style.
# No cap on legs per sport: with a 15-leg Flexi ticket the sport mix falls out of
# whatever is actually kicking off, and a football-heavy day should produce a
# football-heavy ticket.
TICKET_PLANS = [
    ("Ticket A · bankers",  (1.50, 1.62), 2, "confidence"),
    ("Ticket B · balanced", (1.52, 1.72), 2, "mixed"),
    ("Ticket C · value",    (1.55, 1.85), 2, "odds"),
]

MIN_LEG_ODDS = 1.50          # a shorter leg only dilutes the ticket
COMP_CAP_LADDER = (2, 3, 5, 999)   # relaxed only to reach the minimum

# ---------------------------------------------------------------------------
# Flexi market policy
#
# SportyBet will not accept stat markets on a Flexi slip: match shots, throw-ins,
# fouls, tackles and the like are not offered there. Every leg therefore has to be
# a market that is actually popular on the slip - 1X2 / favourites, over-under
# goals, both teams to score, corners, bookings, shots on target and handicaps.
#
# Football market ids come from scripts/hotstreaks.py, the rest from
# scripts/sports.py. Anything not listed here is skipped when the tickets are
# built; the run reports exactly what it dropped and why.
# ---------------------------------------------------------------------------
FLEXI_MARKETS = {
    "football":   {"goals", "corners", "cards", "handicap"},
    "amfootball": {"result", "total", "spread"},
    "basketball": {"result", "total", "spread"},
    "baseball":   {"result", "total", "spread"},
    "icehockey":  {"result", "total", "spread"},
    "tennis":     {"result", "sets"},
    "tabletennis": {"result"},
}

# a few type ids are allowed even though their market is not: shots on target is
# a normal slip market, the rest of the "shots" bucket (total shots, xG) is not
FLEXI_TYPE_ALLOW = {"team_sot_o45", "team_sot_o35"}

# what the page tells the reader
MARKET_POLICY_NOTE = ("Only markets SportyBet accepts on a Flexi slip: favourites/1X2, "
                      "over-under goals, both teams to score, corners, bookings, shots on "
                      "target and handicaps. Stat markets (match shots, throw-ins, fouls, "
                      "tackles, offsides) are excluded.")


def flexi_ok(sel: dict) -> bool:
    """Is this selection a market SportyBet takes on a Flexi ticket?"""
    sport = sel.get("sport") or "football"
    if sel.get("type") in FLEXI_TYPE_ALLOW:
        return True
    return (sel.get("market") or "") in FLEXI_MARKETS.get(sport, set())


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
          max_losses: int, now: float, min_size: int = 15,
          max_per_match: int = 2) -> list[dict]:
    """Three independent tickets, each filled to at least `min_size` legs.

    Step 1: every match that kicks off inside the window is scored and dealt out to
    the three tickets in rounds, so all of them get a fair share of the day's card.
    Step 2: each ticket fills its legs from the matches it was dealt - the best
            selection per match first, then a second selection from a match it holds
            if it still needs legs. One team per ticket, one selection per pool.

    Relaxation when a band is empty: the per-competition cap, then the odds band,
    then the band entirely - never below MIN_LEG_ODDS.
    """
    fresh = [s for s in pool if within_window(s, now, window)]
    by_fixture: dict[str, list[dict]] = {}
    for sel in fresh:
        # key on the actual match, not the streak id: the two sides of one game
        # arrive as separate selections
        pair = "|".join(sorted([sel.get("team", ""), sel.get("opponent", "")]))
        by_fixture.setdefault(f"{sel['sport']}:{pair}:{sel.get('ts')}", []).append(sel)
    if not by_fixture:
        return []

    day = datetime.now(WAT).strftime("%Y-%m-%d")
    plans = TICKET_PLANS[:tickets]

    def leg_rank(plan, sel, cap=9999, comps=None, teams=None):
        """Lower is better. In-band legs beat out-of-band ones, then the plan's style."""
        _name, (lo, hi), _c, style = plan
        in_band = lo <= sel["odds"] <= hi
        if style == "odds":
            return (0 if in_band else 1, -sel["odds"], -sel["confidence"])
        if style == "mixed":
            return (0 if in_band else 1, sel["sport"], -sel["confidence"])
        return (0 if in_band else 1, -sel["confidence"], sel["ts"])

    # ---- step 1: deal matches, round by round, so no ticket is starved
    dealt: dict[int, list[str]] = {i: [] for i in range(len(plans))}
    remaining = list(by_fixture)
    while remaining:
        progressed = False
        for ti, plan in enumerate(plans):
            best = None
            for mkey in remaining:
                group = [x for x in by_fixture[mkey] if x["odds"] >= MIN_LEG_ODDS]
                if not group:
                    continue
                pick = min(group, key=lambda x: leg_rank(plan, x))
                key = leg_rank(plan, pick)
                if best is None or key < best[0]:
                    best = (key, mkey)
            if best is None:
                continue
            dealt[ti].append(best[1])
            remaining.remove(best[1])
            progressed = True
        if not progressed:
            break

    used_types: set[tuple] = set()      # one selection lives in one ticket
    global_used: dict[str, int] = {}     # legs taken from a match, across tickets
    out = []
    for ti, plan in enumerate(plans):
        _name, (lo, hi), _c, style = plan
        legs: list[dict] = []
        comps: dict[str, int] = {}
        sports: dict[str, int] = {}
        teams: set[tuple] = set()
        per_match: dict[str, int] = {}

        def add(mkey, sel):
            sp, lg = sel["sport"], sel.get("league") or ""
            per_match[mkey] = per_match.get(mkey, 0) + 1
            global_used[mkey] = global_used.get(mkey, 0) + 1
            comps[lg] = comps.get(lg, 0) + 1
            sports[sp] = sports.get(sp, 0) + 1
            teams.add((sp, sel["team"]))
            used_types.add((sp, sel["team"], sel["type"]))
            when = datetime.fromtimestamp(sel["ts"], WAT)
            legs.append({
                "sameMatch": per_match[mkey] > 1,
                "sport": sp, "sportLabel": SPORT_LABEL.get(sp, sp),
                "league": lg, "headline": sel.get("headline", ""),
                "team": sel.get("team", ""), "opponent": sel.get("opponent", ""),
                "type": sel.get("type", ""), "typeLabel": sel.get("typeLabel", ""),
                "market": sel.get("market", ""),
                "run": sel.get("run", 0), "form": sel.get("form", ""),
                "confidence": sel.get("confidence"), "record": sel.get("record", ""),
                "odds": sel["odds"], "fairOdds": sel.get("fairOdds"),
                "ts": sel["ts"], "date": sel.get("date"),
                "kickoff": when.strftime("%a %d %b · %H:%M"),
            })

        def eligible(mkey, band_lo, band_hi, cap):
            got = []
            for sel in by_fixture[mkey]:
                sp = sel["sport"]
                if not (band_lo <= sel["odds"] <= band_hi) or sel["odds"] < MIN_LEG_ODDS:
                    continue
                if (sp, sel["team"]) in teams:
                    continue
                if (sp, sel["team"], sel["type"]) in used_types:
                    continue
                if comps.get(sel.get("league") or "", 0) >= cap:
                    continue
                got.append(sel)
            return got

        # ---- step 2: one leg (then a second) from each match this ticket holds
        my_matches = list(dealt[ti])
        for _pass in range(size):
            if len(legs) >= size:
                break
            progressed = False
            for mkey in my_matches:
                if len(legs) >= size:
                    break
                if per_match.get(mkey, 0) >= max_per_match:
                    continue
                if per_match.get(mkey, 0) == 0 and len(legs) >= 1 and False:
                    continue
                got = []
                for cap in COMP_CAP_LADDER:
                    got = eligible(mkey, lo, hi, cap)
                    if got:
                        break
                if not got:
                    for cap in COMP_CAP_LADDER[:2]:
                        got = eligible(mkey, MIN_LEG_ODDS, max(hi + 0.35, 2.5), cap)
                        if got:
                            break
                if not got:
                    got = eligible(mkey, MIN_LEG_ODDS, 99.0, COMP_CAP_LADDER[0])
                if not got:
                    # before taking a second leg, let every other match have its first
                    if per_match.get(mkey, 0) >= 1:
                        continue
                    got = eligible(mkey, MIN_LEG_ODDS, 99.0, 9999)
                if not got:
                    continue
                best = min(got, key=lambda x: leg_rank(plan, x))
                add(mkey, best)
                progressed = True
            if not progressed:
                break

        # ---- step 3: borrow more of the day's card if the ticket is still short.
        # The matches were dealt out in step 1, so a second leg often has to come
        # from a match another ticket already holds - never more than max_per_match
        # legs per match in total, so two tickets never share the same fixture twice.
        if len(legs) < min_size:
            def borrow_rank(mkey):
                # own matches first, then whatever has the fewest legs taken
                return (0 if mkey in my_matches else 1, global_used.get(mkey, 0))
            for mkey in sorted(by_fixture, key=borrow_rank):
                if len(legs) >= min_size:
                    break
                if global_used.get(mkey, 0) >= max_per_match:
                    continue
                got = eligible(mkey, lo, hi, 9999)
                if not got:
                    got = eligible(mkey, MIN_LEG_ODDS, 99.0, 9999)
                if not got:
                    continue
                add(mkey, min(got, key=lambda x: leg_rank(plan, x)))
                if mkey in remaining:
                    remaining.remove(mkey)

        if not legs:
            continue
        legs.sort(key=lambda l: l["ts"])
        out.append({
            "id": f"{day}-T{ti + 1}",
            "name": _name,
            "windowHours": window,
            "minLegs": min_size,
            "meetsMinimum": len(legs) >= min_size,
            "distinctMatches": len(per_match),
            "sharedLegs": sum(1 for l in legs if l.get("sameMatch")),
            "flexi": flexi_maths(legs, max_losses),
            "outcomes": ticket_outcomes(legs, max_losses),
            "sports": sorted(sports, key=lambda x: -sports[x]),
            "sportBreakdown": {SPORT_LABEL.get(k, k): v for k, v in
                               sorted(sports.items(), key=lambda x: -x[1])},
            "leagues": sorted(comps, key=lambda x: -comps[x]),
            "legs": legs,
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=float, default=24.0, help="hours until the last leg kicks off")
    ap.add_argument("--size", type=int, default=16, help="maximum legs per ticket")
    ap.add_argument("--min-size", type=int, default=15,
                    help="minimum legs per ticket (SportyBet Flexi needs the full slip)")
    ap.add_argument("--max-per-match", type=int, default=2,
                    help="most legs one match may contribute to a single ticket")
    ap.add_argument("--tickets", type=int, default=3)
    ap.add_argument("--max-losses", type=int, default=5)
    args = ap.parse_args()

    now = datetime.now(timezone.utc).timestamp()
    raw_pool = load_pools()
    dropped: dict[str, int] = {}
    for s in raw_pool:
        if not flexi_ok(s):
            key = f"{s.get('sport')}/{(s.get('market') or '?')}"
            dropped[key] = dropped.get(key, 0) + 1
    pool = [s for s in raw_pool if flexi_ok(s)]
    per_sport: dict[str, int] = {}
    for s in pool:
        per_sport[s["sport"]] = per_sport.get(s["sport"], 0) + 1
    print(f"[tickets] pool: {len(raw_pool)} selections across all markets "
          f"-> {len(pool)} usable on a Flexi slip "
          f"({', '.join(f'{k}:{v}' for k, v in sorted(per_sport.items()))})")
    if dropped:
        top = sorted(dropped.items(), key=lambda kv: -kv[1])[:8]
        print(f"[tickets] excluded {sum(dropped.values())} stat-market selections "
              f"SportyBet blocks on Flexi: " + ", ".join(f"{k} {v}" for k, v in top))

    window_pool = [s for s in pool if within_window(s, now, args.window)]
    print(f"[tickets] inside the next {args.window:g}h: {len(window_pool)} selections")

    tickets = build(pool, args.window, args.size, args.tickets, args.max_losses, now,
                    min_size=args.min_size, max_per_match=args.max_per_match)
    for t in tickets:
        f, o = t["flexi"], t["outcomes"]
        flag = "" if t["meetsMinimum"] else "  << below the Flexi minimum"
        print(f"[tickets] {t['name']}: {f['legs']} legs from {t['distinctMatches']} matches "
              f"· odds {f['minLegOdds']}-{f['maxLegOdds']} · total {f['totalOdds']:,} "
              f"· return chance {o['anyReturnPct']}% "
              f"· {', '.join(f'{k} {v}' for k, v in t['sportBreakdown'].items())}{flag}")

    payload = {
        "generatedAt": datetime.now(WAT).isoformat(timespec="seconds"),
        "windowHours": args.window,
        "maxLosses": args.max_losses,
        "minLegs": args.min_size,
        "maxLegs": args.size,
        "matchesInWindow": len({("|".join(sorted([s.get("team", ""), s.get("opponent", "")])),
                                 s.get("ts")) for s in window_pool}),
        "poolSize": len(raw_pool),
        "poolFlexiSize": len(pool),
        "excludedMarkets": dict(sorted(dropped.items(), key=lambda kv: -kv[1])),
        "marketPolicy": MARKET_POLICY_NOTE,
        "poolBySport": per_sport,
        "poolInWindow": len(window_pool),
        "tickets": tickets,
        "note": (f"Each ticket is filled to at least {args.min_size} legs so the full slip can "
                 f"go on SportyBet's Flexi option. Every leg kicks off inside the next "
                 f"{args.window:g} hours, no match or team is used twice, and the tickets do not "
                 f"share legs. " + MARKET_POLICY_NOTE + " Prices are model estimates: only take "
                 f"a leg if the book's price is at or above its break-even, otherwise skip it."),
    }
    os.makedirs(DATA, exist_ok=True)
    with open(os.path.join(DATA, "tickets.json"), "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
        fh.write("\n")
    print(f"[tickets] wrote data/tickets.json ({len(tickets)} tickets)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
