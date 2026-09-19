# Hot Streaks — automatic daily data updates

Site: **https://hotstreaks.ymlux.shop** · Repo: `ymborris/hotstreaks` (GitHub Pages)

Before this was set up, every statistics update meant hand-uploading a new 3.2 MB
`index.html` with the whole dataset baked in. Now the page is small and static, the
data lives in `data/streaks.json`, and a GitHub Actions workflow rebuilds it every
night at midnight (WAT). GitHub Pages redeploys automatically on each commit.

---

## How it works

```
GitHub Actions (cron 23:00 UTC = 00:00 WAT, plus 06:00 WAT catch-up)
        │
        ├─ scripts/hotstreaks.py --mode full      (football)
        │     1. reads FotMob's day feed for the last few days (results, fixtures)
        │     2. fetches per-match stats (shots, SOT, xG, corners, cards, fouls,
        │        throw-ins, tackles, interceptions, saves, possession, offsides)
        │     3. appends new matches to data/history/<season>.jsonl   ← the archive
        │     4. extends the two-season backfill by one chunk (until 2024-08-01)
        │     5. evaluates all 78 run types per team
        │     6. writes data/streaks.json (runs · rankings · tickets)
        │
        ├─ scripts/sports.py --mode full     (other sports)
        │     nflverse / MLB / NHL / ESPN  →  data/sports.json
        │
        ├─ scripts/tickets.py                (cross-sport tickets)
        │     both selection pools, next 24 h only  →  data/tickets.json
        │
        └─ git commit + push  →  GitHub Pages serves the new JSON

Browser: index.html fetches ./data/streaks.json + sports.json + tickets.json (cache-busted)
```

The archive is append-only and self-healing: a match is fetched once, stats never
change afterwards, and re-running the pipeline is always safe (it skips what it
already has and only re-fetches the last few days).

### The dashboard tabs

| Tab | What it shows |
|---|---|
| **Streaks** | Everything as before, plus a **kickoff window** selector: next 1 / 3 / 6 / 12 hours, 1 / 3 / 7 / 30 days. With a window active the list sorts by soonest kickoff. The league strip under the sport buttons shows each league's bread and butter — runs, last played, next fixture. **Sort it your way** with the buttons above the list: *Kickoff*, *Longest*, *Most runs* (the team with the most active runs first), *Likely* (highest modelled chance of the run continuing) or *Team*. The Consistency tab carries its own sort menu: *Best 10/10 first*, *Streak*, *Likely*, *Kickoff*, *Team*. |
| **Consistency · 8 of 10** | The second question, asked separately from streaks: which selections landed in **at least 8 of the last 10 games**? A team that won 8 of 10 and then lost twice is here and not in Streaks; a team on a 3-game run that has only hit 3 times in ten is in Streaks and not here. Every market is analysed. Where a team has fewer than ten games on file, the card prints the real denominator (9/9, 8/8) rather than pretending it has ten. |
| **Power rankings** | The runs most likely to continue, weighted towards the strongest competitions (top-5 leagues plus a few more). Shows modelled confidence and the **break-even odds**. |
| **High odds** | Long-running selections the market should price generously (2.20+). Treat as value, not bankers. |
| **Accumulators** | Three tickets that draw on **every** sport, each one built to **at least 15 legs** so SportyBet's **Flexi** option is available. Every leg kicks off within 24 hours, every leg is priced 1.50+, a team never appears twice on a ticket, and no selection is shared between tickets. The card shows legs / matches, the Flexi return arithmetic and a badge saying whether the Flexi minimum is met. |

### Streak vs consistency, and the star

Two different questions, two lists, one star:

| | **Streaks** | **Consistency · 8 of 10** |
|---|---|---|
| Question | Is the run *alive right now*? | Did it *keep landing* across the last ten games? |
| Qualifies | The run is still going, minimum 3 games | At least 8 hits in the last (up to) 10 games |
| Won 8 of 10, then lost twice | ✗ | ✓ |
| Three straight wins inside ten games | ✓ | ✗ (only 3 hits) |
| Starred | The selection also has an 8-in-10 record | The selection is also on a live streak |

A **★** next to a team means both lists carry it — a live streak *and* an 8-in-10 hit
rate. It is drawn in both lists so the double-qualifiers can be spotted at a glance.
One consequence is worth knowing: a 10/10 record *always* implies a live streak of at
least ten, so the very top of the Consistency list is nearly all starred. The entries
that are consistent **without** a live streak are the 9/10 and 8/10 ones whose run has
just broken — the *Streak* sort (or the *Minimum live streak* slider, set to 0) is the
fastest way to see them on their own.

**How the list is capped.** Both builders keep the list readable instead of endless:
at most **900** football selections and **260 per other sport**, at most **8** per team
(football) or **6** (other sports), and inside that cap a **70% starred / 30% unstarred**
split — otherwise a pure "best first" cut would hide every streak-broken 8-in-10 behind
rows of 10/10s.

**"Likely" is a model number, not a promise.** Each entry carries the same model price
the ticket builder uses (`confidence` = modelled probability in percent, with the
break-even odds alongside), taken from the price book for that team and market. When a
selection has no price, it sorts last and simply shows no percentage.

### How the daily tickets are built

`scripts/tickets.py` runs last and reads both selection pools. Rules, in order:

1. **24-hour window.** Only selections whose next match kicks off within the next
   24 hours are eligible — so the day's stake settles the same day and rolls into
   tomorrow. That is the whole point of the window: it is a compounding system, not
   a long-range accumulator.
2. **Best available, any sport.** Each match contributes its strongest qualifying
   selection, so a ticket can mix football, baseball, basketball, tennis and the
   rest — or come out single-sport on a day when one sport dominates. Nothing forces
   a mix.
3. **At least 15 legs per ticket.** That is the floor SportyBet's Flexi option
   wants. The day's matches are dealt out to the three tickets first, strongest
   first, so all three get a fair share of the card; each ticket then takes one leg
   from each of its matches, and a **second leg from a match it already holds**
   when it still needs legs. A second leg is always marked in the table, is capped
   at two per match, and never happens at another ticket's expense.
4. **Flexi-legal markets only.** SportyBet will not take stat markets on a Flexi
   slip, so every leg has to come from a market it does accept: favourites / 1X2,
   over-under goals, both teams to score, corners, bookings, shots on target and
   handicaps. Match shots, throw-ins, fouls, tackles, offsides, keeper saves and
   possession are excluded before the tickets are built — the run prints how many
   selections that dropped. The rest of the site (streaks, rankings, high odds)
   still tracks every market; the restriction applies to the tickets only.
5. **Leg bands.** Ticket A takes 1.50–1.62 (bankers), B 1.52–1.72 (balanced),
   C 1.55–1.85 (value). Nothing below 1.50, since a shorter leg just dilutes a
   15-leg ticket. If a band runs dry the builder widens it, then drops the
   per-competition cap, then takes anything above the 1.50 floor.
6. **Independence.** No team twice on one ticket and no selection on two tickets —
   a bad result can only hurt one ticket.
7. **Flexi arithmetic.** Expect around three to five losing legs on a 15-leg
   ticket; the "return if exactly 5 lose" figure is what the ticket pays in that
   case, and "chance of a Flexi return at max 5 losses" is the modelled probability
   that at least one combination comes back.

Place a leg only when the book's price is at or above the printed break-even
(`1 ÷ modelled probability`). Below break-even the ticket is value-negative even
when it wins.

### How the prices are made (read this before staking)

No free source publishes live prices for the markets this dashboard tracks, so the
numbers are **model estimates**, not bookmaker quotes:

```
probability = (times the condition happened in the last 20 games + 6 × competition average)
              ÷ (games + 6)                                  ← shrunk to the mean
probability is capped at 90%                                 ← no selection is a certainty
break-even  = 1 ÷ probability              ← the MINIMUM price worth taking
est. odds   = break-even × (1 − 5.5%)      ← what a book is likely to offer
```

**The important consequence:** a leg only has value if SportyBet's actual price is
**at or above its break-even**. The accumulator tabs print the break-even next to
every leg for exactly that reason, along with the ticket's average break-even. If
the book offers less, skip that leg.

The Flexi figures (total odds, worst-case surviving odds, chance of a return) are
arithmetic on those modelled prices — they are not SportyBet's internal split, so
check their calculator before staking. A 15-leg ticket at ~1.50 per leg needs about
6 legs to fail to break even on average, which is why the "chance of a return at
max 5 losses" figure is shown per ticket and the ticket carries a badge when it
clears the 15-leg Flexi minimum.

### Other sports

| Sport | Leagues | Source | Notes |
|---|---|---|---|
| American football | **NFL, NCAA** | nflverse `games.csv` (NFL) + ESPN (college) | NFL is one request for the whole archive, including closing **spread and total lines**; college is walked day by day |
| Basketball | **NBA, WNBA, NCAA men, NCAA women** | ESPN scoreboard | one request per date per league; falls back to the ESPN core API |
| Baseball | MLB | `statsapi.mlb.com` (official MLB) | day schedules, walked back chunk by chunk |
| Ice hockey | NHL | `api-web.nhle.com` (official NHL) | day schedules, and the NHL's weekly payload is filtered to the requested day |
| Tennis | **ATP, WTA** | ESPN scoreboard | every singles match of the day with set scores; men's and women's draws are read separately |

**Kickoff times.** nflverse publishes its kickoff time in US Eastern, not UTC.
Stamp it as UTC and every NFL game appears about four hours early — which put the
Thursday night game on the wrong day (the 20:15 ET kickoff is 01:15 WAT the next
morning, so the page said "today" for a game that had not started). Kickoffs are
converted properly now, and stored history is re-dated once on the next run (the
fix is marked on each row, so it never runs twice).

The same rule applies everywhere: **the day a match is shown under is its WAT
date, derived from the source's UTC kickoff**, never from the date bucket the
requested feed was queried with. College and NBA/WNBA basketball come from the ESPN
scoreboard as UTC timestamps, so an evening game in the US state that tips off
after 23:00 UTC lands on the next WAT day — which is exactly what a reader in
Lagos should see.

**Today means today.** A run is badged `TODAY` only when its next kickoff's **WAT
date equals today's WAT date**. If Sofascore shows a college game tomorrow, this
page shows tomorrow too.

**Leagues per sport.** Each sport reads one or more leagues, and every league is
counted and priced on its own (college basketball is not priced with NBA numbers).
Under the sport chips the page shows one line per league: games stored, runs found,
and when it plays next — so a league that is out of season says so instead of
quietly disappearing. College basketball and college football both start in
November; their chips fill in automatically when games resume.

*(The ESPN host matters: `site.api.espn.com` answers 403 from datacentre IPs —
sandbox and GitHub runners alike. `site.web.api.espn.com` serves the same payload on
an open host, and `sports.core.api.espn.com` is the backup, so the pipeline does not
depend on any one of them. Table tennis has no keyless source left, so its chip was
removed rather than left showing "coming soon".)*

Each sport gets a handful of run types suited to its scoring:

| Sport | Run types |
|---|---|
| American football | result runs, team points (20/27/30), defence (allows under 20), match totals, NFL spread and total-line runs, plus stat lines: **forces 2+ turnovers, 300+ passing yards, 450+ total yards** |
| Basketball | result runs, team points (100/110/120), match totals, plus **15+ forced turnovers, 40+ rebounds, 25+ assists** |
| Baseball | result runs, team runs (4+/5+), defence (3 or fewer), match totals |
| Ice hockey | result runs, team goals (3+/4+), defence (2 or fewer), match totals |
| Tennis | result runs, unbeaten runs, straight-sets wins |

The stat-line types need one extra request per game; the pipeline spends its stat
budget on the **most recent** games first, so current streaks get them immediately
and older games fill in over the following nights.
Sports whose source is unreachable are marked **(soon)** in the UI and say why; the
pipeline simply retries on the next run and they light up by themselves.

To add another competition or sport, add an entry to `SOURCE` / `TYPES` in
`scripts/sports.py` — the run engine, history store and UI work off the same shape.

### What is tracked

65 domestic competitions plus the three UEFA club competitions:

| Competition | Source ids | Notes |
|---|---|---|
| UEFA Champions League | 42 (+10611 qualifying) | league phase and qualifying merged |
| UEFA Europa League | 73 (+10613) | |
| UEFA Conference League | 10216 (+10615) | |
| Russian Premier League | 63 | |
| Ukrainian Premier League | 441 | |
| HNL (Croatia) | 252 | |
| Nemzeti Bajnokság I (Hungary) | 212 | |
| Eerste Divisie (Netherlands) | 111 | second tier, below the Eredivisie |
| Slovak Super Liga | 176 | |
| Prva Liga (Slovenia) | 173 | |
| First Professional League (Bulgaria) | 270 | |
| Cypriot First Division | 136 | |
| Egyptian Premier League | 519 | |
| Algerian Ligue 1 | 516 | |
| Bosnian Premier League | 267 | |
| Belarusian Premier League | 263 | |
| Cymru Premier (Wales) | 116 | |
| NIFL Premiership (Northern Ireland) | 129 | |
| Serbian Super Liga | 182 | |
| Danish Superliga | 46 | country-qualified: Romania's top flight is also "Superliga" |
| Czech First League | 122 | |
| Saudi Pro League | 536 | |
| Israeli Premier League | 127 | FotMob calls it Ligat ha'Al |
| Persian Gulf Pro League (Iran) | 523 | |
| Colombian Primera A | 274 | |
| Premier Soccer League (South Africa) | 537 | |
| UAE Pro League | 538 | |
| Besta deildin (Iceland) | 215 | |
| Erovnuli Liga (Georgia) | 439 | |
| Kategoria Superiore (Albania) | 260 | |
| Macedonian Prva Liga | 249 | country-qualified: Slovenia's is also "Prva Liga" |
| Virsliga (Latvia) | 226 | |
| Premium liiga (Estonia) | 248 | |
| A Lyga (Lithuania) | 228 | |
| Iraqi Stars League | 524 | |
| Armenian Premier League | 118 | |
| Faroese Premier League | 250 | |

Kosovo is the one league that could not be added: FotMob's league index covers 94
countries and Kosovo is not among them, so there is no Kosovar top flight to read
from this source.

European matches are stored as their **own** competitions, so they never bleed into
a team's domestic runs — Arsenal has a "no-draw run" for the Premier League and a
separate one for the Champions League. Cards show `UEFA Champions League · Europe`.

The `next match` panel still shows the soonest fixture in *any* competition (so a
domestic card can point at an upcoming European night), and it looks 25 days ahead
so European matchdays — roughly three weeks apart — are covered.

Labels are country-qualified where the competition's own name would collide with
one already tracked (FotMob calls four different leagues "Premier League", and
Algeria's top flight is also "Ligue 1"), because the dashboard keys each
competition by its label.

Adding a competition is one line in the `LEAGUES` list in `scripts/hotstreaks.py`
— id, label, country. The pipeline notices the tracked set changed, re-walks the
stored history for the new competition and fills it in over the following runs, so
nothing has to be backfilled by hand. Copa Libertadores, Copa Sudamericana and the
CONCACAF Champions Cup are one line each the same way.

### Files

| Path | What it is |
|---|---|
| `index.html` | The dashboard. Same UI as before, but the inline dataset is gone and it loads `data/streaks.json`. |
| `scripts/hotstreaks.py` | Football pipeline: fixtures, stats, streaks, model prices, rankings, high odds. Standard library only. |
| `scripts/sports.py` | The other sports: sources, history, streaks and model prices for NFL + NCAA football, NBA + WNBA + NCAA basketball, ATP + WTA tennis, MLB and NHL. |
| `scripts/tickets.py` | Merges every sport's priced selections into the day's three tickets (`data/tickets.json`). |
| `data/tickets.json` | The three tickets the page renders. |
| `data/selections_football.json`, `data/selections_sports.json` | The priced selection pools the ticket builder reads. |
| `.github/workflows/daily.yml` | The nightly schedule, manual trigger, and the commit step. The sanity block now also refuses to deploy a bad consistency list: it checks `statsCount` matches the rows, that every row is 8-of-10-or-better (`hits >= 8`, `hits <= games <= 10`), that each sport reported its own list, and it warns (without failing) when the list is thinner than expected. |
| `data/streaks.json` | Output consumed by the page (rebuilt every run). Carries `streaks` (the runs) and `stats` (`statsCount` entries = the 8-of-10 list, each with `hits`, `games`, `recent`, `streak` and the model `confidence`). Every streak row also carries `confidence`, `fairOdds` and `record`, and `teamId` so the two lists can be matched exactly. |
| `data/history/*.jsonl` | Match archive, one JSON object per match, one file per season. |
| `data/state.json` | Backfill cursor, last run time, pending-stats counter. |
| `data/venue_cache.json` | Stadium names for upcoming fixtures (avoids re-fetching). |

### Schedule

| Cron (UTC) | Local time (WAT) | Purpose |
|---|---|---|
| `0 23 * * *` | 00:00 | The midnight update |
| `0 5 * * *` | 06:00 | Catch-up: MLS / Liga MX matches that finish after midnight, and GitHub's scheduler delays |

You can also run it on demand: **Actions → Daily streaks update → Run workflow**
(mode `full` for a normal run, `backfill` to pull history faster, `rebuild` to
recompute from the archive without touching the network).

---

## Install (one-off)

Everything in this folder goes into the repo root, keeping the same paths.

### Option A — with a terminal (fastest)

```bash
git clone https://github.com/ymborris/hotstreaks
cd hotstreaks
# copy index.html, scripts/, .github/, data/, README.md into this folder, then:
git add -A
git commit -m "Add automatic daily streak updates"
git push
```

### Option B — GitHub web UI (no tools needed)

1. **Replace `index.html`** — repo → *Add file → Upload files* → drag `index.html` → *Commit changes* (same name = overwrite).
2. **Add the script** — *Add file → Create new file* → name it `scripts/hotstreaks.py` → paste the file contents → *Commit changes*.
3. **Add the workflow** — *Add file → Create new file* → name it `.github/workflows/daily.yml` → paste contents → *Commit changes*. (Typing the slashes creates the folders.)
4. **Add the data folder** — *Add file → Upload files* → drag the whole `data` folder (it keeps the folder structure) → *Commit changes*.
5. **Check the Action** — *Actions* tab → the workflow should appear. Press *Run workflow* once to verify it completes green.
6. **Check the site** — hard-refresh https://hotstreaks.ymlux.shop (Ctrl/Cmd+Shift+R). The subtitle should show a fresh *as of* date.

> Step 4 is optional. Without the archive the site still works immediately: each
> nightly run pulls ~8,000 matches, so history grows backwards at roughly **two
> months per run** until it reaches 2024-08-01 (about four runs, i.e. two days of
> scheduled runs). Uploading the archive just skips that ramp-up and gives full
> two-season depth from the first load.

### Building the whole archive in one run (optional)

If you'd rather not upload 15 MB of history, let GitHub build it for you:

> **Actions → Daily streaks update → Run workflow →** mode `full`,
> max_details `30000`, max_minutes `300`

One run like that pulls roughly two seasons for all 31 competitions (~30–60 min on
a GitHub runner) and commits everything. After it finishes, drop max_details back
to the default and the scheduled runs just maintain the data.

### Settings that matter

* *Settings → Actions → General → Workflow permissions* → **Read and write permissions** (the workflow commits data back to the repo; the workflow file also sets `contents: write`, but if commits fail with a 403 this is the switch to flip).
* Pages stays as it is: deploy from branch `main`, root folder.

---

## What changed in the numbers

The previous dataset was assembled from three different sources and had gaps and
duplicates (e.g. Benfica's run showed two September fixtures days apart, and some
MLS matches appeared twice). This pipeline uses one consistent source, so expect:

* **Some run lengths will change** — usually upward, because matches that were
  missing before are now present. Nothing is "broken"; the counts are simply
  measured on a complete fixture list.
* **The number of runs is higher** (roughly 15,000–20,000 once the archive is
  full, vs 4,087 originally) — partly complete stats, partly the European runs.
  Complete per-match stats mean far more teams qualify for the advanced lines
  (throw-ins, tackles, interceptions) that were patchy before.
* **Team names come from FotMob** ("Sporting CP" where the old file said
  "Sp Lisbon", "R. Racing Club", "M'gladbach"). The page's built-in nickname map
  still rewrites the common ones.
* **Short club codes are recomputed** (initials of the club name, e.g. Bayern
  Munich → `BM`), which matches the old scheme for ~95% of clubs.
* **European matches now count** as their own competitions (previously they only
  supplied the "next match" panel). Adding them triggers a one-off re-walk of the
  archive — stored matches are skipped, only European ones are fetched.

---

## Housekeeping

`data/streaks.json` is rewritten on every run, so each daily commit adds a new
version of it to the repo's history (Git compresses and delta-packs it, so growth
is far smaller than the file size suggests — but not zero). If the repo ever feels
heavy, either:

* run `git gc --aggressive --prune=now` locally and force-push once, or
* ask me to switch the output to `data/streaks.json.gz` (the page would decompress
  it in the browser with `DecompressionStream`). That cuts committed size ~8×.

The match archive (`data/history/*.jsonl`) is append-only and tiny in comparison.

## Tuning

Everything is a flag on the script:

```bash
python3 scripts/hotstreaks.py --mode full \
  --days 4 \            # recent days re-checked each run
  --chunk-days 21 \     # history pulled per run while backfilling
  --max-details 8000 \  # per-run cap on stats requests
  --max-minutes 240 \   # hard wall-clock budget
  --venue-budget 400    # stadium lookups per run
```

Change the schedule or the budgets by editing `.github/workflows/daily.yml`.

Respectful use of the free data source: requests are throttled, retried with
backoff, and every finished match is only ever fetched once.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Page shows "Could not load data/streaks.json …" | The file is missing or renamed. Run the workflow once (*Actions → Run workflow*) and confirm `data/streaks.json` exists in the repo. |
| Subtitle date is stale | Check the *Actions* tab. A red run means the data source was unreachable; the next scheduled run retries, and the previous data stays online meanwhile. |
| Commits fail with 403 | *Settings → Actions → General → Workflow permissions* → "Read and write permissions". |
| Runs are still short after the first day | Backfill is still working backwards (~2 months per run). Use the one-shot recipe above (max_details 30000) or upload the archive. |
| Want it more/less frequent | Edit the `cron:` lines in `.github/workflows/daily.yml` (times are UTC; WAT = UTC+1). |
| First run can't reach FotMob | The runner is probably blocked by the data source. Re-run it; if it persists, tell me and I'll switch the fetch layer to the keyless ESPN endpoints (same fields except throw-ins). |
