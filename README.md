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
        ├─ scripts/hotstreaks.py --mode full
        │     1. reads FotMob's day feed for the last few days (results, fixtures)
        │     2. fetches per-match stats (shots, SOT, xG, corners, cards, fouls,
        │        throw-ins, tackles, interceptions, saves, possession, offsides)
        │     3. appends new matches to data/history/<season>.jsonl   ← the archive
        │     4. extends the two-season backfill by one chunk (until 2024-08-01)
        │     5. evaluates all 78 run types per team
        │     6. writes data/streaks.json
        │
        └─ git commit + push  →  GitHub Pages serves the new JSON

Browser: index.html fetches ./data/streaks.json (cache-busted) and renders as before
```

The archive is append-only and self-healing: a match is fetched once, stats never
change afterwards, and re-running the pipeline is always safe (it skips what it
already has and only re-fetches the last few days).

### Files

| Path | What it is |
|---|---|
| `index.html` | The dashboard. Same UI as before, but the inline dataset is gone and it loads `data/streaks.json`. |
| `scripts/hotstreaks.py` | The whole pipeline. Standard library only — no dependencies to install. |
| `.github/workflows/daily.yml` | The nightly schedule, manual trigger, and the commit step. |
| `data/streaks.json` | Output consumed by the page (rebuilt every run). |
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

One run like that pulls roughly two seasons for all 28 competitions (~30–60 min on
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
* **The number of runs will be higher** (roughly 12,000–18,000 vs 4,087 today).
  Complete per-match stats mean far more teams qualify for the advanced lines
  (throw-ins, tackles, interceptions) that were patchy before.
* **Team names come from FotMob** ("Sporting CP" where the old file said
  "Sp Lisbon", "R. Racing Club", "M'gladbach"). The page's built-in nickname map
  still rewrites the common ones.
* **Short club codes are recomputed** (initials of the club name, e.g. Bayern
  Munich → `BM`), which matches the old scheme for ~95% of clubs.
* **Cup and European matches** are used for "next match" but do not extend a
  domestic run, same as before.

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
