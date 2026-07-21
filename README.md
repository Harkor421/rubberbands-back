# Rubberbands & More — backend (the drawer)

Hold **$RBM**, get a dollar. The drawer wallet accumulates **native ETH**
(creator fees). Every **5 minutes** it walks the holder list from the
**biggest bag to the smallest** and sends each holder **$1 worth of ETH**
until the drawer runs out. Native transfers only: no DEX swap, no reward
token, nothing that can revert on a thin pool. No claiming, ever.

> **For the next agent:** this file is the runbook. Read "Deployed state"
> and "Launch runbook" before touching anything. The engine is one file,
> `index.js`. The sibling repo `rubberbands-front` is the site that
> consumes this backend's WebSocket.

## Deployed state (as of 2026-07-21)

| Thing | Where |
|---|---|
| Railway project | `rubberbands-back`, workspace "harkor421's Projects" |
| Public URL | `https://rubberbands-back-production.up.railway.app` (HTTP + WS on the same port) |
| MongoDB | Railway service `MongoDB` in the same project, wired via `MONGO_URL` reference variable |
| Drawer wallet | `0xd3C37183A461FbC291bb76F5bA6dAe8dD31007a4` |
| Secrets | `ADMIN_PASSWORD` and `DISTRIBUTOR_PRIVATE_KEY` live **only** in Railway service variables (dashboard → rubberbands-back → Variables) |
| Engine state | real mode (`DRY_RUN` unset), boot-paused, **waiting for `POST /set-ca`** |

Deploy updates with `railway up` from this directory (CLI is logged in as
the owner). A new deployment snapshots the current variables; plain
`railway redeploy` right after changing variables has bitten us before
(see Gotchas).

## Launch runbook

1. **Fund the drawer wallet** with ETH on Robinhood Chain. Payouts AND
   gas come out of it. `LEAVE_ETH` (0.003) is always held back for gas.
2. Launch the $RBM coin (noxa.fun). Copy its CA.
3. Point the engine at it (password from Railway variables):
   ```bash
   curl -X POST https://rubberbands-back-production.up.railway.app/set-ca \
     -H "Content-Type: application/json" \
     -d '{"ca":"0xRBM_CA","password":"ADMIN_PASSWORD"}'
   ```
   First cycle fires 5 minutes later. Holders crawl + ETH price start
   immediately; the engine resumes by itself after every redeploy (CA and
   running flag persist in Mongo).
4. Optional fireworks at launch: `POST /force-pay {password}` runs a
   cycle right now without touching the 5-minute clock.
5. Watch it: `railway logs`, or `GET /status`, or just open the site.

**Rehearsal:** set `DRY_RUN=true` on the service and it simulates the
whole loop (fake pot that refills itself, fake tx hashes, real holder
data, no sends). Unset to go live again.

## How a cycle works (`runPayCycle`, index.js)

1. ETH/USD from Blockscout stats, cached; **no fresh price (≤30 min), no
   cycle**. The dollar is never guessed.
2. Holder snapshot must be **≤10 min old** or the cycle skips (never pay
   over frozen data). Snapshots only ever come from a **complete** crawl.
3. Eligible = holds **≥0.1%** and **≤50%** of supply; contracts, LP
   pools, zero/dead, the drawer itself, and `EXCLUDE_ADDRESSES` are out.
   Sorted by raw balance DESC. That order IS the product: biggest first.
4. Budget = (drawer − `LEAVE_ETH`) × `BUDGET_PCT`%. Per-recipient cost =
   $1 in wei + gasLimit×maxFee (gas limit estimated per cycle, 2×
   headroom). `count = min(budget / perCost, holders, MAX_RECIPIENTS_PER_CYCLE)`.
5. Send loop: plain `wallet.sendTransaction` per holder, throttled RPC,
   nonce-recovery (a timeout / "nonce too low" where the pending nonce
   moved past ours means the tx DID land: record it, never re-send, so a
   holder can never be paid twice). "insufficient funds" aborts the rest
   of the cycle cleanly.
6. Everything is recorded: per-holder totals + every payment to Mongo
   (`payments` collection) and the state file, then broadcast over WS.

Cycles are serialized through a promise chain; `/force-pay` can never
overlap the timer. A CA switch bumps `tokenEpoch`, which aborts in-flight
sends AND stale holder crawls (both are epoch-guarded).

## API

All POSTs need the admin password (`"password"` in body, `?password=`,
or `x-admin-password` header). Admin is **disabled** (fail closed) until
`ADMIN_PASSWORD` is set.

| Endpoint | Body | Does |
|---|---|---|
| `GET /health` | — | liveness |
| `GET /status` | — | full state incl. holders, cycle, totals |
| `POST /set-ca` | `{ca, password}` | set the $RBM CA and start paying its holders |
| `POST /start` / `POST /stop` | `{ca?, password}` | run control |
| `POST /clear-ca` | `{password}` | wipe the CA + stop |
| `POST /force-pay` | `{password}` | run a payout cycle right now |
| `POST /set-payout` | `{usd, password}` | change $/holder at runtime (persisted in Mongo; `PAYOUT_USD` env still wins on boot) |

Aliases kept for muscle memory: `/set-home`, `/set-token`,
`/clear-home`, `/clear-token`.

## WebSocket protocol (what the frontend consumes)

On connect: `service_status`, `holders_update`, `cycle_state`, buffered
`payments_update`, last `distribution_result`. Then live:

- `holders_update` — ranked line: `{holders: [{owner, rank, amount, percentage, rawBalance, paidUsd, paidCount}], totalHolders, eligibleHolders, mint, minEligiblePct}`
- `cycle_state` — `{potEth, potUsd, ethUsd, payoutUsd, payEveryMs, nextPayTs, coverage, paidUsdTotal, paidEthTotal, payoutsTotal, mcapUsd, running, dryRun}` (every 5s)
- `payments_update` — `{payments: [{to, usd, eth, rank, sig, ts}]}` as each $1 lands
- `distribution_result` — cycle recap `{count, totalUsd, totalEth, eligible, coveredThrough, ranOut}`

## Env vars

Required in production: `ADMIN_PASSWORD`, `DISTRIBUTOR_PRIVATE_KEY`,
`MONGO_URL`. Everything else has working defaults for Robinhood Chain.

| Var | Default | Meaning |
|---|---|---|
| `CA` | — | pin the $RBM CA at boot (else set via API, resumes from Mongo) |
| `PAYOUT_USD` | `1` | dollars per holder per cycle |
| `PAY_EVERY_MS` | `300000` | cycle period |
| `BUDGET_PCT` | `100` | % of the spendable drawer used per cycle |
| `LEAVE_ETH` | `0.003` | native held back for gas |
| `MIN_ELIGIBLE_PCT` / `MAX_HOLDER_PCT` | `0.1` / `50` | eligibility band |
| `MAX_RECIPIENTS_PER_CYCLE` | `500` | hard cap per cycle |
| `NATIVE_GAS_LIMIT` | `0` (auto) | fixed send gas limit if set |
| `DRY_RUN` / `DRY_POT_ETH` | `false` / `0.02` | simulate everything |
| `RPC_URL` / `CHAIN_ID` | robinhood mainnet / `4663` | chain |
| `BLOCKSCOUT_URL` / `BLOCKSCOUT_API_KEY` | public instance / — | holders + prices (a key raises rate limits) |
| `RPC_MAX_PER_SEC` | `9` | RPC throttle |
| `ETH_USD_OVERRIDE` | — | emergency fixed price |
| `MONGO_DB` | `rubberbands` | database name (server can be shared) |
| `AUTO_START` | `false` | start at boot when `CA` is pinned |

## Gotchas (learned the hard way)

- **Variables vs deployments (Railway):** a deployment snapshots env at
  creation. After changing variables, `railway up` (fresh deployment),
  not just a restart, or the process keeps the old env.
- **Blockscout 429s:** giant holder lists (e.g. WETH) on the public
  instance rate-limit the crawl. Normal launchpad tokens are 1–3 pages
  and fine at the 15s poll. If crawls fail persistently, add
  `BLOCKSCOUT_API_KEY`. A failed crawl is safe: partial results are
  thrown away and the last good snapshot ages out (cycle skips).
- **The engine does nothing until `/set-ca`** and refuses to pay without
  fresh price + fresh holders. Silence in the logs is usually one of
  those two gates; `/status` shows both timestamps.
- Local dev: `npm i && DRY_RUN=true ADMIN_PASSWORD=x PAY_EVERY_MS=20000 node index.js`,
  then `POST /set-ca` with any real token CA on the chain.

## Files

```
index.js   the whole engine (config → persistence → crawl → price → cycle → API → boot)
```
