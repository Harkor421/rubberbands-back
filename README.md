# Rubberbands & More — backend (the drawer)

Hold **$RBM**, get a dollar. One CA drives everything:

- **CA** — our coin (**$RBM**). Holders and eligibility are tracked on it.

The drawer wallet accumulates **native ETH** (creator fees). Every **5
minutes** it walks the holder list from the **biggest bag to the smallest**
and sends each holder **$1 worth of ETH** until the drawer runs out. Native
transfers only — no DEX swap, no reward token, nothing that can revert on a
thin pool. No claiming, ever.

- eligible = holders **≥0.1%** of supply (≤50%); LP pools / routers /
  contracts / zero / dead / the drawer itself excluded automatically
- order = balance DESC — "from the ones that hold more to the ones that
  hold less"
- stop = when the cycle budget (drawer − gas reserve) can't cover another
  $1 + its gas
- the dollar is a real dollar: ETH/USD from Blockscout stats, and a cycle
  **never pays on a stale or unknown price**

Holders + contract flags come from the Blockscout indexer; the RPC only
sends the payouts (throttled to stay under the req/sec cap).

## Run

```bash
npm i
ADMIN_PASSWORD=… DISTRIBUTOR_PRIVATE_KEY=0x… MONGO_URL=… node index.js
```

Key env vars: `CA`, `RPC_URL`, `CHAIN_ID`, `BLOCKSCOUT_URL`,
`PAYOUT_USD` (1), `PAY_EVERY_MS` (300000), `BUDGET_PCT` (100),
`LEAVE_ETH` (0.003), `MIN_ELIGIBLE_PCT` (0.1), `MAX_RECIPIENTS_PER_CYCLE`
(500), `DRY_RUN`, `DRY_POT_ETH`, `AUTO_START`.

## API

All POSTs need the admin password (`"password"` in body, `?password=`, or
`x-admin-password` header). Admin is **disabled** until `ADMIN_PASSWORD` is set.

| Endpoint | Body | Does |
|---|---|---|
| `GET /health` | — | liveness |
| `GET /status` | — | full state |
| `POST /set-ca` | `{ca, password}` | set the $RBM CA and start paying its holders |
| `POST /start` / `POST /stop` | `{ca?, password}` | run control |
| `POST /clear-ca` | `{password}` | wipe the CA + stop |
| `POST /force-pay` | `{password}` | run a payout cycle right now |
| `POST /set-payout` | `{usd, password}` | change the per-holder $ amount (persisted) |

Legacy aliases (`/set-home`, `/set-token`, `/clear-home`, `/clear-token`)
route to the same handlers.

**Launch order:** deploy → fund the drawer wallet → `POST /set-ca {ca}`.
First cycle fires 5 minutes later. `DRY_RUN=true` simulates the whole loop
(fake pot, fake hashes) for testing.

## WebSocket

Broadcasts: `service_status`, `holders_update` (ranked line incl. per-holder
`paidUsd`), `cycle_state` (drawer ETH/USD, countdown, coverage, totals),
`payments_update` (each $1 as it lands), `distribution_result` (cycle recap).
