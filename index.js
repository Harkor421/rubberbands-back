// index.js — Rubberbands & More (Robinhood Chain) — the $1 dispenser
//
// Hold $RBM, get a dollar. One CA:
//   • CA (POST /set-ca) — our coin; holders + eligibility are tracked on it.
// The drawer wallet accumulates ETH (creator fees). Every 5 minutes it walks
// the holder list from the BIGGEST bag to the smallest and sends each holder
// $1 worth of native ETH until the drawer runs out:
//
//   • payout_i = $PAYOUT_USD (default $1) of ETH, native transfer — no claiming
//   • order    = holders ranked by balance DESC ("the ones that hold more
//                to the ones that hold less")
//   • stop     = when the cycle budget (drawer − gas reserve) can't cover
//                another payout + its gas
//   • eligible = holders ≥0.1% of supply, ≤50%; LP pools / routers /
//                contracts / zero / dead / the drawer itself excluded.
//
// No reward-token swap, no vault, no milestones — a native send can't revert
// on a thin pool, so the distribution always works if the drawer has ETH.
// ETH/USD comes from the Blockscout stats API (last-known-good cached; a
// cycle never pays on an unknown or stale price). Holders + contract flags
// come from the Blockscout indexer; the RPC only sends the payouts.
//
// npm i ethers express ws axios mongodb
import express from "express";
import http from "http";
import fs from "fs";
import path from "path";
import { WebSocketServer, WebSocket } from "ws";
import axios from "axios";
import { ethers } from "ethers";
import { MongoClient } from "mongodb";

/* ============================== CONFIG ============================== */
const PORT = Number(process.env.PORT || 8080);
const RPC_URL = process.env.RPC_URL || "https://rpc.mainnet.chain.robinhood.com";
const CHAIN_ID = Number(process.env.CHAIN_ID || 4663);
const BLOCKSCOUT_URL = (process.env.BLOCKSCOUT_URL || "https://robinhoodchain.blockscout.com").replace(/\/$/, "");
const BLOCKSCOUT_API_KEY = process.env.BLOCKSCOUT_API_KEY || "";
// Public Blockscout (no key) — used for the ETH/USD price + holder-crawl fallback.
const BLOCKSCOUT_PUBLIC = (process.env.BLOCKSCOUT_PUBLIC || "https://robinhoodchain.blockscout.com").replace(/\/$/, "");

let TOKEN = process.env.CA || process.env.HOME_CA || process.env.TOKEN || null; // $RBM — holders + eligibility live here
const DISTRIBUTOR_PRIVATE_KEY = process.env.DISTRIBUTOR_PRIVATE_KEY || "";

// The dispenser
let PAYOUT_USD = Number(process.env.PAYOUT_USD || 1);              // $ per holder per cycle (runtime: POST /set-payout)
const PAYOUT_USD_PINNED = process.env.PAYOUT_USD != null;          // env wins over Mongo on boot
const PAY_EVERY_MS = Number(process.env.PAY_EVERY_MS || 5 * 60_000);
const BUDGET_PCT = Number(process.env.BUDGET_PCT || 100);          // % of the spendable drawer used per cycle
const LEAVE_WEI = ethers.parseEther(process.env.LEAVE_ETH ?? "0.003"); // native always kept back for gas
const NATIVE_GAS_LIMIT = BigInt(process.env.NATIVE_GAS_LIMIT || 0);    // 0 = estimate on the first recipient
const MAX_RECIPIENTS_PER_CYCLE = Number(process.env.MAX_RECIPIENTS_PER_CYCLE || 500);
// Refuse to pay on an ETH/USD price older than this (never guess the dollar).
const PRICE_STALE_MS = Number(process.env.PRICE_STALE_MS || 30 * 60_000);
const ETH_USD_OVERRIDE = process.env.ETH_USD_OVERRIDE ? Number(process.env.ETH_USD_OVERRIDE) : null;

// Eligibility (same proven model as the Tendies fryer)
const MIN_ELIGIBLE_PCT = Number(process.env.MIN_ELIGIBLE_PCT || 0.1);
const MAX_HOLDER_PCT = Number(process.env.MAX_HOLDER_PCT || 50);
const EXCLUDE_ADDRESSES = new Set(
  (process.env.EXCLUDE_ADDRESSES || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean)
);
const EXCLUDE_CONTRACTS = String(process.env.EXCLUDE_CONTRACTS ?? "true") === "true";
const HOLDERS_POLL_MS = Number(process.env.HOLDERS_POLL_MS || 15_000);
// Refuse to distribute over a holder snapshot older than this (indexer outage protection).
const HOLDERS_STALE_MS = Number(process.env.HOLDERS_STALE_MS || 10 * 60_000);

// Display-only polling
const PRICE_POLL_MS = Number(process.env.PRICE_POLL_MS || 30_000);   // ETH/USD + mcap
const POT_POLL_MS = Number(process.env.POT_POLL_MS || 30_000);       // drawer balance

// Payout shape
const DRY_RUN = String(process.env.DRY_RUN || "false") === "true";
const DRY_POT_ETH = Number(process.env.DRY_POT_ETH || 0.02);         // simulated drawer seed
const SEND_DELAY_MS = Number(process.env.SEND_DELAY_MS || 250);

const STATE_FILE = process.env.STATE_FILE || path.join(process.cwd(), "rbm-state.json");
const MONGO_URL = process.env.MONGO_URL || process.env.MONGODB_URI || process.env.DATABASE_URL || "";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "";

const ERC20_ABI = [
  "function totalSupply() view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function balanceOf(address) view returns (uint256)",
];
const ZERO = "0x0000000000000000000000000000000000000000";
const DEAD = "0x000000000000000000000000000000000000dead";

/* =========================== CONNECTIONS =========================== */
const provider = RPC_URL ? new ethers.JsonRpcProvider(RPC_URL, CHAIN_ID) : null;
// The RPC (QuickNode) caps req/sec. The payout loop bursts past it, so route
// EVERY RPC call through one serialized queue with a minimum gap → we never
// trip "15/second limit reached".
const RPC_MAX_PER_SEC = Number(process.env.RPC_MAX_PER_SEC || 9);
if (provider) {
  const origSend = provider.send.bind(provider);
  const minGapMs = Math.ceil(1000 / Math.max(1, RPC_MAX_PER_SEC));
  let rpcChain = Promise.resolve();
  let lastAt = 0;
  provider.send = (method, params) => {
    const run = rpcChain.then(async () => {
      const wait = minGapMs - (Date.now() - lastAt);
      if (wait > 0) await new Promise((r) => setTimeout(r, wait));
      lastAt = Date.now();
      return origSend(method, params);
    });
    rpcChain = run.then(() => {}, () => {}); // keep the queue alive across errors
    return run;
  };
  // gentler receipt polling — one confirmation on a ~100ms-block L2 is quick.
  provider.pollingInterval = Number(process.env.RPC_POLL_MS || 1500);
}
const wallet = DISTRIBUTOR_PRIVATE_KEY && provider ? new ethers.Wallet(DISTRIBUTOR_PRIVATE_KEY, provider) : null;
if (!provider) console.warn("⚠️ No RPC_URL set — cannot read the chain.");
if (!wallet && !DRY_RUN) console.warn("⚠️ No distributor key — running in DRY mode.");

/* ============================== STATE ============================== */
let isRunning = false;
let holdersTimer = null, priceTimer = null, potTimer = null, payTimer = null, countdownTimer = null, dryFillTimer = null;

const app = express();
app.use(express.json());
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const WS_OPEN = WebSocket?.OPEN ?? 1;

let latestHolders = null;
let lastDistributionResult = null;
let tokenDecimals = 18;
let tokenTotalSupply = 0n;

let ethUsd = null;                 // ETH/USD (Blockscout stats)
let ethUsdFetchedAt = 0;
let mcapUsd = null;                // display only (DexScreener)
let nextPayTs = null;
let distributing = false;          // true while a cycle is in flight (admin 409s)
let distChain = Promise.resolve(); // serializes cycles — nothing skipped, nothing overlapped
let tokenEpoch = 0;                // bumped on CA switch; in-flight work from the old CA aborts
let lastGasLimit = 60_000n;        // remembered from the last cycle (coverage estimate)

let potWei = 0n;                   // native ETH in the drawer wallet (real or simulated)
let dryPotWei = ethers.parseEther(String(DRY_POT_ETH));

const balances = new Map();        // $RBM holder address -> BigInt raw
const contractHolders = new Set(); // holders flagged is_contract by the indexer

const paidByHolder = new Map();    // addr -> { usd: number, wei: BigInt, n: number }
let paidUsdTotal = 0;              // all-time totals
let paidWeiTotal = 0n;
let payoutsTotal = 0;
let payBuffer = [];                // payments awaiting Mongo insert
const recentPayments = [];
const MAX_RECENT_PAYMENTS = 200;

/* ============================== HELPERS ============================ */
const now = () => Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function sendTo(s, o) { if (s.readyState === WS_OPEN) s.send(JSON.stringify(o)); }
function broadcast(o) { const p = JSON.stringify(o); for (const c of wss.clients) if (c.readyState === WS_OPEN) c.send(p); }
function pushRecentPayments(arr) { for (const p of arr) recentPayments.push(p); while (recentPayments.length > MAX_RECENT_PAYMENTS) recentPayments.shift(); }
const isAddr = (a) => { try { return ethers.isAddress(a); } catch { return false; } };
const fmtUnits = (raw, dec) => Number(ethers.formatUnits(raw ?? 0n, dec ?? 18));
const inDryMode = () => DRY_RUN || !wallet;
function sanitizeAddress(input) {
  if (!input) throw new Error("Empty address");
  const m = String(input).match(/0x[a-fA-F0-9]{40}/);
  if (!m) throw new Error("No EVM address found");
  return ethers.getAddress(m[0]);
}
function isExcluded(addr, distAddr) {
  return addr === distAddr || addr === ZERO || addr === DEAD
    || EXCLUDE_ADDRESSES.has(addr)
    || (EXCLUDE_CONTRACTS && contractHolders.has(addr));
}
// $X of ETH in wei at the current price (float precision is far beyond a cent)
function usdToWei(usd) {
  if (!ethUsd || ethUsd <= 0) return 0n;
  return ethers.parseEther((usd / ethUsd).toFixed(18));
}
const bsAuth = () => (BLOCKSCOUT_API_KEY ? { apikey: BLOCKSCOUT_API_KEY } : {});
function makeFakeHash() { return `0xSIM${Date.now().toString(16)}${Math.random().toString(16).slice(2, 10)}`; }

/* ===================== STATE PERSISTENCE ========================== */
function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const raw = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (raw.token) TOKEN = raw.token;
    for (const [a, v] of Object.entries(raw.paid || {})) {
      paidByHolder.set(a, { usd: Number(v.usd || 0), wei: BigInt(v.wei || "0"), n: Number(v.n || 0) });
    }
    paidUsdTotal = Number(raw.paidUsdTotal || 0);
    paidWeiTotal = BigInt(raw.paidWeiTotal || "0");
    payoutsTotal = Number(raw.payoutsTotal || 0);
    console.log(`💾 State: $${paidUsdTotal.toFixed(2)} paid across ${payoutsTotal} payouts, ${paidByHolder.size} recipients on file`);
  } catch (e) { console.warn("State load failed:", e.message); }
}
function writeStateFileNow() {
  try {
    const paid = {};
    for (const [a, v] of paidByHolder) paid[a] = { usd: v.usd, wei: v.wei.toString(), n: v.n };
    // atomic: write a temp file then rename, so a crash can never corrupt the state
    const tmp = `${STATE_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify({ token: TOKEN, paid, paidUsdTotal, paidWeiTotal: paidWeiTotal.toString(), payoutsTotal }));
    fs.renameSync(tmp, STATE_FILE);
  } catch (e) { console.warn("State save failed:", e.message); }
}
let saveTimer = null;
function saveStateSoon() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; writeStateFileNow(); }, 3_000);
}

/* ===================== MongoDB persistence ======================== */
let mongoDb = null;
async function initMongo() {
  if (!MONGO_URL) { console.log("ℹ️  No MONGO_URL — state persists only to the (ephemeral) file."); return; }
  try {
    const client = new MongoClient(MONGO_URL, { serverSelectionTimeoutMS: 8000 });
    await client.connect();
    // own database name — the Mongo server can be shared with other projects
    // without colliding on the config/state/payments collections
    mongoDb = client.db(process.env.MONGO_DB || "rubberbands");
    console.log("🍃 MongoDB connected.");
  } catch (e) { console.warn("Mongo connect failed:", e.message); mongoDb = null; }
}
async function loadFromMongo(token) {
  if (!mongoDb || !token) return;
  try {
    const doc = await mongoDb.collection("state").findOne({ _id: token.toLowerCase() });
    paidByHolder.clear(); paidUsdTotal = 0; paidWeiTotal = 0n; payoutsTotal = 0;
    if (doc) {
      for (const [a, v] of Object.entries(doc.paid || {})) {
        paidByHolder.set(a, { usd: Number(v.usd || 0), wei: BigInt(v.wei || "0"), n: Number(v.n || 0) });
      }
      paidUsdTotal = Number(doc.paidUsdTotal || 0);
      paidWeiTotal = BigInt(doc.paidWeiTotal || "0");
      payoutsTotal = Number(doc.payoutsTotal || 0);
      console.log(`🍃 Restored $${paidUsdTotal.toFixed(2)} / ${payoutsTotal} payouts for ${token}`);
    }
    // restore the paid feed so the site shows history right after a restart
    const pays = await mongoDb.collection("payments")
      .find({ ca: token.toLowerCase() }).sort({ ts: -1 }).limit(MAX_RECENT_PAYMENTS).toArray();
    if (pays.length) {
      recentPayments.length = 0;
      pushRecentPayments(pays.reverse().map((p) => ({ to: p.to, usd: p.usd, eth: p.eth, rank: p.rank ?? null, sig: p.sig, ts: p.ts })));
      console.log(`🍃 Restored ${pays.length} recent payments`);
    }
  } catch (e) { console.warn("Mongo load failed:", e.message); }
}
// The active CA + running flag live in Mongo too — Railway's filesystem is
// ephemeral, so without this every redeploy would silently stop the engine.
async function saveActiveConfig(running) {
  if (!mongoDb) return;
  try {
    await mongoDb.collection("config").updateOne(
      { _id: "active" },
      { $set: { token: TOKEN, running: !!running, payoutUsd: PAYOUT_USD, updatedAt: now() } },
      { upsert: true }
    );
  } catch (e) { console.warn("Mongo config save failed:", e.message); }
}
async function loadActiveConfig() {
  if (!mongoDb) return null;
  try { return await mongoDb.collection("config").findOne({ _id: "active" }); }
  catch { return null; }
}

let mongoSaveTimer = null;
function saveMongoSoon() {
  if (!mongoDb || mongoSaveTimer) return;
  mongoSaveTimer = setTimeout(async () => {
    mongoSaveTimer = null;
    try {
      const paid = {};
      for (const [a, v] of paidByHolder) paid[a] = { usd: v.usd, wei: v.wei.toString(), n: v.n };
      await mongoDb.collection("state").updateOne(
        { _id: (TOKEN || "").toLowerCase() },
        { $set: { paid, paidUsdTotal, paidWeiTotal: paidWeiTotal.toString(), payoutsTotal, updatedAt: now() } },
        { upsert: true }
      );
    } catch (e) { console.warn("Mongo save failed:", e.message); }
  }, 2_000);
}

function addPaid(addr, usd, wei) {
  const cur = paidByHolder.get(addr) || { usd: 0, wei: 0n, n: 0 };
  cur.usd += usd; cur.wei += wei; cur.n += 1;
  paidByHolder.set(addr, cur);
  paidUsdTotal += usd; paidWeiTotal += wei; payoutsTotal += 1;
  saveMongoSoon();
}

// Every individual transfer is also persisted to the `payments` collection, so
// the paid feed and the all-time stats survive restarts and are auditable.
async function flushPaymentsMongo() {
  if (!mongoDb || !payBuffer.length) return;
  const docs = payBuffer.splice(0);
  try {
    await mongoDb.collection("payments").insertMany(
      docs.map((p) => ({ ...p, ca: (TOKEN || "").toLowerCase() }))
    );
  } catch (e) { console.warn("Mongo payments insert failed:", e.message); }
}

/* ===================== TOKEN METADATA ============================= */
async function loadTokenMeta() {
  try {
    const resp = await axios.get(`${BLOCKSCOUT_URL}/api/v2/tokens/${TOKEN}`, {
      params: bsAuth(), timeout: 15_000, validateStatus: () => true,
    });
    if (resp.status === 200 && resp.data) {
      if (resp.data.decimals != null) tokenDecimals = Number(resp.data.decimals);
      if (resp.data.total_supply != null) tokenTotalSupply = BigInt(resp.data.total_supply);
      if (tokenTotalSupply > 0n) return;
    }
  } catch {}
  if (!provider) return;
  const c = new ethers.Contract(TOKEN, ERC20_ABI, provider);
  try { tokenDecimals = Number(await c.decimals()); } catch {}
  try { tokenTotalSupply = await c.totalSupply(); } catch {}
}

/* ============== HOLDERS via Blockscout (explorer API) ============= */
// One complete crawl against one base URL. Retries 429s AND 5xx with backoff.
// Builds into LOCAL structures — a partial crawl is thrown away, never applied,
// so a mid-crawl failure can't leave us paying over a truncated list.
async function crawlHolders(base, useAuth) {
  const map = new Map();
  const contracts = new Set();
  let params = useAuth ? { ...bsAuth() } : {};
  for (let page = 0; page < 1000; page++) {
    let resp;
    for (let tries = 0; tries < 5; tries++) {
      resp = await axios.get(`${base}/api/v2/tokens/${TOKEN}/holders`, { params, timeout: 20_000, validateStatus: () => true });
      if (resp.status === 200) break;
      await sleep(1000 * (tries + 1)); // backoff on 429 / 5xx and retry
    }
    if (resp.status !== 200) throw new Error(`Blockscout holders ${resp.status}`);
    const items = resp.data?.items || [];
    for (const it of items) {
      const addr = (it.address?.hash || it.address_hash || "").toLowerCase();
      const val = it.value ?? it.balance;
      if (addr && val != null) map.set(addr, BigInt(val));
      if (addr && it.address?.is_contract) contracts.add(addr);
    }
    const npp = resp.data?.next_page_params;
    if (!npp || !items.length) break;
    params = useAuth ? { ...npp, ...bsAuth() } : { ...npp };
  }
  return { map, contracts };
}
// Primary = Pro API; the public instance is the full fallback (the Pro API
// 500s on some tokens). Only a COMPLETE crawl is ever applied.
async function fetchHoldersBlockscout() {
  let res;
  try {
    res = await crawlHolders(BLOCKSCOUT_URL, true);
  } catch (e) {
    if (BLOCKSCOUT_PUBLIC && BLOCKSCOUT_PUBLIC !== BLOCKSCOUT_URL) {
      console.warn(`holders crawl failed on primary (${e.message}) — recrawling via public instance`);
      res = await crawlHolders(BLOCKSCOUT_PUBLIC, false);
    } else {
      throw e;
    }
  }
  contractHolders.clear();
  for (const a of res.contracts) contractHolders.add(a);
  return res.map;
}

/* ======================== POLLER: HOLDERS ========================= */
async function pollHolders() {
  try {
    if (!TOKEN || !isAddr(TOKEN)) throw new Error("CA not set / invalid");
    if (tokenTotalSupply === 0n) await loadTokenMeta();
    const map = await fetchHoldersBlockscout();
    balances.clear();
    for (const [a, v] of map) balances.set(a, v);

    const supply = fmtUnits(tokenTotalSupply, tokenDecimals) || 0;
    const distAddr = wallet ? wallet.address.toLowerCase() : null;

    const eligible = [];
    for (const [addr, raw] of balances) {
      if (raw <= 0n) continue;
      if (isExcluded(addr, distAddr)) continue;
      const amount = fmtUnits(raw, tokenDecimals);
      const pct = supply > 0 ? (amount / supply) * 100 : 0;
      if (pct < MIN_ELIGIBLE_PCT || pct > MAX_HOLDER_PCT) continue;
      eligible.push({ owner: addr, raw, amount, percentage: pct });
    }

    // the line: biggest bag first — that's the whole payout order
    eligible.sort((a, b) => (b.raw > a.raw ? 1 : b.raw < a.raw ? -1 : a.owner.localeCompare(b.owner)));
    const holders = eligible.map((h, i) => {
      const paid = paidByHolder.get(h.owner);
      return {
        owner: h.owner,
        rank: i + 1,
        amount: h.amount,
        percentage: h.percentage,
        rawBalance: h.raw.toString(),
        paidUsd: paid ? paid.usd : 0,
        paidCount: paid ? paid.n : 0,
      };
    });

    latestHolders = {
      type: "holders_update",
      totalHolders: [...balances.values()].filter((v) => v > 0n).length,
      eligibleHolders: holders.length,
      totalSupply: supply,
      holders,
      mint: TOKEN,
      minEligiblePct: MIN_ELIGIBLE_PCT,
      ts: now(),
    };
    broadcast(latestHolders);
    console.log(`📤 ${holders.length} in line (${contractHolders.size} contracts skipped)`);
  } catch (err) {
    console.error("holders poll:", err.message);
    broadcast({ type: "error", message: `holders: ${err.message}`, ts: now() });
  }
}

/* ==================== PRICES (ETH/USD + mcap) ===================== */
async function fetchEthUsd() {
  if (ETH_USD_OVERRIDE) { ethUsd = ETH_USD_OVERRIDE; ethUsdFetchedAt = now(); return ethUsd; }
  try {
    const resp = await axios.get(`${BLOCKSCOUT_PUBLIC}/api/v2/stats`, { timeout: 12_000, validateStatus: () => true });
    const p = Number(resp.data?.coin_price);
    if (Number.isFinite(p) && p > 0) { ethUsd = p; ethUsdFetchedAt = now(); }
  } catch {}
  return ethUsd;
}
// Market cap is display-only (the site shows it); DexScreener prices bonding
// curves AND graduated pools correctly on this chain.
async function fetchMcap() {
  if (!TOKEN) return;
  try {
    const r = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${TOKEN}`, {
      timeout: 12_000, validateStatus: () => true,
    });
    if (r.status !== 200) return;
    const pairs = (r.data?.pairs || []).filter((p) =>
      String(p.chainId || "").toLowerCase().includes("robinhood") &&
      (p.baseToken?.address || "").toLowerCase() === TOKEN.toLowerCase()
    );
    if (!pairs.length) return;
    pairs.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));
    const mc = Number(pairs[0].marketCap ?? pairs[0].fdv);
    if (Number.isFinite(mc) && mc > 0) mcapUsd = mc;
  } catch {}
}
async function priceTick() {
  const ep = tokenEpoch;
  await fetchEthUsd();
  await fetchMcap();
  if (ep !== tokenEpoch) return;
  broadcast(cycleStateSnapshot());
}

/* ======================== POLLER: DRAWER ========================== */
async function pollPot() {
  if (inDryMode()) { potWei = dryPotWei; broadcast(cycleStateSnapshot()); return; }
  try {
    potWei = await provider.getBalance(wallet.address);
    broadcast(cycleStateSnapshot());
  } catch (e) { console.warn("pot poll:", e.message); }
}

/* ========================= STATE SNAPSHOT ========================= */
function cycleStateSnapshot() {
  const priceOk = ethUsd != null && now() - ethUsdFetchedAt <= PRICE_STALE_MS;
  const payoutWei = priceOk ? usdToWei(PAYOUT_USD) : 0n;
  const spendable = potWei > LEAVE_WEI ? potWei - LEAVE_WEI : 0n;
  const budget = (spendable * BigInt(Math.round(BUDGET_PCT))) / 100n;
  // rough coverage for display: how many $1 payouts (incl. est. gas) the drawer holds
  let coverage = 0;
  if (payoutWei > 0n) {
    const perCost = payoutWei + lastGasLimit * 1_000_000_000n; // ~1 gwei placeholder when no live fee data
    coverage = Number(budget / (perCost > 0n ? perCost : payoutWei));
  }
  return {
    type: "cycle_state",
    potEth: fmtUnits(potWei, 18),
    potUsd: priceOk ? fmtUnits(potWei, 18) * ethUsd : null,
    ethUsd: priceOk ? ethUsd : null,
    payoutUsd: PAYOUT_USD,
    payEveryMs: PAY_EVERY_MS,
    nextPayTs,
    budgetPct: BUDGET_PCT,
    coverage,
    eligibleHolders: latestHolders?.eligibleHolders ?? null,
    paidUsdTotal,
    paidEthTotal: fmtUnits(paidWeiTotal, 18),
    payoutsTotal,
    mcapUsd,
    minEligiblePct: MIN_ELIGIBLE_PCT,
    running: isRunning,
    dryRun: inDryMode(),
    ts: now(),
  };
}

/* ===================== THE $1 DISPENSER =========================== */
// Serialized: a forced cycle can never overlap the timer's cycle, and neither
// is ever silently skipped — later calls simply wait their turn in the chain.
function payCycle(kind) {
  const run = distChain.then(() => runPayCycle(kind));
  distChain = run.catch(() => {});
  return run;
}
async function runPayCycle(kind) {
  const ep = tokenEpoch;
  distributing = true;
  try {
    const dry = inDryMode();

    // 1) the dollar must be a real dollar — fresh ETH/USD or no cycle
    if (!ethUsd || now() - ethUsdFetchedAt > PRICE_STALE_MS) await fetchEthUsd();
    if (!ethUsd || now() - ethUsdFetchedAt > PRICE_STALE_MS) {
      console.log(`🟨 ${kind}: no fresh ETH/USD price — not paying on a guess.`);
      return;
    }

    // 2) never pay over frozen holder data
    if (!latestHolders || now() - (latestHolders.ts || 0) > HOLDERS_STALE_MS) {
      console.log(`🟨 ${kind}: holder snapshot stale or missing — skipping cycle.`);
      return;
    }
    // defensively re-sort: biggest bag first is the contract of this product
    const line = [...(latestHolders.holders || [])].sort((a, b) => {
      const ar = BigInt(a.rawBalance || "0"), br = BigInt(b.rawBalance || "0");
      return br > ar ? 1 : br < ar ? -1 : a.owner.localeCompare(b.owner);
    });
    if (!line.length) { console.log(`🟨 ${kind}: nobody in line.`); return; }

    // 3) count the drawer
    const payoutWei = usdToWei(PAYOUT_USD);
    if (payoutWei <= 0n) return;
    let pot;
    if (dry) {
      pot = dryPotWei;
    } else {
      pot = await provider.getBalance(wallet.address);
      potWei = pot;
    }
    const spendable = pot > LEAVE_WEI ? pot - LEAVE_WEI : 0n;
    const budget = (spendable * BigInt(Math.round(BUDGET_PCT))) / 100n;
    if (budget < payoutWei) {
      console.log(`🟨 ${kind}: drawer ($${(fmtUnits(budget, 18) * ethUsd).toFixed(2)}) can't cover a single $${PAYOUT_USD} — restocking.`);
      broadcast(cycleStateSnapshot());
      return;
    }

    // 4) gas: reserve payout + gas per recipient so the loop can never strand
    //    a send halfway. Native transfers on this Orbit L2 price L1 data into
    //    gas, so the limit is well above 21k — unused gas is refunded.
    let gasLimit = NATIVE_GAS_LIMIT > 0n ? NATIVE_GAS_LIMIT : lastGasLimit;
    let gasFields = {};
    let maxFee = 1_000_000_000n; // dry-mode placeholder
    if (!dry) {
      const feeData = await provider.getFeeData();
      maxFee = feeData.maxFeePerGas || feeData.gasPrice || 1n;
      gasFields = feeData.maxFeePerGas
        ? { maxFeePerGas: feeData.maxFeePerGas, maxPriorityFeePerGas: feeData.maxPriorityFeePerGas ?? feeData.maxFeePerGas }
        : { gasPrice: feeData.gasPrice };
      if (NATIVE_GAS_LIMIT <= 0n) {
        try {
          const est = await provider.estimateGas({ from: wallet.address, to: ethers.getAddress(line[0].owner), value: payoutWei });
          gasLimit = est * 2n; // 100% headroom
          if (gasLimit < 40_000n) gasLimit = 40_000n;
        } catch { gasLimit = 100_000n; }
      }
      lastGasLimit = gasLimit;
    }
    const perCost = payoutWei + gasLimit * maxFee;

    // 5) who gets paid this round: top of the line down, until it runs out
    const count = Math.min(
      Number(budget / perCost),
      line.length,
      MAX_RECIPIENTS_PER_CYCLE
    );
    if (count <= 0) {
      console.log(`🟨 ${kind}: budget covers the payout but not its gas — restocking.`);
      broadcast(cycleStateSnapshot());
      return;
    }
    const recipients = line.slice(0, count);
    const ranOut = count < line.length;
    console.log(`💵 ${kind}: paying $${PAYOUT_USD} (${fmtUnits(payoutWei, 18).toFixed(6)} ETH) to ${count}/${line.length} holders, biggest first${ranOut ? " — drawer runs out before the tail" : ""}`);

    // 6) send loop
    const items = [];
    let sentWei = 0n;
    const record = (to, rank, hash) => {
      sentWei += payoutWei;
      addPaid(to, PAYOUT_USD, payoutWei);
      const pay = { to, usd: PAYOUT_USD, eth: fmtUnits(payoutWei, 18), rank, sig: hash, ts: now() };
      items.push(pay);
      payBuffer.push({ ...pay, kind });
      pushRecentPayments([pay]);
      broadcast({ type: "payments_update", payments: [pay] });
    };

    if (dry) {
      for (const r of recipients) { record(r.owner, r.rank, makeFakeHash()); }
      dryPotWei -= sentWei;
      if (dryPotWei < 0n) dryPotWei = 0n;
      potWei = dryPotWei;
    } else {
      let nonce = await wallet.getNonce();
      let failures = 0, logged = 0;
      for (const r of recipients) {
        if (ep !== tokenEpoch) { console.warn(`⚠️ ${kind}: CA switched — aborting remaining sends`); break; }
        let hash = null, lastErr = null;
        for (let attempt = 0; attempt < 4; attempt++) {
          try {
            const tx = await wallet.sendTransaction({ to: ethers.getAddress(r.owner), value: payoutWei, nonce, gasLimit, ...gasFields });
            hash = tx.hash;
            break;
          } catch (e) {
            lastErr = e;
            const msg = String(e?.message || e).toLowerCase();
            // A timeout / "nonce too low" does NOT mean the tx didn't land. If the
            // chain's pending nonce moved past ours, our transfer DID go through —
            // record it and never re-send (re-sending would pay the holder twice).
            if (/nonce|coalesce|timeout/.test(msg)) {
              try {
                const pend = await wallet.getNonce("pending");
                if (pend > nonce) { hash = `sent-nonce-${nonce}`; break; }
                nonce = pend; // genuinely stale — resync and retry
              } catch {}
              await sleep(1200 * (attempt + 1));
              continue;
            }
            if (/rate|limit|-32007/.test(msg)) { await sleep(1200 * (attempt + 1)); continue; }
            if (/insufficient funds/.test(msg)) { attempt = 4; break; } // drawer is dry — stop retrying this one
            break;
          }
        }
        if (!hash) {
          failures++;
          if (logged++ < 3) console.warn(`payout fail: ${String(lastErr?.message || lastErr).slice(0, 140)}`);
          if (/insufficient funds/.test(String(lastErr?.message || "").toLowerCase())) { console.warn(`⚠️ ${kind}: drawer ran dry mid-cycle — stopping here`); break; }
          await sleep(SEND_DELAY_MS);
          continue;
        }
        nonce++;
        record(r.owner, r.rank, hash);
        await sleep(SEND_DELAY_MS);
      }
      if (failures) console.warn(`⚠️ ${failures} payout(s) failed to send`);
      try { potWei = await provider.getBalance(wallet.address); } catch {}
    }

    saveStateSoon(); saveMongoSoon();
    await flushPaymentsMongo();
    lastDistributionResult = {
      type: "distribution_result",
      kind,
      count: items.length,
      totalUsd: items.length * PAYOUT_USD,
      totalEth: fmtUnits(sentWei, 18),
      eligible: line.length,
      coveredThrough: items.length,   // paid ranks 1..N
      ranOut,
      dryRun: dry,
      ts: now(),
    };
    broadcast(lastDistributionResult);
    broadcast(cycleStateSnapshot());
    console.log(`${dry ? "🧪 DRY" : "✅"} ${kind}: handed $${(items.length * PAYOUT_USD).toFixed(2)} to ${items.length} holders (${fmtUnits(sentWei, 18).toFixed(6)} ETH)`);
  } catch (err) {
    console.error("Cycle error:", err?.message || err);
    broadcast({ type: "error", message: `cycle: ${err.message || err}`, ts: now() });
  } finally {
    distributing = false;
  }
}
function payTick() {
  nextPayTs = now() + PAY_EVERY_MS;
  payCycle("cycle");
}

/* ========================== WEBSOCKET ============================= */
wss.on("connection", (socket) => {
  sendTo(socket, { type: "service_status", running: isRunning, ca: TOKEN, ts: now() });
  sendTo(socket, latestHolders || { type: "holders_update", totalHolders: 0, holders: [], ts: now() });
  sendTo(socket, cycleStateSnapshot());
  if (recentPayments.length) sendTo(socket, { type: "payments_update", payments: recentPayments });
  if (lastDistributionResult) sendTo(socket, lastDistributionResult);
});

server.listen(PORT, () => {
  console.log(`✅ Rubberbands & More dispenser — HTTP+WS on :${PORT}`);
  console.log(`   POST /set-ca · /start · /stop · /clear-ca · /force-pay · /set-payout · GET /status`);
});

/* ========================= HTTP API ================================ */
function checkAdmin(req) {
  if (!ADMIN_PASSWORD) return false; // fail closed — admin endpoints need ADMIN_PASSWORD set
  const pw = req.body?.password || req.query?.password || req.headers["x-admin-password"];
  return pw === ADMIN_PASSWORD;
}
if (!ADMIN_PASSWORD) console.warn("⚠️ ADMIN_PASSWORD not set — all admin endpoints are DISABLED until it is.");
function busy(res) {
  return res.status(409).json({ error: "a payout cycle is in flight — retry in a moment" });
}
function resetForNewToken(newToken) {
  tokenEpoch++; // aborts any in-flight cycle / poll from the old CA
  TOKEN = newToken;
  balances.clear();
  contractHolders.clear();
  paidByHolder.clear();
  paidUsdTotal = 0; paidWeiTotal = 0n; payoutsTotal = 0;
  payBuffer = [];
  recentPayments.length = 0;
  latestHolders = null;
  lastDistributionResult = null;
  tokenTotalSupply = 0n;
  mcapUsd = null;
  dryPotWei = ethers.parseEther(String(DRY_POT_ETH));
}

app.get("/health", (_req, res) => res.json({ ok: true, ts: now() }));

app.post(["/set-ca", "/set-home", "/set-token"], async (req, res) => {
  try {
    if (!checkAdmin(req)) return res.status(401).json({ error: "unauthorized" });
    if (!provider && !inDryMode()) return res.status(400).json({ error: "No RPC_URL configured" });
    if (distributing) return busy(res);
    const ca = req.body?.ca || req.body?.homeCa;
    if (!ca) return res.status(400).json({ error: "Missing 'ca'" });
    const a = sanitizeAddress(ca);
    resetForNewToken(a);
    await loadFromMongo(a);
    await loadTokenMeta();
    if (!isRunning) startAllLoops();
    else {
      pollHolders();
      nextPayTs = now() + PAY_EVERY_MS;
      if (payTimer) { clearInterval(payTimer); payTimer = setInterval(payTick, PAY_EVERY_MS); }
    }
    await saveActiveConfig(isRunning);
    broadcast({ type: "service_status", running: isRunning, ca: TOKEN, ts: now() });
    broadcast(cycleStateSnapshot());
    console.log(`🔁 CA switched to ${TOKEN}`);
    res.json({ ok: true, ca: TOKEN, running: isRunning });
  } catch (e) { res.status(400).json({ error: e.message || String(e) }); }
});

app.post("/start", async (req, res) => {
  try {
    if (!checkAdmin(req)) return res.status(401).json({ error: "unauthorized" });
    if (!provider && !inDryMode()) return res.status(400).json({ error: "No RPC_URL configured" });
    const ca = req.body?.ca || req.body?.homeCa;
    if (!ca && !TOKEN) return res.status(400).json({ error: "Missing 'ca'" });
    if (isRunning) return res.status(400).json({ error: "Already running" });
    if (ca) {
      const a = sanitizeAddress(ca);
      if (a.toLowerCase() !== (TOKEN || "").toLowerCase()) { resetForNewToken(a); await loadFromMongo(a); }
    }
    await loadTokenMeta();
    startAllLoops();
    await saveActiveConfig(true);
    res.json({ ok: true, running: true, ca: TOKEN });
  } catch (e) { res.status(400).json({ error: e.message || String(e) }); }
});
app.post("/stop", (req, res) => {
  if (!checkAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  if (!isRunning) return res.status(400).json({ error: "Not running" });
  stopAllLoops();
  saveActiveConfig(false);
  res.json({ ok: true, running: false });
});
app.post(["/clear-ca", "/clear-home", "/clear-token"], (req, res) => {
  if (!checkAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  if (distributing) return busy(res);
  const was = TOKEN;
  stopAllLoops();
  resetForNewToken(null);
  saveStateSoon();
  broadcast({ type: "service_status", running: false, ca: null, ts: now() });
  broadcast(cycleStateSnapshot());
  saveActiveConfig(false);
  console.log(`🧹 CA cleared (was ${was || "none"})`);
  res.json({ ok: true, ca: null, running: false, cleared: was || null });
});
// Run a payout cycle right now (doesn't touch the 5-minute clock).
app.post("/force-pay", async (req, res) => {
  if (!checkAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  if (!TOKEN) return res.status(400).json({ error: "CA not set" });
  await payCycle("forced");
  res.json({ ok: true, result: lastDistributionResult });
});
// Change the per-holder dollar amount at runtime (persisted in Mongo config;
// a PAYOUT_USD env var still wins on the next boot).
app.post("/set-payout", async (req, res) => {
  if (!checkAdmin(req)) return res.status(401).json({ error: "unauthorized" });
  if (distributing) return busy(res);
  const v = Number(req.body?.usd);
  if (!Number.isFinite(v) || v <= 0 || v > 1000) return res.status(400).json({ error: "usd must be 0 < usd <= 1000" });
  PAYOUT_USD = v;
  await saveActiveConfig(isRunning);
  broadcast(cycleStateSnapshot());
  console.log(`💲 payout set to $${PAYOUT_USD}/holder/cycle`);
  res.json({ ok: true, payoutUsd: PAYOUT_USD });
});
app.get("/status", (_req, res) => {
  res.json({
    running: isRunning, ca: TOKEN, chain: "evm", chainId: CHAIN_ID,
    model: `every ${Math.round(PAY_EVERY_MS / 60000)} minutes the drawer pays $${PAYOUT_USD} of ETH to each holder, biggest bag first, until it runs out`,
    payoutUsd: PAYOUT_USD, payEveryMs: PAY_EVERY_MS, budgetPct: BUDGET_PCT,
    minEligiblePct: MIN_ELIGIBLE_PCT, maxHolderPct: MAX_HOLDER_PCT,
    ethUsd, mcapUsd,
    drawerWallet: wallet ? wallet.address : null,
    potEth: fmtUnits(potWei, 18),
    paidUsdTotal, payoutsTotal,
    latestHolders, lastDistributionResult,
    cycle: cycleStateSnapshot(),
    ts: now(),
  });
});

/* ========================== START/STOP LOOPS ====================== */
function startAllLoops() {
  if (isRunning) return;
  if (!TOKEN) { console.error("Cannot start: CA not set"); return; }
  isRunning = true;
  pollHolders();
  holdersTimer = setInterval(pollHolders, HOLDERS_POLL_MS);
  priceTick();
  priceTimer = setInterval(priceTick, PRICE_POLL_MS);
  pollPot();
  potTimer = setInterval(pollPot, POT_POLL_MS);
  nextPayTs = now() + PAY_EVERY_MS;
  payTimer = setInterval(payTick, PAY_EVERY_MS);
  countdownTimer = setInterval(() => broadcast(cycleStateSnapshot()), 5_000);
  if (inDryMode()) {
    // simulated creator fees so a dry deploy behaves end-to-end
    dryFillTimer = setInterval(() => {
      dryPotWei += ethers.parseEther((0.0002 + Math.random() * 0.0018).toFixed(6));
      potWei = dryPotWei;
    }, 8_000);
  }
  broadcast({ type: "service_status", running: true, ca: TOKEN, ts: now() });
  console.log("▶️  Rubberbands & More started — first cycle in " + Math.round(PAY_EVERY_MS / 1000) + "s.");
}
function stopAllLoops() {
  if (!isRunning) return;
  [holdersTimer, priceTimer, potTimer, payTimer, countdownTimer, dryFillTimer].forEach((t) => t && clearInterval(t));
  holdersTimer = priceTimer = potTimer = payTimer = countdownTimer = dryFillTimer = null;
  isRunning = false; nextPayTs = null;
  broadcast({ type: "service_status", running: false, ca: TOKEN, ts: now() });
  broadcast(cycleStateSnapshot());
  console.log("⏹  Rubberbands & More stopped.");
}

/* ============================ BOOT ================================= */
(async () => {
  loadState();
  await initMongo();
  // resume the CA that was live before the last redeploy (Mongo is authoritative)
  let resume = false;
  const cfg = await loadActiveConfig();
  if (!process.env.CA && !process.env.TOKEN && cfg?.token && cfg?.running) {
    TOKEN = cfg.token; resume = true; console.log(`🍃 Resuming CA from Mongo: ${TOKEN}`);
  }
  if (!PAYOUT_USD_PINNED && Number.isFinite(cfg?.payoutUsd) && cfg.payoutUsd > 0) {
    PAYOUT_USD = cfg.payoutUsd; console.log(`🍃 Restored payout: $${PAYOUT_USD}/holder`);
  }
  await loadFromMongo(TOKEN);
  if ((resume || String(process.env.AUTO_START || "false") === "true") && TOKEN && (provider || inDryMode())) {
    await loadTokenMeta();
    startAllLoops();
  } else {
    console.log("⏸  Boot paused. POST /start { ca } to begin (or AUTO_START=true + CA + RPC_URL).");
  }
})();
