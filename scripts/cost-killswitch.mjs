#!/usr/bin/env node
/**
 * Account-level Cloudflare cost killswitch — USD model.
 *
 * Queries every billable analytics dataset, multiplies usage by the current
 * CF unit price to produce an estimated daily $ spend. Trips on a single
 * `DAILY_USD_ALERT` threshold and identifies the top-cost resource.
 *
 * Env:
 *   CF_ACCOUNT_ID           — required
 *   CF_API_TOKEN            — required (Account Analytics: Read)
 *   DAILY_USD_ALERT         — optional, default 100 ($)
 *
 * Exit codes:
 *   0  — estimated daily spend within threshold
 *   1  — threshold exceeded
 *   2  — query failed (do NOT auto-disable)
 */

const ACCOUNT_ID = process.env.CF_ACCOUNT_ID;
const API_TOKEN = process.env.CF_API_TOKEN;

if (!ACCOUNT_ID || !API_TOKEN) {
  console.error("CF_ACCOUNT_ID and CF_API_TOKEN are required");
  process.exit(2);
}

const thresholdUsd = Number(process.env.DAILY_USD_ALERT || 100);

// CF unit prices (per-million, except duration which is per-million-GB-seconds).
// Verified against https://developers.cloudflare.com/*/platform/pricing/ 2026-04.
const P_PER_MILLION = {
  workerRequests: 0.30,
  doRequests: 0.15,
  doDurationGbSec: 12.50,
  d1RowsRead: 0.001,
  d1RowsWritten: 1.00,
  kvReads: 0.50,
  kvWrites: 5.00, // writes + lists + deletes share this tier per CF docs
  r2ClassA: 4.50,
  r2ClassB: 0.36,
};
const DO_MEMORY_GB = 0.125; // every DO instance is allocated 128 MiB

// R2 action classification per CF pricing docs.
const R2_CLASS_A = new Set([
  "PutObject", "CopyObject", "DeleteObject", "DeleteObjects",
  "CreateMultipartUpload", "CompleteMultipartUpload", "UploadPart",
  "UploadPartCopy", "AbortMultipartUpload",
  "ListBuckets", "ListObjectsV2", "ListMultipartUploads", "ListParts",
  "PutBucket", "DeleteBucket",
]);
const R2_CLASS_B = new Set(["GetObject", "HeadObject", "HeadBucket"]);
// KV "read" is cheap tier; everything else (write/list/delete) is the $5/M tier.

const now = new Date();
const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
const startIso = start.toISOString();
const nowIso = now.toISOString();

const query = `
  query Usage($accountTag: String!, $start: Time!, $end: Time!) {
    viewer {
      accounts(filter: { accountTag: $accountTag }) {
        workersInvocationsAdaptive(
          filter: { datetime_geq: $start, datetime_lt: $end }
          limit: 10000
        ) {
          dimensions { scriptName }
          sum { requests errors }
        }
        durableObjectsInvocationsAdaptiveGroups(
          filter: { datetime_geq: $start, datetime_lt: $end }
          limit: 10000
        ) {
          dimensions { scriptName namespaceId }
          sum { requests }
        }
        durableObjectsPeriodicGroups(
          filter: { datetime_geq: $start, datetime_lt: $end }
          limit: 10000
        ) {
          dimensions { namespaceId }
          sum { activeTime }
        }
        d1AnalyticsAdaptiveGroups(
          filter: { datetime_geq: $start, datetime_lt: $end }
          limit: 10000
        ) {
          dimensions { databaseId }
          sum { rowsRead rowsWritten }
        }
        kvOperationsAdaptiveGroups(
          filter: { datetime_geq: $start, datetime_lt: $end }
          limit: 10000
        ) {
          dimensions { actionType namespaceId }
          sum { requests }
        }
        r2OperationsAdaptiveGroups(
          filter: { datetime_geq: $start, datetime_lt: $end }
          limit: 10000
        ) {
          dimensions { actionType bucketName }
          sum { requests }
        }
      }
    }
  }
`;

async function callGraphQL() {
  const res = await fetch("https://api.cloudflare.com/client/v4/graphql", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${API_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query,
      variables: { accountTag: ACCOUNT_ID, start: startIso, end: nowIso },
    }),
  });
  if (!res.ok) throw new Error(`GraphQL HTTP ${res.status}: ${await res.text()}`);
  const data = await res.json();
  if (data.errors?.length) throw new Error(`GraphQL errors: ${JSON.stringify(data.errors)}`);
  return data.data.viewer.accounts[0] ?? {};
}

function addCost(map, key, amount) {
  if (amount <= 0) return;
  map[key] = (map[key] ?? 0) + amount;
}

function sortDesc(obj) {
  return Object.fromEntries(Object.entries(obj).sort((a, b) => b[1] - a[1]));
}

function main() {
  return callGraphQL().then((acct) => {
    // Per-owner cost accumulation, labeled by kind so the issue can
    // disambiguate "agent-kanban (worker)" from a same-named DB.
    const costByOwner = {};
    const costByCategory = {};

    // ---- Workers: requests ----
    for (const r of acct.workersInvocationsAdaptive ?? []) {
      const name = r.dimensions?.scriptName ?? "(unknown)";
      const reqs = r.sum?.requests ?? 0;
      const cost = (reqs / 1e6) * P_PER_MILLION.workerRequests;
      addCost(costByOwner, `worker:${name}`, cost);
      addCost(costByCategory, "Workers requests", cost);
    }

    // ---- DO: requests + duration ----
    const nsToScript = {};
    for (const r of acct.durableObjectsInvocationsAdaptiveGroups ?? []) {
      const ns = r.dimensions?.namespaceId;
      const sn = r.dimensions?.scriptName;
      if (ns && sn) nsToScript[ns] = sn;
      const name = sn ?? "(unknown)";
      const reqs = r.sum?.requests ?? 0;
      const cost = (reqs / 1e6) * P_PER_MILLION.doRequests;
      addCost(costByOwner, `do:${name}`, cost);
      addCost(costByCategory, "DO requests", cost);
    }
    for (const r of acct.durableObjectsPeriodicGroups ?? []) {
      const ns = r.dimensions?.namespaceId;
      const sn = nsToScript[ns] ?? `namespace:${ns}`;
      const activeSec = (r.sum?.activeTime ?? 0) / 1e6; // μs → s
      const gbSec = activeSec * DO_MEMORY_GB;
      const cost = (gbSec / 1e6) * P_PER_MILLION.doDurationGbSec;
      addCost(costByOwner, `do:${sn}`, cost);
      addCost(costByCategory, "DO duration", cost);
    }

    // ---- D1: rows read/written by database ----
    for (const r of acct.d1AnalyticsAdaptiveGroups ?? []) {
      const db = r.dimensions?.databaseId ?? "(unknown)";
      const rr = r.sum?.rowsRead ?? 0;
      const rw = r.sum?.rowsWritten ?? 0;
      const readCost = (rr / 1e6) * P_PER_MILLION.d1RowsRead;
      const writeCost = (rw / 1e6) * P_PER_MILLION.d1RowsWritten;
      addCost(costByOwner, `d1:${db}`, readCost + writeCost);
      addCost(costByCategory, "D1 rows read", readCost);
      addCost(costByCategory, "D1 rows written", writeCost);
    }

    // ---- KV: reads vs writes (by namespaceId) ----
    for (const r of acct.kvOperationsAdaptiveGroups ?? []) {
      const ns = r.dimensions?.namespaceId ?? "(unknown)";
      const actionType = r.dimensions?.actionType ?? "unknown";
      const reqs = r.sum?.requests ?? 0;
      const isRead = actionType === "read";
      const rate = isRead ? P_PER_MILLION.kvReads : P_PER_MILLION.kvWrites;
      const cost = (reqs / 1e6) * rate;
      addCost(costByOwner, `kv:${ns}`, cost);
      addCost(costByCategory, isRead ? "KV reads" : "KV writes/deletes/lists", cost);
    }

    // ---- R2: Class A vs B (by bucket) ----
    for (const r of acct.r2OperationsAdaptiveGroups ?? []) {
      const bucket = r.dimensions?.bucketName || "(no-bucket)";
      const action = r.dimensions?.actionType ?? "";
      const reqs = r.sum?.requests ?? 0;
      let rate = 0;
      let cat = "R2 other";
      if (R2_CLASS_A.has(action)) { rate = P_PER_MILLION.r2ClassA; cat = "R2 Class A"; }
      else if (R2_CLASS_B.has(action)) { rate = P_PER_MILLION.r2ClassB; cat = "R2 Class B"; }
      const cost = (reqs / 1e6) * rate;
      addCost(costByOwner, `r2:${bucket}`, cost);
      addCost(costByCategory, cat, cost);
    }

    // ---- Totals & sort ----
    const totalCost = Object.values(costByOwner).reduce((a, b) => a + b, 0);
    const byOwnerSorted = sortDesc(costByOwner);
    const byCategorySorted = sortDesc(costByCategory);

    const tripped = totalCost > thresholdUsd;
    const topOwnerEntry = Object.entries(byOwnerSorted)[0];
    const topOwner = topOwnerEntry ? { name: topOwnerEntry[0], costUSD: topOwnerEntry[1] } : null;

    const result = {
      tripped,
      estimatedDailyCostUSD: Number(totalCost.toFixed(4)),
      thresholdUSD: thresholdUsd,
      windowStart: startIso,
      windowEnd: nowIso,
      topOwner,
      costByCategory: Object.fromEntries(Object.entries(byCategorySorted).map(([k, v]) => [k, Number(v.toFixed(4))])),
      costByOwner: Object.fromEntries(Object.entries(byOwnerSorted).slice(0, 20).map(([k, v]) => [k, Number(v.toFixed(4))])),
      reasons: tripped ? [`estimatedDailyCostUSD ${totalCost.toFixed(2)} > ${thresholdUsd}`] : [],
    };

    console.log(JSON.stringify(result));

    const fmt = (v) => `$${v.toFixed(4)}`.padStart(12);
    const lines = [
      `[killswitch] window=${startIso}..${nowIso}`,
      `  total estimated spend: $${totalCost.toFixed(4)} / $${thresholdUsd}  (${((totalCost / thresholdUsd) * 100).toFixed(2)}%)`,
      ``,
      `  by category:`,
      ...Object.entries(byCategorySorted).map(([k, v]) => `    ${k.padEnd(28)} ${fmt(v)}`),
      ``,
      `  top 10 by owner:`,
      ...Object.entries(byOwnerSorted).slice(0, 10).map(([k, v]) => `    ${k.padEnd(55)} ${fmt(v)}`),
      ``,
      `  tripped: ${tripped}${tripped ? ` (${result.reasons.join("; ")})` : ""}`,
    ];
    console.error(lines.join("\n"));

    process.exit(tripped ? 1 : 0);
  }).catch((err) => {
    console.error(`[killswitch] query failed: ${err.message}`);
    console.log(JSON.stringify({ tripped: false, error: err.message }));
    process.exit(2);
  });
}

main();
