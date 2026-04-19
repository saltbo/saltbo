#!/usr/bin/env node
/**
 * Account-level Cloudflare cost killswitch.
 *
 * Queries usage grouped by scriptName / databaseId so a tripped threshold
 * can be attributed to a specific offender. Emits a JSON result with
 * totals, per-resource breakdown, and top offenders.
 *
 * Note on DO duration: the `durableObjectsPeriodicGroups` dataset exposes
 * only `namespaceId` as a dimension (not `scriptName`), so we build a
 * namespaceId → scriptName map from the Invocations dataset and translate.
 *
 * Env:
 *   CF_ACCOUNT_ID             — required
 *   CF_API_TOKEN              — required, needs Account Analytics: Read
 *   DAILY_WORKER_REQUESTS     — optional, default  5_000_000
 *   DAILY_D1_ROWS_READ        — optional, default 50_000_000
 *   DAILY_DO_REQUESTS         — optional, default  2_000_000
 *   DAILY_DO_DURATION_SEC     — optional, default    500_000   (active seconds)
 *
 * Exit codes:
 *   0  — all thresholds ok
 *   1  — one or more thresholds tripped
 *   2  — query failed (do NOT auto-disable on this)
 */

const ACCOUNT_ID = process.env.CF_ACCOUNT_ID;
const API_TOKEN = process.env.CF_API_TOKEN;

if (!ACCOUNT_ID || !API_TOKEN) {
  console.error("CF_ACCOUNT_ID and CF_API_TOKEN are required");
  process.exit(2);
}

const thresholds = {
  workerRequests: Number(process.env.DAILY_WORKER_REQUESTS || 5_000_000),
  d1RowsRead: Number(process.env.DAILY_D1_ROWS_READ || 50_000_000),
  doRequests: Number(process.env.DAILY_DO_REQUESTS || 2_000_000),
  doDurationSec: Number(process.env.DAILY_DO_DURATION_SEC || 500_000),
};

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
          sum { readQueries writeQueries rowsRead rowsWritten }
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
  if (!res.ok) {
    throw new Error(`GraphQL HTTP ${res.status}: ${await res.text()}`);
  }
  const data = await res.json();
  if (data.errors?.length) {
    throw new Error(`GraphQL errors: ${JSON.stringify(data.errors)}`);
  }
  return data.data.viewer.accounts[0] ?? {};
}

// Group rows by a dimension path, returning { [dimValue]: summedMetric } sorted desc.
function groupByDim(rows, dimPath, valueKey) {
  if (!Array.isArray(rows)) return {};
  const out = {};
  for (const r of rows) {
    let k = r?.dimensions;
    for (const p of dimPath) k = k?.[p];
    if (k == null) k = "(unknown)";
    out[k] = (out[k] ?? 0) + (r?.sum?.[valueKey] ?? 0);
  }
  return Object.fromEntries(Object.entries(out).sort((a, b) => b[1] - a[1]));
}

function totalOf(grouped) {
  return Object.values(grouped).reduce((a, b) => a + b, 0);
}

// Return the top entry if it alone exceeds the threshold, else null.
function topOffender(breakdown, threshold) {
  const first = Object.entries(breakdown)[0];
  if (!first) return null;
  return first[1] > threshold ? { name: first[0], value: first[1] } : null;
}

function formatRow(label, used, limit) {
  const pct = limit > 0 ? ((used / limit) * 100).toFixed(1) : "?";
  return `  ${label.padEnd(16)} ${String(used).padStart(12)} / ${String(limit).padStart(12)}  (${pct}%)`;
}

function main() {
  return callGraphQL().then((acct) => {
    // Build namespaceId -> scriptName map from the Invocations dataset,
    // then translate Periodic data (which lacks scriptName).
    const nsToScript = {};
    for (const r of acct.durableObjectsInvocationsAdaptiveGroups ?? []) {
      const ns = r?.dimensions?.namespaceId;
      const sn = r?.dimensions?.scriptName;
      if (ns && sn) nsToScript[ns] = sn;
    }
    const periodicByScript = {};
    for (const r of acct.durableObjectsPeriodicGroups ?? []) {
      const ns = r?.dimensions?.namespaceId;
      const sn = nsToScript[ns] ?? `namespace:${ns}`;
      periodicByScript[sn] = (periodicByScript[sn] ?? 0) + (r?.sum?.activeTime ?? 0);
    }
    const doDurationUsecByScript = Object.fromEntries(
      Object.entries(periodicByScript).sort((a, b) => b[1] - a[1])
    );

    const breakdown = {
      workerRequestsByScript: groupByDim(acct.workersInvocationsAdaptive, ["scriptName"], "requests"),
      doRequestsByScript: groupByDim(acct.durableObjectsInvocationsAdaptiveGroups, ["scriptName"], "requests"),
      doDurationUsecByScript,
      d1RowsReadByDB: groupByDim(acct.d1AnalyticsAdaptiveGroups, ["databaseId"], "rowsRead"),
    };

    const usage = {
      workerRequests: totalOf(breakdown.workerRequestsByScript),
      doRequests: totalOf(breakdown.doRequestsByScript),
      doDurationSec: Math.round(totalOf(doDurationUsecByScript) / 1_000_000),
      d1RowsRead: totalOf(breakdown.d1RowsReadByDB),
      d1RowsWritten: groupByDim(acct.d1AnalyticsAdaptiveGroups, ["databaseId"], "rowsWritten"),
    };
    usage.d1RowsWritten = totalOf(usage.d1RowsWritten);

    const reasons = [];
    const offenders = {};
    if (usage.workerRequests > thresholds.workerRequests) {
      reasons.push(`workerRequests ${usage.workerRequests} > ${thresholds.workerRequests}`);
      offenders.workerRequests = topOffender(breakdown.workerRequestsByScript, thresholds.workerRequests);
    }
    if (usage.d1RowsRead > thresholds.d1RowsRead) {
      reasons.push(`d1RowsRead ${usage.d1RowsRead} > ${thresholds.d1RowsRead}`);
      offenders.d1RowsRead = topOffender(breakdown.d1RowsReadByDB, thresholds.d1RowsRead);
    }
    if (usage.doRequests > thresholds.doRequests) {
      reasons.push(`doRequests ${usage.doRequests} > ${thresholds.doRequests}`);
      offenders.doRequests = topOffender(breakdown.doRequestsByScript, thresholds.doRequests);
    }
    if (usage.doDurationSec > thresholds.doDurationSec) {
      reasons.push(`doDurationSec ${usage.doDurationSec} > ${thresholds.doDurationSec}`);
      const thresholdUsec = thresholds.doDurationSec * 1_000_000;
      offenders.doDurationSec = topOffender(doDurationUsecByScript, thresholdUsec);
    }

    const tripped = reasons.length > 0;
    const result = { tripped, windowStart: startIso, windowEnd: nowIso, usage, thresholds, breakdown, offenders, reasons };

    console.log(JSON.stringify(result));

    const lines = [
      `[killswitch] window=${startIso}..${nowIso}`,
      formatRow("workerRequests:", usage.workerRequests, thresholds.workerRequests),
      formatRow("d1RowsRead:", usage.d1RowsRead, thresholds.d1RowsRead),
      formatRow("doRequests:", usage.doRequests, thresholds.doRequests),
      formatRow("doDurationSec:", usage.doDurationSec, thresholds.doDurationSec),
      "",
      "  workers by requests:",
      ...Object.entries(breakdown.workerRequestsByScript).slice(0, 8).map(([k, v]) => `    ${k.padEnd(30)} ${v}`),
      "",
      "  databases by rowsRead:",
      ...Object.entries(breakdown.d1RowsReadByDB).slice(0, 8).map(([k, v]) => `    ${k.padEnd(40)} ${v}`),
      "",
      `  tripped: ${tripped}${tripped ? ` (${reasons.join("; ")})` : ""}`,
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
