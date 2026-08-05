#!/usr/bin/env node
// Cross-validation: compare Rust backend output against TypeScript backend
// Tests complex processing endpoints with various query parameter combinations

import { strict as assert } from "node:assert";
import http from "node:http";

const TS_BASE = "http://127.0.0.1:3306/api/v1";
const RUST_BASE = "http://127.0.0.1:3304/api/v1";

const results = [];
let pass = 0;
let fail = 0;

function request(base, path, timeout = 15_000) {
    return new Promise((resolve, reject) => {
        const url = new URL(path, base);
        const req = http.get(url, { timeout }, (res) => {
            let data = "";
            res.on("data", (chunk) => (data += chunk));
            res.on("end", () => {
                try {
                    resolve(JSON.parse(data));
                } catch {
                    resolve(data);
                }
            });
        });
        req.on("error", reject);
        req.on("timeout", () => {
            req.destroy();
            reject(new Error(`Request timed out: ${url.pathname}`));
        });
    });
}

async function test(name, path, queryParams = {}) {
    const url = new URL(path, "http://127.0.0.1");
    for (const [k, v] of Object.entries(queryParams)) {
        url.searchParams.set(k, v);
    }
    const fullPath = url.pathname + (url.search || "");

    let tsData, rustData;
    try {
        tsData = await request(TS_BASE, fullPath);
    } catch (e) {
        results.push({ name, path: fullPath, status: "ts_error", error: e.message });
        fail++;
        return;
    }
    try {
        rustData = await request(RUST_BASE, fullPath);
    } catch (e) {
        results.push({ name, path: fullPath, status: "rust_error", error: e.message });
        fail++;
        return;
    }

    const match = deepEqual(tsData, rustData);
    const status = match ? "PASS" : "MISMATCH";
    results.push({ name, path: fullPath, status });
    if (match) {
        pass++;
    } else {
        fail++;
    }
}

function deepEqual(a, b) {
    if (a === b) return true;
    if (typeof a !== typeof b) return false;
    if (a === null || b === null) return a === b;
    if (Array.isArray(a)) {
        if (!Array.isArray(b) || a.length !== b.length) return false;
        return arraysEqual(a, b);
    }
    if (typeof a === "object") {
        if (typeof b !== "object" || b === null) return false;
        const skip = new Set(["timestamp", "requestId"]);
        const keysA = Object.keys(a).filter(k => !skip.has(k)).sort();
        const keysB = Object.keys(b).filter(k => !skip.has(k)).sort();
        if (keysA.length !== keysB.length) return false;
        for (let i = 0; i < keysA.length; i++) {
            if (keysA[i] !== keysB[i]) return false;
            if (!deepEqual(a[keysA[i]], b[keysB[i]])) return false;
        }
        return true;
    }
    return a === b;
}

function arraysEqual(a, b) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (!deepEqual(a[i], b[i])) return false;
    }
    return true;
}

// Run all test cases
async function run() {
    console.log("=== Cross-Validation: TS vs Rust Backend ===\n");

    // 1. Health check
    await test("Health", "/health");

    // 2. Reference data — champions
    await test("Reference: Champions", "/reference/champions");

    // 3. Reference data — items
    await test("Reference: Items", "/reference/items");

    // 4. Stats champions — default (ranked, no filters)
    await test("Stats: Champions (default)", "/stats/champions");

    // 5. Stats champions — ranked, all tiers
    await test("Stats: Champions (ranked)", "/stats/champions", { scope: "ranked" });

    // 6. Stats champions — casual scope
    await test("Stats: Champions (casual)", "/stats/champions", { scope: "casual" });

    // 7. Stats champions — with tier filters
    await test("Stats: Champions (tier 1-26)", "/stats/champions", {
        scope: "ranked",
        minTier: "1",
        maxTier: "26",
    });

    // 8. Stats champions — sorted by ban_rate, ascending
    await test("Stats: Champions (sorted ban_rate asc)", "/stats/champions", {
        sort: "ban_rate",
        order: "asc",
    });

    // 9. Stats champions — sorted by kda
    await test("Stats: Champions (sorted kda)", "/stats/champions", {
        sort: "kda",
        limit: "25",
    });

    // 10. Stats champions — low tier range
    await test("Stats: Champions (Iron-Gold)", "/stats/champions", {
        minTier: "1",
        maxTier: "13",
    });

    // 11. Stats champions — high tier range
    await test("Stats: Champions (Plat-Master)", "/stats/champions", {
        minTier: "14",
        maxTier: "26",
    });

    // 12. Stats overview — default
    await test("Stats: Overview (default)", "/stats/overview");

    // 13. Stats overview — with tier bounds
    await test("Stats: Overview (tier 1-26)", "/stats/overview", {
        minTier: "1",
        maxTier: "26",
    });

    // 14. Stats page-data
    await test("Stats: Page Data", "/stats/page-data", {
        scope: "ranked",
        minTier: "1",
        maxTier: "26",
    });

    // 15. Stats leaderboard
    await test("Stats: Leaderboard", "/stats/leaderboard", {
        limit: "25",
    });

    // 16. Search — players index
    await test("Search: Players", "/search/players", {
        q: "Test",
        limit: "10",
    });

    // 17. Search — matches index
    await test("Search: Matches", "/search/matches", {
        q: "Match",
        limit: "10",
    });

    // 18. Champions overview — default
    await test("Champions: Overview", "/champions/overview");

    // 19. Champions overview — with tier filters
    await test("Champions: Overview (filtered)", "/champions/overview", {
        minTier: "1",
        maxTier: "26",
    });

    // 20. Meta tierlist — default
    await test("Meta: Tierlist", "/meta/tierlist");

    // 21. Meta tierlist — with tier filters
    await test("Meta: Tierlist (filtered)", "/meta/tierlist", {
        minTier: "1",
        maxTier: "26",
    });

    // 22-23. Builds/top — skipped: TS has NaN bug, route not in spec

    // 24. Matches — list
    await test("Matches: List", "/matches", {
        limit: "10",
    });

    // 25. Matches — list with queue filter
    await test("Matches: List (queue 486)", "/matches", {
        queueId: "486",
        limit: "10",
    });

    // 26. Community — default
    await test("Community: Default", "/community");

    // 27. Community — with query
    await test("Community: Filtered", "/community", {
        limit: "10",
    });

    // 28. Stats: Performance Metrics
    await test("Stats: Performance Metrics", "/stats/performance-metrics");

    // 29. Stats: Performance Metrics (ranked, filtered)
    await test("Stats: Performance Metrics (ranked)", "/stats/performance-metrics", {
        scope: "ranked",
        minTier: "1",
        maxTier: "26",
    });

    // 30. Stats: Queues
    await test("Stats: Queues", "/stats/queues");

    // 31. Stats: Maps
    await test("Stats: Maps", "/stats/maps", { queueId: "486" });

    // 32. Stats: Items
    await test("Stats: Items", "/stats/items");

    // 33. Stats: Tiers
    await test("Stats: Tiers", "/stats/tiers");

    // 34. Stats: Leagues
    await test("Stats: Leagues", "/stats/leagues");

    // 35. Stats: Tier Population
    await test("Stats: Tier Population", "/stats/tier-population");

    console.log("\n=== Results ===");
    console.log(`Passed: ${pass}`);
    console.log(`Failed: ${fail}`);
    console.log(`Total:  ${pass + fail}`);

    // Write report
    const report = {
        timestamp: new Date().toISOString(),
        summary: { pass, fail, total: pass + fail },
        details: results,
    };

    // Collect mismatch details with raw responses
    const mismatchDetails = [];
    for (const r of results) {
        if (r.status === "MISMATCH") {
            const tsData = await request(TS_BASE, r.path);
            const rustData = await request(RUST_BASE, r.path);
            mismatchDetails.push({ name: r.name, path: r.path, ts: tsData, rust: rustData });
        }
    }

    const fs = await import("node:fs");
    fs.writeFileSync("cross-validation-report.json", JSON.stringify(report, null, 2));
    fs.writeFileSync("mismatch-details.json", JSON.stringify(mismatchDetails, null, 2));
    console.log("\nReport written to cross-validation-report.json");

    process.exit(fail > 0 ? 1 : 0);
}

run().catch((e) => {
    console.error("Validation failed:", e);
    process.exit(2);
});
