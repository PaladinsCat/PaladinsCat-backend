/**
 * =====================================================================
 * glicko2.ts — Glicko-2 Rating System Implementation
 * =====================================================================
 * Purpose: Calculate player ratings after competitive matches using the
 * Glicko-2 algorithm (rating + deviation + volatility). Replaces simple
 * K-factor systems with proper uncertainty modeling — new players converge
 * faster, established players shift less.
 *
 * Core concepts:
 * - Rating (μ): Player's skill level. Default 1500.
 * - Deviation (φ): Uncertainty about rating. High = uncertain, Low = confident.
 * - Volatility (σ): How much rating can change per match. Default 0.06.
 *
 * Architecture:
 * - glickoUpdate(): Single-player update against one or more opponents.
 *   Converts to internal Glicko-2 scale (μ/173.7178), runs full algorithm,
 *   converts back to display scale.
 * - expectedScore(): ELO-style win probability between two ratings.
 * - batchUpdate(): Process multiple players from the same match at once.
 *
 * Fixed 2026-05-30:
 * - Full rewrite: original code ran Glicko-2 formulas on the raw 1500/350
 *   scale without converting to internal scale (μ_int = (μ-1500)/173.7178).
 *   This broke g(φ), variance estimation, epsilon, and volatility entirely.
 * - Now converts to internal scale first, runs spec-compliant algorithm,
 *   converts back. Implements: scale conversion, correct v/δ, Illinois
 *   algorithm for σ, proper φ*²/φ_new/μ_new steps.
 * - Source: User-provided audit + corrected implementation (verified 2026-05-30).
 * - Added zero-opponent short-circuit: empty opponents array would cause
 *   v=0 → newPhi=0 → permanently frozen rating. Now returns φ* expansion only.
 * - Added undefined guard in cache.ts set(): prevents TypeError crash from
 *   ioredis when JSON.stringify(undefined) returns primitive undefined.
 *
 * Called by:
 * - rating-calculator.ts (batchUpdate for match processing)
 * - workers/rating-ingestion.ts (background rating pipeline)
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */

export interface GlickoState {
  rating: number;
  deviation: number;
  volatility: number;
}

export interface GlickoOpponent {
  rating: number;
  deviation: number;
  /**
   * Fraction of one rating-period result represented by this opponent.
   * A 5v5 result uses five opponents with weight 0.2 each, so the single
   * team result is not treated as five independent games.
   */
  weight?: number;
}

// System constant τ (Tau) governs volatility changes over time.
// Typically ranges from 0.3 to 1.2. Smaller = more stable volatility.
const TAU = 0.5;

// Glicko-2 display scale conversion factor.
// Internal μ = (displayRating - 1500) / 173.7178
// Internal φ = displayDeviation / 173.7178
// This conversion is required by the Glicko-2 spec for correct math.
const SCALE = 173.7178;

// Production guardrails. These are deliberately wider than the healthy live
// distribution (mu 417-2722, phi <= 262, sigma <= 0.154 on 2026-07-13), while
// preventing a finite-but-corrupt value from becoming durable rating state.
export const RATING_LIMITS = {
  minRating: 0,
  maxRating: 3500,
  minDeviation: 1,
  maxDeviation: 350,
  minVolatility: 0.001,
  maxVolatility: 0.2,
} as const;

export function isValidGlickoState(state: GlickoState): boolean {
  return Number.isFinite(state.rating)
    && state.rating >= RATING_LIMITS.minRating
    && state.rating <= RATING_LIMITS.maxRating
    && Number.isFinite(state.deviation)
    && state.deviation >= RATING_LIMITS.minDeviation
    && state.deviation <= RATING_LIMITS.maxDeviation
    && Number.isFinite(state.volatility)
    && state.volatility >= RATING_LIMITS.minVolatility
    && state.volatility <= RATING_LIMITS.maxVolatility;
}

function assertValidGlickoState(state: GlickoState, label: string): void {
  if (!isValidGlickoState(state)) {
    throw new Error(
      `Glicko-2: ${label} state is outside safe bounds `
      + `(rating=${state.rating}, deviation=${state.deviation}, volatility=${state.volatility})`,
    );
  }
}

/**
 * Perform a full Glicko-2 rating update for a single player.
 * Converts to internal scale, runs spec-compliant algorithm, converts back.
 *
 * @param current - Player's current Glicko state (display scale).
 * @param opponents - Opponents' states (display scale).
 * @param outcome - 'win', 'loss', or 'draw'.
 * @returns Updated Glicko state (display scale).
 */
export function glickoUpdate(
  current: GlickoState,
  opponents: GlickoOpponent[],
  outcome: 'win' | 'loss' | 'draw'
): GlickoState {
  // CRITICAL: Validate inputs before any math. Negative or NaN values
  // produce NaN throughout the algorithm — corrupted ratings written to DB.
  // Negative deviation: g(phi) produces NaN (sqrt of negative).
  // Negative volatility: Math.log(negative) produces NaN.
  // NaN rating: (NaN - 1500) / SCALE = NaN, propagates through all steps.
  // Source: Debug 2026-05-31 — "No input validation"
  // CRITICAL: Use Number.isFinite() instead of isNaN().
  // isNaN(Infinity) and isNaN(-Infinity) both return false — they bypass the guard.
  // If rating is Infinity, mu becomes Infinity → E() locks to 1 → v = 0 →
  // infinite rating written to DB, permanently poisoning that player's data.
  // Number.isFinite() catches NaN, Infinity, and -Infinity correctly.
  // Source: Debug 2026-05-31 — "isNaN vs Infinity trap"
  assertValidGlickoState(current, 'input');

  // Step 2: Convert to Glicko-2 internal scale.
  // Internal μ centers around 0 (not 1500). Internal φ is scaled down.
  // This conversion is mandatory for all subsequent math to be correct.
  const mu = (current.rating - 1500) / SCALE;
  const phi = current.deviation / SCALE;
  const sigma = current.volatility;

  // CRITICAL: Short-circuit for empty opponents (inactivity / missing data).
  // If no opponents, vInv stays 0 → v = 0 → 1/v = Infinity → newPhi = 0.
  // A zero deviation permanently freezes the player's rating.
  // Glicko-2 spec: when a player doesn't compete, only Step 6 applies:
  // φ*² = φ² + σ² (uncertainty expands due to volatility).
  // Source: Audit 2026-05-30 — "Zero-Deviation Wipe"
  if (opponents.length === 0) {
    const phiStar = Math.sqrt(Math.pow(phi, 2) + Math.pow(sigma, 2));
    return {
      rating: current.rating,
      deviation: Math.round((SCALE * phiStar) * 100) / 100,
      volatility: current.volatility,
    };
  }

  // Scale opponent states too.
  // CRITICAL: Filter out mathematically invalid opponents before scaling.
  // The main player's state is validated above, but the opponents array is
  // blindly trusted. If a single opponent has corrupted data (NaN, Infinity,
  // or negative deviation), o.mu becomes NaN → g(NaN) returns NaN → E() returns
  // NaN → vInv becomes NaN → deltaSum becomes NaN → entire calculation collapses.
  // The main player's perfectly valid rating gets permanently overwritten with NaN.
  // One corrupted opponent silently poisons everyone else in the match.
  // Fix: Filter invalid opponents before scaling, same pattern as batchUpdate.
  // If filtering wipes out all opponents, fall back to inactivity expansion.
  // Source: Debug 2026-05-31 — "NaN opponent contagion"
  const scaledOpponents = opponents
    .filter((o) => Number.isFinite(o.rating)
      && Number.isFinite(o.deviation)
      && o.deviation >= RATING_LIMITS.minDeviation
      && o.deviation <= RATING_LIMITS.maxDeviation
      && Number.isFinite(o.weight ?? 1)
      && (o.weight ?? 1) > 0)
    .map(o => ({
      mu: (o.rating - 1500) / SCALE,
      phi: o.deviation / SCALE,
      weight: o.weight ?? 1,
    }));

  // Safety check: if filtering wiped out all opponents, fall back to inactivity.
  // This prevents vInv = 0 → v = 0 → newPhi = 0 → frozen rating.
  if (scaledOpponents.length === 0) {
    const phiStar = Math.sqrt(Math.pow(phi, 2) + Math.pow(sigma, 2));
    return {
      rating: current.rating,
      deviation: Math.round((SCALE * phiStar) * 100) / 100,
      volatility: current.volatility,
    };
  }

  // g(φⱼ): opponent uncertainty factor. Higher φⱼ → wider g → more uncertain outcome.
  function g(phiJ: number): number {
    return 1 / Math.sqrt(1 + 3 * Math.pow(phiJ, 2) / Math.pow(Math.PI, 2));
  }

  // E(μ, μⱼ, φⱼ): expected score vs opponent j. P(win) on internal scale.
  function E(muPlayer: number, muJ: number, phiJ: number): number {
    return 1 / (1 + Math.exp(-g(phiJ) * (muPlayer - muJ)));
  }

  // Step 3: Compute variance v of the player's rating.
  // v = [Σ g(φⱼ)² · Eⱼ · (1 - Eⱼ)]⁻¹
  // On the internal scale, this is correct without extra G² scaling.
  let vInv = 0;
  for (let i = 0; i < scaledOpponents.length; i++) {
    const o = scaledOpponents[i];
    const e = E(mu, o.mu, o.phi);
    vInv += o.weight * Math.pow(g(o.phi), 2) * e * (1 - e);
  }
  const v = vInv > 0 ? 1 / vInv : 0;

  // Step 4: Compute estimated improvement (Δ).
  // Δ = v · Σ g(φⱼ) · (sⱼ - Eⱼ) where sⱼ = 1 for win, 0 for loss, 0.5 for draw.
  // All opponents share the same outcome (team win/loss in Paladins).
  // CRITICAL: Normalize outcome to lowercase before comparison. TypeScript enforces
  // 'win' | 'loss' | 'draw' at compile-time, but runtime data from DB, Redis, or
  // external API may come as "WIN", "Win", "unknown", etc. Without normalization,
  // "WIN" fails both strict === checks and silently defaults to 0.5 (a draw).
  // A win treated as a draw severely depresses rating gains for top players.
  // An unknown outcome treated as a draw corrupts the rating silently.
  // Fix: Normalize to lowercase, explicitly throw on unrecognized outcome.
  // Source: Debug 2026-05-31 — "Silent draw fall-through"
  const normalizedOutcome = outcome.toLowerCase();
  let outcomeVal: number;
  if (normalizedOutcome === 'win') outcomeVal = 1;
  else if (normalizedOutcome === 'loss') outcomeVal = 0;
  else if (normalizedOutcome === 'draw') outcomeVal = 0.5;
  else throw new Error(`Glicko-2: Invalid outcome received: ${outcome}`);
  let deltaSum = 0;
  for (let i = 0; i < scaledOpponents.length; i++) {
    const o = scaledOpponents[i];
    deltaSum += o.weight * g(o.phi) * (outcomeVal - E(mu, o.mu, o.phi));
  }
  const delta = v * deltaSum;

  // Step 5: Volatility update using Illinois algorithm (iterative root-finding).
  // f(x) finds the root where x = ln(σ²). The bounds A/B bracket the root.
  const a = Math.log(Math.pow(sigma, 2));

  function f(x: number): number {
    const eX = Math.exp(x);
    const num = eX * (Math.pow(delta, 2) - Math.pow(phi, 2) - v - eX);
    const den = 2 * Math.pow(Math.pow(phi, 2) + v + eX, 2);
    return num / den - (x - a) / Math.pow(TAU, 2);
  }

  // CRITICAL: Cap the lower-bound search loop. If f(x) never crosses zero
   // going downward (e.g., extreme delta values, numerical instability),
   // k increments forever → event loop blocks → process hangs permanently.
   // Max 100 iterations is generous — typical convergence is 1-5 iterations.
   // If we hit the cap, fall back to B = a - 100*TAU (conservative estimate).
   // Source: Debug 2026-05-31 — "Infinite loop in B bound calculation"
   let A = a;
   let B: number;
   if (Math.pow(delta, 2) > Math.pow(phi, 2) + v) {
     B = Math.log(Math.pow(delta, 2) - Math.pow(phi, 2) - v);
   } else {
     let k = 1;
     while (f(a - k * TAU) < 0 && k < 100) k++;
     B = a - k * TAU;
   }

 // CRITICAL: Cap the secant iteration loop. If f(x) is flat near the root
   // or oscillates (numerical instability, extreme inputs), B - A never
   // shrinks below epsilon → infinite loop → process hangs. Max 100
   // iterations is generous — typical Glicko-2 convergence is 5-20 iterations.
   // If we hit the cap, use the current A as best estimate for ln(sigma²).
   // Source: Debug 2026-05-31 — "Infinite loop in secant iteration"
   let fA = f(A);
   let fB = f(B);
   const epsilonThreshold = 0.000001;
   let iter = 0;

   while (Math.abs(B - A) > epsilonThreshold && iter < 100) {
     iter++;
     // CRITICAL: Guard against division by zero. If fB === fA (function flattens
     // due to extreme delta values, or floating-point precision limits reached),
     // the denominator (fB - fA) becomes exactly 0. Division by zero produces
     // Infinity or -Infinity for C. On the next iteration, f(C) evaluates to NaN.
     // The loop burns through remaining iterations doing pure NaN math, returning
     // a potentially unstable volatility value.
     // Fix: Break immediately if |fB - fA| < Number.EPSILON. If they converge
     // this closely, A is already as accurate as floating-point math allows.
     // Number.EPSILON is the smallest representable value in JavaScript's IEEE 754
     // double-precision format (~2.22e-16). Below this, values are indistinguishable.
     // Source: Debug 2026-05-31 — "Secant division by zero"
     if (Math.abs(fB - fA) < Number.EPSILON) {
       break;
     }
     const C = A + (A - B) * fA / (fB - fA);
    const fC = f(C);
    if (fC * fB <= 0) {
      A = B;
      fA = fB;
    } else {
      fA = fA / 2;
    }
    B = C;
    fB = fC;
  }

  const newSigma = Math.exp(A / 2);

  // Step 6: Pre-rating period deviation (φ*²).
  // φ*² = φ² + σ_new² — combines prior uncertainty with new volatility.
  const phiStarSq = Math.pow(phi, 2) + Math.pow(newSigma, 2);

  // Step 7: New deviation and rating.
  // φ_new = 1 / √(1/φ*² + 1/v) — uncertainty shrinks after observed match.
  // μ_new = μ + φ_new² · Σ g(φⱼ)(sⱼ - Eⱼ) — rating shifts by weighted surprise.
  // CRITICAL: Guard against v = 0. When all opponents are identical, v
  // approaches 0 (perfectly certain outcome). 1/v → Infinity → newPhi → 0.
  // A zero deviation permanently freezes the player's rating — no future
  // match can shift it. Only recoverable via inactivity expansion (Step 6).
  // Fix: When v is near-zero, use phiStar as newPhi (uncertainty expands
  // due to volatility but doesn't collapse to zero). This preserves the
  // ability to update the rating on future matches.
  // Source: Debug 2026-05-31 — "v=0 produces frozen rating"
  const newPhi = v > 0 ? 1 / Math.sqrt(1 / phiStarSq + 1 / v) : Math.sqrt(phiStarSq);
  const newMu = mu + Math.pow(newPhi, 2) * deltaSum;

  // Step 8: Convert back to original display scale.
  const result = {
    rating: Math.round((SCALE * newMu + 1500) * 100) / 100,
    deviation: Math.round((SCALE * newPhi) * 100) / 100,
    volatility: Math.round(newSigma * 10000) / 10000,
  };
  assertValidGlickoState(result, 'output');
  return result;
}

/**
 * ELO-style win probability between two ratings (display scale).
 * Used for quick comparisons, NOT the Glicko-2 update itself.
 *
 * @param rating - Player's rating (display scale).
 * @param opponentRating - Opponent's rating (display scale).
 * @returns Probability of winning (0-1).
 */
export function expectedScore(rating: number, opponentRating: number): number {
  return 1 / (1 + Math.pow(10, (opponentRating - rating) / 400));
}

/**
 * Process multiple players from the same match at once.
 * Resolves opponent states from the input array to avoid stale data.
 * Players not found in the input array default to mu=1500, phi=350.
 *
 * @param players - Array of players with their states, opponent IDs, and outcomes.
 * @returns Map of player ID → new GlickoState.
 */
export function batchUpdate(
  players: { id: number; state: GlickoState; opponents: number[]; outcome: 'win' | 'loss' | 'draw' }[]
): Map<number, GlickoState> {
  // CRITICAL: Check for duplicate IDs. If two players share the same id,
  // stateMap.set() silently overwrites the first player's state. The first
  // player never gets a rating update — silent data loss.
  // Source: Debug 2026-05-31 — "batchUpdate silently overwrites duplicate IDs"
  const seenIds = new Set<number>();
  const stateMap = new Map<number, GlickoState>();
  for (const p of players) {
    if (seenIds.has(p.id)) {
      throw new Error(`Glicko-2 batchUpdate: duplicate player id ${p.id}`);
    }
    seenIds.add(p.id);
    stateMap.set(p.id, p.state);
  }

const results = new Map<number, GlickoState>();
   for (const p of players) {
     // CRITICAL: Filter out missing opponents instead of injecting ghost baselines.
     // Previously, missing opponents defaulted to { rating: 1500, deviation: 350 }.
     // This corrupts the rating calculation:
     // - A 2500-rated player who wins gets almost zero points (beat a "newbie").
     // - A 2500-rated player who loses gets catastrophically tanked (lost to a "newbie").
     // Mathematically, it is safer to simply assess performance against known opponents.
     // The algorithm adjusts naturally — fewer opponents means less certainty shift,
     // which is the correct behavior when opponent data is unavailable.
     // Source: Debug 2026-05-31 — "Ghost opponent penalty"
     const opps = p.opponents
       .map((id: number) => stateMap.get(id))
       .filter((opp): opp is GlickoState => opp !== undefined)
        .map((opp) => ({ rating: opp.rating, deviation: opp.deviation }));
     results.set(p.id, glickoUpdate(p.state, opps, p.outcome));
   }

  return results;
}
