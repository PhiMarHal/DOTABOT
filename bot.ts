/**
 * Defense of the Agents — Melee Classic bot (v0.3)
 * =================================================
 * Locked to: melee + "classic" skin (Defensive Aura), Game 3 (AI ranked), mid stack.
 *
 * ARCHITECTURE (changed after the v0.2 field test):
 *   - REST is the BACKBONE. It is proven to work: presence detection, ability
 *     picks, lane switches, recall/sprint/stroll can ALL be driven by
 *     GET /api/game/state + POST /api/strategy/deployment. The bot is fully
 *     functional on REST alone (~1.5s reactions).
 *   - The WebSocket is an ACCELERATOR. When it delivers snapshots, the bot
 *     upgrades to 20 Hz: positional awareness, measured-DPS recall, escapes.
 *     If it delivers nothing (as observed in the field), a watchdog says so
 *     loudly and rotates to a fallback host — and the bot keeps playing on REST.
 *
 * BUGS THIS VERSION FIXES (from the live run):
 *   1. Join-retry spam WAS lane spam. Every deployment POST carrying `heroLane`
 *      is a lane command; retrying the join every 5s pressed "mid" every 5s,
 *      which (mid while in mid) makes the hero squeeze-walk instead of attack.
 *      Now the join deployment is sent EXACTLY ONCE per round; confirmation
 *      comes from REST state, not from the (possibly dead) WebSocket.
 *   2. Ability picking was gated behind WS-based join confirmation, so it never
 *      ran — the server random-assigned abilities. Picks are now driven by the
 *      REST poll, unconditionally.
 *   3. Silent WS failure. Parse errors are now logged (once per connection),
 *      a watchdog reports "no snapshots", and hosts rotate automatically.
 *   4. Lane discipline. All lane commands flow through one choke point with
 *      server-lane dedup + hysteresis + a hold timer. A warrior that isn't
 *      rotating for a reason HOLDS STILL AND SWINGS.
 *
 * Run:  npx tsx bot.ts   (reads DOTA_API_KEY / DOTA_AGENT_NAME from .env)
 */

import "dotenv/config";
import WebSocket from "ws";

// ----------------------------- Config ----------------------------------------

const REST_BASE = "https://game.defenseoftheagents.com"; // proven working
const WS_HOSTS = [
    "wss://game.defenseoftheagents.com",                    // documented
    "wss://wc2-agentic-dev-3o6un.ondigitalocean.app",       // origin fallback
];

const API_KEY = process.env.DOTA_API_KEY ?? "";
const AGENT_NAME = process.env.DOTA_AGENT_NAME ?? "";
const GAME_ID = 3;

if (!API_KEY || !AGENT_NAME) {
    console.error("Missing DOTA_API_KEY / DOTA_AGENT_NAME — create .env (see .env.example).");
    process.exit(1);
}

const CFG = {
    heroClass: "melee" as const,
    skin: "classic" as const,
    // Tried in order on first deploy; a rejected item falls through to the next.
    itemPreference: ["cat_ears", "ring_of_regen"] as (string | null)[],
    homeLane: "mid" as Lane,

    // Cadence
    restPollMs: 1500,          // REST state poll (backbone)
    macroMs: 400,              // decision loop
    wsSendGapMs: 300,          // min gap between WS action sends (recall bypasses)
    restPostGapMs: 900,        // min gap between ANY REST POSTs (recall bypasses to 400)
    joinConfirmGraceMs: 25_000,// how long we wait to see ourselves before ONE retry
    rejoinDelayMs: 8_000,      // wait after a round ends before rejoining
    wsWatchdogMs: 8_000,       // no snapshot within this after connect -> rotate host
    liveFreshMs: 1_200,        // WS considered "live" if last snapshot newer than this

    // Recall — LIVE mode (20 Hz, measured damage; press late, channel is invuln)
    recallFloorLive: 0.06,
    predictLookaheadMs: 500,
    dpsWindowLiveMs: 600,
    xpRange: 350,              // enemy hero within this would bank our 200XP bounty

    // Recall — REST mode (coarse 1.5s data: leave a bigger buffer, tower hits ~70)
    recallFloorRest: 0.14,
    dpsWindowRestMs: 4_000,

    // Lanes (hysteresis so we HOLD and attack instead of ping-ponging)
    midBailAdv: -3,            // leave mid only when down by 3+ units
    midRestackAdv: 2,          // return only when mid is up by 2+ units
    laneHoldMs: 6_000,         // min gap between non-emergency lane commands
    escapeSpamMs: 1_500,       // emergency lane re-issue cadence (commit an escape)

    // Geometry (LIVE mode only)
    threatRadius: 230,
    nearBaseRadius: 650,

    // Client-side cooldown clocks (ms)
    recallCdMs: 120_000,
    sprintCdMs: 25_000,
    strollCdMs: 25_000,
};

const AURA_IDS = ["fortitude", "defensive_aura"]; // classic skin may report either id

// ----------------------------- Types ------------------------------------------

type Lane = "top" | "mid" | "bot";
type Faction = "human" | "orc";
type HeroClass = "melee" | "ranged" | "mage";

interface Ability { id: string; level: number; cooldownRemaining?: number; cooldownTotal?: number; activeRemaining?: number; }
interface Unit {
    id: number; type: string; faction: Faction; x: number; y: number;
    hp: number; maxHp: number; lane: Lane; isHero?: boolean; ownerName?: string;
    shielded?: boolean; recallShielded?: boolean; abilities?: Ability[];
}
interface Building { id: number; faction: Faction; type: string; x: number; y: number; hp: number; maxHp: number; lane?: Lane; }
interface ScoreEntry {
    name: string; faction: Faction; heroClass: HeroClass; lane: Lane;
    level: number; hp: number; maxHp: number; alive: boolean;
    abilities: Ability[]; abilityChoices?: string[]; recallCooldownMs?: number; respawnTimer?: number;
}
interface Snapshot {
    tick: number; units: Unit[]; buildings: Building[]; arrows?: any[]; zones?: any[]; events?: any[];
    winner: Faction | null; heroScoreboard?: ScoreEntry[];
}

// REST /api/game/state (documented shape)
interface RestHero {
    name: string; faction: Faction; class: HeroClass; lane: Lane;
    hp: number; maxHp: number; alive: boolean; level: number;
    abilities: { id: string; level: number }[]; abilityChoices?: string[]; recallCooldownMs?: number;
}
interface RestState {
    tick: number;
    lanes: Record<Lane, { human: number; orc: number; frontline: number }>;
    towers: { faction: Faction; lane: Lane; hp: number; maxHp: number; alive: boolean }[];
    bases: Record<Faction, { hp: number; maxHp: number }>;
    heroes: RestHero[];
    winner: Faction | null;
}

// A source-agnostic view of one lane, built from WS or REST.
interface LaneStat {
    lane: Lane; friendly: number; enemy: number; adv: number; enemyHeroesHere: number;
    ownTowerAlive: boolean; ownTowerHp: number; enemyTowerAlive: boolean; enemyTowerHp: number;
    frontline?: number; // REST only: +100 = at orc base, -100 = at human base
}

// A source-agnostic view of our hero.
interface MeView {
    faction: Faction; lane: Lane; level: number; hp: number; maxHp: number; alive: boolean;
    abilities: Ability[]; abilityChoices?: string[]; recallCooldownMs?: number;
}

// ----------------------------- State ------------------------------------------

let ws: WebSocket | null = null;
let wsHostIdx = 0;
let wsFrames = 0;
let wsParseErrLogged = false;
let lastWsSnapshotAt = 0;
let snap: Snapshot | null = null;

let rest: RestState | null = null;
let restPolling = false;

let myFaction: Faction | null = null;
let serverLane: Lane | null = null;       // lane per the server (REST or live WS)
let currentLaneTarget: Lane = CFG.homeLane; // lane we last commanded
let lastLaneCmdAt = 0;

let deploySentAt = 0;                     // 0 = not sent this round
let deployInFlight = false;
let joinedConfirmed = false;
let itemIdx = 0;
let roundOverAt = 0;

let lastWsSendAt = 0;
let lastRestPostAt = 0;
let restBackoffUntil = 0;

let lastPickPostAt = 0;
let lastPickId = "";

const hpHistory: { t: number; hp: number; maxHp: number }[] = [];
let prevBaseHp: number | null = null;

const cd = { recall: 0, sprint: 0, stroll: 0 };
const now = () => Date.now();
const ready = (k: keyof typeof cd) => now() >= cd[k];
const live = () => now() - lastWsSnapshotAt < CFG.liveFreshMs;

// ----------------------------- Small helpers ----------------------------------

const enemyOf = (f: Faction): Faction => (f === "human" ? "orc" : "human");
const dist = (a: { x: number; y: number }, b: { x: number; y: number }) => Math.hypot(a.x - b.x, a.y - b.y);
const hpFracOf = (m: { hp: number; maxHp: number }) => (m.maxHp ? m.hp / m.maxHp : 1);

// frontline in MY direction of advance: positive = pushed toward the enemy.
const myAdvance = (frontline: number) => (myFaction === "human" ? frontline : -frontline);

function recordHp(hp: number, maxHp: number) {
    const t = now();
    hpHistory.push({ t, hp, maxHp });
    while (hpHistory.length && t - hpHistory[0].t > 6000) hpHistory.shift();
}

// HP lost per second, measured over up to `windowMs` of history.
function incomingDps(windowMs: number): number {
    if (hpHistory.length < 2) return 0;
    const b = hpHistory[hpHistory.length - 1];
    let a = hpHistory[0];
    for (let i = hpHistory.length - 1; i >= 0; i--) {
        if (b.t - hpHistory[i].t >= windowMs) { a = hpHistory[i]; break; }
    }
    const dt = (b.t - a.t) / 1000;
    return dt > 0 ? Math.max(0, (a.hp - b.hp) / dt) : 0;
}

// ----------------------------- Unified world views ----------------------------

function meView(): MeView | null {
    if (live()) {
        const s = snap?.heroScoreboard?.find((h) => h.name === AGENT_NAME);
        if (s) return s;
    }
    const r = rest?.heroes?.find((h) => h.name === AGENT_NAME);
    if (!r) return null;
    return {
        faction: r.faction, lane: r.lane, level: r.level, hp: r.hp, maxHp: r.maxHp,
        alive: r.alive, abilities: r.abilities ?? [], abilityChoices: r.abilityChoices,
        recallCooldownMs: r.recallCooldownMs,
    };
}

function myUnit(): Unit | undefined { // LIVE only
    return snap?.units.find((u) => u.isHero && u.ownerName === AGENT_NAME);
}

function laneStats(): LaneStat[] {
    const ef = enemyOf(myFaction!);
    const lanes: Lane[] = ["top", "mid", "bot"];

    if (live() && snap) {
        return lanes.map((lane) => {
            const u = snap!.units.filter((x) => x.lane === lane);
            const et = snap!.buildings.find((b) => b.type === "tower" && b.faction === ef && b.lane === lane);
            const ot = snap!.buildings.find((b) => b.type === "tower" && b.faction === myFaction && b.lane === lane);
            return {
                lane,
                friendly: u.filter((x) => x.faction === myFaction).length,
                enemy: u.filter((x) => x.faction === ef).length,
                adv: u.filter((x) => x.faction === myFaction).length - u.filter((x) => x.faction === ef).length,
                enemyHeroesHere: u.filter((x) => x.isHero && x.faction === ef).length,
                ownTowerAlive: !!ot && ot.hp > 0, ownTowerHp: ot?.hp ?? 0,
                enemyTowerAlive: !!et && et.hp > 0, enemyTowerHp: et?.hp ?? 0,
            };
        });
    }

    if (rest) {
        return lanes.map((lane) => {
            const l = rest!.lanes[lane];
            const friendly = l?.[myFaction!] ?? 0;
            const enemy = l?.[ef] ?? 0;
            const et = rest!.towers.find((t) => t.faction === ef && t.lane === lane);
            const ot = rest!.towers.find((t) => t.faction === myFaction && t.lane === lane);
            return {
                lane, friendly, enemy, adv: friendly - enemy,
                enemyHeroesHere: rest!.heroes.filter((h) => h.faction === ef && h.alive && h.lane === lane).length,
                ownTowerAlive: !!ot?.alive, ownTowerHp: ot?.hp ?? 0,
                enemyTowerAlive: !!et?.alive, enemyTowerHp: et?.hp ?? 0,
                frontline: l?.frontline,
            };
        });
    }
    return [];
}

function enemyPhysicalShare(): number {
    const ef = myFaction ? enemyOf(myFaction) : null;
    const roster: { faction: Faction; heroClass: HeroClass }[] =
        rest?.heroes?.map((h) => ({ faction: h.faction, heroClass: h.class })) ??
        snap?.heroScoreboard?.map((h) => ({ faction: h.faction, heroClass: h.heroClass })) ?? [];
    const es = roster.filter((h) => h.faction === ef);
    if (!es.length) return 0.5;
    return es.filter((h) => h.heroClass === "melee" || h.heroClass === "ranged").length / es.length;
}

// ----------------------------- REST I/O ----------------------------------------

async function restPost(body: Record<string, any>, opts: { urgent?: boolean } = {}): Promise<{ ok: boolean; status: number; text: string }> {
    const gap = opts.urgent ? 400 : CFG.restPostGapMs;
    if (now() < restBackoffUntil || now() - lastRestPostAt < gap) return { ok: false, status: 0, text: "throttled" };
    lastRestPostAt = now();
    try {
        const r = await fetch(`${REST_BASE}/api/strategy/deployment`, {
            method: "POST",
            headers: { "Content-Type": "application/json", Authorization: `Bearer ${API_KEY}` },
            body: JSON.stringify(body),
        });
        const text = await r.text();
        if (r.status === 429) { restBackoffUntil = now() + 10_000; console.log("[rest] 429 — backing off 10s"); }
        else if (!r.ok) console.log(`[rest] deploy ${r.status}: ${text}`);
        else {
            try { const j = JSON.parse(text); if (j.warning) console.log(`[rest] warning: ${j.warning}`); } catch { }
        }
        return { ok: r.ok, status: r.status, text };
    } catch (e) {
        console.log("[rest] POST error:", (e as Error).message);
        return { ok: false, status: 0, text: String(e) };
    }
}

async function pollRest() {
    if (restPolling) return;
    restPolling = true;
    try {
        const r = await fetch(`${REST_BASE}/api/game/state?game=${GAME_ID}`);
        if (!r.ok) { console.log(`[rest] state ${r.status}`); return; }
        rest = (await r.json()) as RestState;

        // --- round end / restart handling ---
        if (rest.winner) {
            if (!roundOverAt) {
                roundOverAt = now();
                console.log(`[round] over — ${rest.winner} won. Rejoining next round…`);
            }
            resetRoundState(false);
            return;
        }
        if (roundOverAt && now() - roundOverAt < CFG.rejoinDelayMs) return; // let the new round settle
        roundOverAt = 0;

        const meR = rest.heroes.find((h) => h.name === AGENT_NAME);

        if (meR) {
            if (!joinedConfirmed) console.log(`[join] confirmed in game as ${meR.faction} ${meR.class} (${meR.lane})`);
            joinedConfirmed = true;
            myFaction = meR.faction;
            // Server lane is truth, except right after we commanded a switch (3s sequence).
            if (now() - lastLaneCmdAt > 3500) serverLane = meR.lane;
            if (typeof meR.recallCooldownMs === "number" && meR.recallCooldownMs > 0)
                cd.recall = Math.max(cd.recall, now() + meR.recallCooldownMs);
            if (!live() && meR.alive) recordHp(meR.hp, meR.maxHp);

            // --- ability picking (REST-authoritative; this is the reliable path) ---
            await maybePickAbility(meR);
        } else {
            joinedConfirmed = false;
            await maybeDeploy();
        }
    } catch (e) {
        console.log("[rest] poll error:", (e as Error).message);
    } finally {
        restPolling = false;
    }
}

async function maybePickAbility(meR: RestHero) {
    const choices = meR.abilityChoices;
    if (!choices?.length) { lastPickId = ""; return; }
    // Don't hammer: if we just posted this same pick, give the server a few seconds.
    const pick = nextAbilityPick({
        heroClass: meR.class, abilities: meR.abilities as Ability[], abilityChoices: choices,
    } as ScoreEntry);
    if (!pick) return;
    if (pick === lastPickId && now() - lastPickPostAt < 5000) return;
    const res = await restPost({ abilityChoice: pick });
    if (res.ok) {
        lastPickId = pick; lastPickPostAt = now();
        console.log(`[act] ability -> ${pick}   (choices were: ${choices.join(", ")})`);
    }
}

async function maybeDeploy() {
    if (deployInFlight) return;
    const retryDue = deploySentAt > 0 && now() - deploySentAt > CFG.joinConfirmGraceMs;
    if (deploySentAt > 0 && !retryDue) return; // sent, waiting for confirmation — DO NOT respam
    deployInFlight = true;
    try {
        const item = CFG.itemPreference[itemIdx] ?? null;
        const body: Record<string, any> = {
            heroClass: CFG.heroClass, heroLane: CFG.homeLane, skin: CFG.skin,
            message: "melee classic online",
        };
        if (item) body.equippedItem = item;
        console.log(`[join] deploying melee/classic @ ${CFG.homeLane}${item ? ` +${item}` : ""}${retryDue ? " (retry)" : ""}`);
        const res = await restPost(body, { urgent: true });
        if (res.ok) {
            deploySentAt = now();
            lastLaneCmdAt = now();            // the join deploy IS a lane command — count it
            currentLaneTarget = CFG.homeLane;
        } else if (res.status === 400 && /item|equip/i.test(res.text) && itemIdx < CFG.itemPreference.length - 1) {
            itemIdx++;                        // item rejected -> fall through to next preference
            console.log(`[join] item rejected, falling back to ${CFG.itemPreference[itemIdx] ?? "no item"}`);
        } else if (res.status === 400 && /full/i.test(res.text)) {
            deploySentAt = now();             // game full: wait a full grace period before retrying
        }
        // throttled/failed -> deploySentAt stays 0 and the next poll tries again
    } finally {
        deployInFlight = false;
    }
}

function resetRoundState(hard: boolean) {
    joinedConfirmed = false;
    deploySentAt = 0;
    serverLane = null;
    currentLaneTarget = CFG.homeLane;
    hpHistory.length = 0;
    prevBaseHp = null;
    cd.recall = 0; cd.sprint = 0; cd.stroll = 0;
    lastPickId = "";
    if (hard) { snap = null; rest = null; }
}

// ----------------------------- WebSocket (accelerator) ------------------------

function wsConnect() {
    const host = WS_HOSTS[wsHostIdx];
    const url = `${host}/?game=${GAME_ID}`;
    let gotSnapshotThisConn = false;
    wsParseErrLogged = false;
    ws = new WebSocket(url);

    const watchdog = setTimeout(() => {
        if (!gotSnapshotThisConn) {
            console.log(`[ws] no snapshots from ${host} after ${CFG.wsWatchdogMs / 1000}s — rotating host (bot keeps playing via REST)`);
            wsHostIdx = (wsHostIdx + 1) % WS_HOSTS.length;
            try { ws?.close(); } catch { }
        }
    }, CFG.wsWatchdogMs);

    ws.on("open", () => {
        console.log(`[ws] connected: ${host}`);
        try { ws!.send(JSON.stringify({ type: "auth", token: API_KEY })); } catch { }
    });

    ws.on("message", (data) => {
        wsFrames++;
        try {
            const s = JSON.parse(data.toString()) as Snapshot;
            if (s && Array.isArray(s.units)) {
                gotSnapshotThisConn = true;
                clearTimeout(watchdog);
                onWsSnapshot(s);
            }
        } catch (e) {
            if (!wsParseErrLogged) {
                wsParseErrLogged = true;
                const head = data.toString().slice(0, 120);
                console.log(`[ws] unparseable frame (logged once): ${head}`);
            }
        }
    });

    ws.on("close", () => {
        clearTimeout(watchdog);
        if (!gotSnapshotThisConn) wsHostIdx = (wsHostIdx + 1) % WS_HOSTS.length;
        setTimeout(wsConnect, 1500);
    });
    ws.on("error", (e) => console.log("[ws] error:", (e as Error).message));
}

let schemaDumped = false;
function onWsSnapshot(s: Snapshot) {
    snap = s;
    lastWsSnapshotAt = now();
    if (!schemaDumped) {
        schemaDumped = true;
        const u = s.units.find((x) => x.isHero) ?? s.units[0];
        console.log("[schema] LIVE MODE ON — top-level keys:", Object.keys(s).join(", "));
        console.log("[schema] sample unit:", JSON.stringify(u)?.slice(0, 300));
        console.log(`[schema] arrows=${"arrows" in s} zones=${"zones" in s} events=${"events" in s}`);
    }
    const me = s.heroScoreboard?.find((h) => h.name === AGENT_NAME);
    if (me) {
        myFaction = me.faction;
        if (now() - lastLaneCmdAt > 3500) serverLane = me.lane;
        if (typeof me.recallCooldownMs === "number" && me.recallCooldownMs > 0)
            cd.recall = Math.max(cd.recall, now() + me.recallCooldownMs);
        if (me.alive) {
            const u = myUnit();
            if (u) recordHp(u.hp, u.maxHp);
        }
    }
    reflex(); // the one 20 Hz decision
}

// ----------------------------- Actions (single choke points) ------------------

async function sendMovement(kind: "sprint" | "stroll"): Promise<boolean> {
    if (!ready(kind)) return false;
    let ok = false;
    if (live() && ws?.readyState === WebSocket.OPEN && now() - lastWsSendAt >= CFG.wsSendGapMs) {
        ws.send(JSON.stringify({ type: kind })); lastWsSendAt = now(); ok = true;
    } else {
        ok = (await restPost({ action: kind })).ok;
    }
    if (ok) { cd[kind] = now() + (kind === "sprint" ? CFG.sprintCdMs : CFG.strollCdMs); console.log(`[act] ${kind}`); }
    return ok;
}

async function sendRecall(reason: string): Promise<boolean> {
    if (!ready("recall")) return false;
    let ok = false;
    if (live() && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "recall" })); ok = true; // bypasses WS gate: this is the emergency
    } else {
        ok = (await restPost({ action: "recall" }, { urgent: true })).ok;
    }
    if (ok) { cd.recall = now() + CFG.recallCdMs; console.log(`[act] recall (${reason})`); }
    return ok;
}

/**
 * THE lane choke point. Rules:
 *  - Never re-issue the lane we're already in/committed to (unless allowRepeat:
 *    deliberate escape-commit spam).
 *  - Non-emergency changes obey laneHoldMs; emergencies obey escapeSpamMs.
 *  - Optionally bundles a sprint (same REST POST, or a follow-up WS send).
 */
async function commandLane(lane: Lane, reason: string, opts: { sprint?: boolean; emergency?: boolean; allowRepeat?: boolean } = {}) {
    const committed = now() - lastLaneCmdAt < 3500 ? currentLaneTarget : (serverLane ?? currentLaneTarget);
    if (lane === committed && !opts.allowRepeat) {
        if (opts.sprint) void sendMovement("sprint");
        return;
    }
    const gate = opts.emergency ? CFG.escapeSpamMs : CFG.laneHoldMs;
    if (now() - lastLaneCmdAt < gate) return;

    const wantSprint = !!opts.sprint && ready("sprint");
    let ok = false;
    if (live() && ws?.readyState === WebSocket.OPEN && now() - lastWsSendAt >= CFG.wsSendGapMs) {
        ws.send(JSON.stringify({ type: "switchLane", lane })); lastWsSendAt = now(); ok = true;
        if (wantSprint) setTimeout(() => { void sendMovement("sprint"); }, 250);
    } else {
        const body: Record<string, any> = { heroLane: lane };
        if (wantSprint) body.action = "sprint";
        ok = (await restPost(body, { urgent: !!opts.emergency })).ok;
        if (ok && wantSprint) { cd.sprint = now() + CFG.sprintCdMs; }
    }
    if (ok) {
        console.log(`[act] lane ${committed}->${lane} (${reason})${wantSprint ? " +sprint" : ""}`);
        currentLaneTarget = lane;
        lastLaneCmdAt = now();
    }
}

// ----------------------------- Strategy: abilities -----------------------------
// Coached build: max Aura -> Cleave 1 (never more) -> Divine 1 -> finish Aura ->
// Thorns (to 4 vs physical rosters, low vs mages) -> Fury. All abilities cap at 4.

function nextAbilityPick(me: ScoreEntry): string | null {
    const offered = me.abilityChoices ?? [];
    const lvl = (id: string) => me.abilities.find((a) => a.id === id)?.level ?? 0;
    const auraId = AURA_IDS.find((id) => offered.includes(id) || lvl(id) > 0) ?? "fortitude";
    const physical = enemyPhysicalShare() >= 0.5;
    const MAX = 4;

    const wants: [string, number][] = [
        [auraId, 1],
        ["cleave", 1],
        ["divine_shield", 1],
        [auraId, 4],
        ["thorns", 1],
    ];
    if (physical) wants.push(["thorns", 4], ["fury", 4]);
    else wants.push(["fury", 4], ["thorns", 2]);

    for (const [id, target] of wants) {
        if (lvl(id) < Math.min(target, MAX) && offered.includes(id)) return id;
    }
    return offered.find((id) => lvl(id) < MAX && id !== "cleave") ?? null;
}

// ----------------------------- Strategy: recall (LIVE reflex, 20 Hz) ----------

function enemyHeroesInXpRange(u: Unit): number {
    const ef = enemyOf(myFaction!);
    return snap!.units.filter((x) => x.isHero && x.faction === ef && dist(x, u) <= CFG.xpRange).length;
}

function threatCount(u: Unit): number {
    const ef = enemyOf(myFaction!);
    return snap!.units.filter((x) => x.faction === ef && dist(x, u) <= CFG.threatRadius).length;
}

// Divine Shield will absorb the next burst: active now, or learned & off cooldown
// (auto-procs on next hit). If so — facetank, don't burn recall.
function divineCovers(me: MeView, u: Unit): boolean {
    const learned = me.abilities.find((a) => a.id === "divine_shield");
    if (!learned || learned.level <= 0) return false;
    if (u.shielded) return true;
    const cdSrc = u.abilities?.find((a) => a.id === "divine_shield") ?? learned;
    return (cdSrc.cooldownRemaining ?? 0) <= 0;
}

function reflex() {
    if (!joinedConfirmed || !myFaction) return;
    const me = meView();
    const u = myUnit();
    if (!me || !u || !me.alive) return;
    if (divineCovers(me, u)) return;

    const projected = incomingDps(CFG.dpsWindowLiveMs) * (CFG.predictLookaheadMs / 1000);
    const lethalNext = me.hp - projected <= me.maxHp * 0.02 && threatCount(u) > 0;
    const floor = hpFracOf(me) <= CFG.recallFloorLive;
    if ((lethalNext || floor) && enemyHeroesInXpRange(u) >= 1) {
        void sendRecall(`live: ${(hpFracOf(me) * 100) | 0}% hp, dps=${incomingDps(CFG.dpsWindowLiveMs) | 0}, heroes=${enemyHeroesInXpRange(u)}`);
    }
}

// ----------------------------- Strategy: macro (LIVE + REST) ------------------

function ownBase(): Building | undefined { // LIVE only
    return snap?.buildings.find((b) => b.faction === myFaction && b.type !== "tower");
}

function baseThreatLaneLive(): Lane | null {
    const base = ownBase();
    if (!base || !snap) return null;
    const ef = enemyOf(myFaction!);
    const nearE = snap.units.filter((x) => x.faction === ef && dist(x, base) <= 550);
    if (hpFracOf(base) < 0.5 || nearE.length >= 6 || nearE.some((x) => x.isHero)) {
        const byLane = (l: Lane) => nearE.filter((x) => x.lane === l).length;
        return (["top", "mid", "bot"] as Lane[]).sort((a, b) => byLane(b) - byLane(a))[0];
    }
    return null;
}

function baseThreatLaneRest(lanes: LaneStat[]): Lane | null {
    if (!rest || !myFaction) return null;
    const base = rest.bases[myFaction];
    const dropping = prevBaseHp !== null && base.hp < prevBaseHp;
    prevBaseHp = base.hp;
    const deepPush = lanes.filter((l) => (l.frontline !== undefined) && myAdvance(l.frontline) <= -60 && l.enemyHeroesHere >= 1);
    if (dropping || base.hp / base.maxHp < 0.55 || deepPush.length) {
        const sorted = [...lanes].sort((a, b) => myAdvance(a.frontline ?? 0) - myAdvance(b.frontline ?? 0));
        return sorted[0]?.lane ?? null;
    }
    return null;
}

function retreatLane(lanes: LaneStat[], exclude: Lane): Lane | null { // LIVE escape target
    const cands = lanes.filter((l) => l.lane !== exclude && l.ownTowerAlive && l.adv >= 0).sort((a, b) => b.adv - a.adv);
    return cands.length ? cands[0].lane : null;
}

function allyHeroNear(u: Unit): boolean {
    return !!snap?.units.some((x) => x.isHero && x.faction === myFaction && x.ownerName !== AGENT_NAME && dist(x, u) <= 350);
}

async function macro() {
    if (!joinedConfirmed || !myFaction || roundOverAt) return;
    const me = meView();
    if (!me) return;
    const lanes = laneStats();
    if (!lanes.length) return;

    // Dead: optionally pre-set respawn lane toward a threatened base. Nothing else.
    if (!me.alive) {
        const def = live() ? baseThreatLaneLive() : baseThreatLaneRest(lanes);
        if (def) await commandLane(def, "pre-set respawn: defend", { emergency: true });
        return;
    }

    const isLive = live();
    const u = isLive ? myUnit() : undefined;

    // 1) LIVE escape: about to die, recall down -> commit a peel (deliberate spam).
    if (isLive && u) {
        const projected = incomingDps(CFG.dpsWindowLiveMs) * (CFG.predictLookaheadMs / 1000);
        const dying = (me.hp - projected <= me.maxHp * 0.02 || hpFracOf(me) <= CFG.recallFloorLive) && threatCount(u) > 0;
        if (dying && !divineCovers(me, u) && !ready("recall")) {
            const rl = retreatLane(lanes, me.lane);
            if (rl) { await commandLane(rl, "!escape (recall down)", { sprint: true, emergency: true, allowRepeat: true }); return; }
        }
    }

    // 2) REST-mode recall: coarse data, so a wider buffer. Deny the bounty only
    //    if an enemy hero shares our lane; otherwise dying is cheaper than recall.
    if (!isLive && ready("recall")) {
        const hereHeroes = lanes.find((l) => l.lane === me.lane)?.enemyHeroesHere ?? 0;
        const droppingFast = incomingDps(CFG.dpsWindowRestMs) * 2 >= me.hp; // ~2s to live
        if (hereHeroes >= 1 && (hpFracOf(me) <= CFG.recallFloorRest || (droppingFast && hpFracOf(me) <= 0.3))) {
            await sendRecall(`rest: ${(hpFracOf(me) * 100) | 0}% hp, enemy heroes in lane=${hereHeroes}`);
            return;
        }
    }

    // 3) Base defense overrides everything else.
    const defLane = isLive ? baseThreatLaneLive() : baseThreatLaneRest(lanes);
    if (defLane) {
        const farForward = isLive
            ? (u && ownBase() ? dist(u, ownBase()!) > CFG.nearBaseRadius * 1.6 : false)
            : myAdvance(lanes.find((l) => l.lane === me.lane)?.frontline ?? 0) > 25;
        if (farForward && ready("recall")) { await sendRecall("defend base (teleport home)"); return; }
        await commandLane(defLane, "!defend base", { sprint: true, emergency: true });
        // LIVE: if already home, isolated, and they're coming — stroll to regroup.
        if (isLive && u && defLane === me.lane && !allyHeroNear(u)) {
            const l = lanes.find((x) => x.lane === defLane)!;
            if (l.enemy > l.friendly) await sendMovement("stroll");
        }
        return;
    }

    // 4) Mid-stack with hysteresis. In mid and not clearly lost -> HOLD AND SWING.
    const mid = lanes.find((l) => l.lane === "mid")!;
    const effLane = serverLane ?? currentLaneTarget;
    if (effLane === "mid") {
        if (mid.adv <= CFG.midBailAdv) {
            const side = lanes.filter((l) => l.lane !== "mid")
                .map((l) => ({ l, s: l.adv + (l.ownTowerAlive ? 1 : 0) - l.enemyHeroesHere }))
                .sort((a, b) => b.s - a.s)[0];
            if (side) await commandLane(side.l.lane, "mid lost, rotate", { sprint: true });
        }
        // else: hold. No command. This is the fix for the wandering warrior.
    } else if (mid.adv >= CFG.midRestackAdv) {
        await commandLane("mid", "restack mid");
    }

    // 5) Siege: clearly-winning lane with dead/low enemy tower, once we have levels.
    if (me.level >= 7) {
        const push = lanes
            .filter((l) => l.adv >= 2 && (!l.enemyTowerAlive || l.enemyTowerHp < 400))
            .sort((a, b) => b.adv - a.adv)[0];
        if (push && push.lane !== effLane) await commandLane(push.lane, "siege", { sprint: true });
    }
}

// ----------------------------- Heartbeat & boot --------------------------------

setInterval(() => {
    if (!joinedConfirmed) return;
    const me = meView();
    if (!me) return;
    const mode = live() ? "LIVE(20Hz)" : "REST(1.5s)";
    console.log(
        `[status] mode=${mode} lane=${serverLane ?? "?"} lvl=${me.level} hp=${(hpFracOf(me) * 100) | 0}%` +
        ` recall=${ready("recall") ? "ready" : Math.ceil((cd.recall - now()) / 1000) + "s"} wsFrames=${wsFrames}`
    );
}, 30_000);

console.log(`[boot] ${AGENT_NAME} | game ${GAME_ID} | melee/classic | stack ${CFG.homeLane} | items: ${CFG.itemPreference.join(" > ")}`);
wsConnect();
setInterval(pollRest, CFG.restPollMs);
setInterval(() => { macro().catch((e) => console.log("[macro] error:", (e as Error).message)); }, CFG.macroMs);
void pollRest();