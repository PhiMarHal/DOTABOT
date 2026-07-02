/**
 * Defense of the Agents — bot v0.5
 * =================================
 * Game 3 (AI ranked), mid stack, items: cat_ears > ring_of_regen.
 *
 * CLASS/SKIN PREFERENCE (falls through on rejection):
 *   1. Melee + Treant skin  -> build: Earthquake(fury) to 4 ASAP, Divine 1,
 *      Cleave 1, pump Fortitude, then Thorns.
 *   2. Mage + Farcaster (pixagreen_mage) -> Fireball first, Heal(fortitude) 1
 *      early, Fireball 4 > Tornado 4 > Skeleton 1, then pump Heal/Fortitude.
 *   3. Mage base skin -> Fireball 4 > Tornado 4 > Skeleton 1 > Fortitude.
 *   (If we end up melee WITHOUT Treant, the Classic aura build applies.)
 *
 * MOVEMENT DOCTRINE (from coaching): heroes only move FORWARD. Rotating to
 * "defend" is only useful when the fight point is reachable going forward:
 *   - INTERCEPT: switch lanes iff enemy heroes are COMING (frontline on our
 *     side but not yet at the base), that lane has MORE enemy heroes than ours,
 *     and its fight point is not behind us (forward-only reachability).
 *   - RECALL-DEFEND: only when enemy heroes + creeps are AT our base, recall is
 *     ready, and we're not at the enemy base / about to finish a tower.
 *   - Otherwise, being far forward and HOLDING is itself defense (we block the
 *     creep reinforcements feeding their push).
 *   No base-HP-scratch triggers. No naive rotations out of fights.
 *
 * DATA LAYERS:
 *   - REST poll (1.5s): always on; creeps/frontlines/towers, join, ability picks.
 *   - WS (20 Hz, gzip frames): scoreboard used for fast HP/recall reflex when it
 *     is object-shaped. NOTE: `units` currently arrive as bare ARRAYS (positional
 *     format unknown), so positional logic is disabled until mapped — the bot
 *     writes one full decoded frame to frame-sample.json to enable that mapping.
 *
 * Run:  npx tsx bot.ts     (.env: DOTA_API_KEY, DOTA_AGENT_NAME)
 * Debug: DOTA_DEBUG=1 npx tsx bot.ts   (periodic decision traces)
 */

import "dotenv/config";
import WebSocket from "ws";
import zlib from "node:zlib";
import fs from "node:fs";

// ----------------------------- Config ----------------------------------------

const REST_BASE = "https://game.defenseoftheagents.com";
const WS_HOSTS = [
    "wss://game.defenseoftheagents.com",
    "wss://wc2-agentic-dev-3o6un.ondigitalocean.app",
];

const API_KEY = process.env.DOTA_API_KEY ?? "";
const AGENT_NAME = process.env.DOTA_AGENT_NAME ?? "";
const DEBUG = process.env.DOTA_DEBUG === "1";
const GAME_ID = 3;

if (!API_KEY || !AGENT_NAME) {
    console.error("Missing DOTA_API_KEY / DOTA_AGENT_NAME — create .env (see .env.example).");
    process.exit(1);
}

interface DeployPref { heroClass: HeroClass; skin?: string; label: string; }
const SKIN_PREFS: DeployPref[] = [
    { heroClass: "melee", skin: "treant", label: "Treant warrior" },
    { heroClass: "mage", skin: "pixagreen_mage", label: "Farcaster mage" },
];
const BASE_PREF: DeployPref = { heroClass: "mage", label: "base mage" };
// Skins currently 403 without a connected wallet, so the default chain goes
// straight to base mage. Once a wallet is connected, run with DOTA_TRY_SKINS=1
// to attempt Treant -> Farcaster first. All skin builds are kept in buildWants.
const DEPLOY_PREFS: DeployPref[] =
    process.env.DOTA_TRY_SKINS === "1" ? [...SKIN_PREFS, BASE_PREF] : [BASE_PREF];

const CFG = {
    itemPreference: ["cat_ears", "ring_of_regen"] as (string | null)[],
    homeLane: "mid" as Lane,

    // Cadence
    restPollMs: 1500,
    macroMs: 400,
    wsSendGapMs: 300,
    restPostGapMs: 900,
    joinConfirmGraceMs: 25_000,
    rejoinDelayMs: 8_000,
    wsWatchdogMs: 8_000,
    liveFreshMs: 1_200,

    // Recall — fast path (20 Hz scoreboard HP)
    recallFloorLive: 0.06,
    predictLookaheadMs: 500,
    dpsWindowLiveMs: 600,

    // Recall — REST path (1.5s samples: wide buffer + one-poll projection)
    recallFloorRest: 0.25,
    combatWindowMs: 5_000,

    // Forward-only defense doctrine
    interceptNear: -35,      // myAdvance(frontline) below this = they're threatening our side
    interceptAtBase: -85,    // at/inside our base doorstep
    reachSlack: 5,           // target fight point must be >= ours - slack (forward-only)
    defendCreepCount: 3,     // enemy creeps at base required for a recall-defend
    atEnemyBaseAdvance: 80,  // we're basically at their base -> never recall-defend
    towerFinishHp: 300,      // we're about to crack a tower -> never recall-defend

    // Lanes
    midBailAdv: -3,
    midRestackAdv: 2,
    laneHoldMs: 6_000,
    escapeSpamMs: 1_500,

    // Cooldowns (ms)
    recallCdMs: 120_000,
    sprintCdMs: 25_000,
    strollCdMs: 25_000,
    recallChannelMs: 2_600,
};

// Skin variants reuse the base ability id (field-verified: Classic aura shows as
// "fortitude"). Alias sets in case a server build reports variant ids instead.
const ALIAS: Record<string, string[]> = {
    fortitude: ["fortitude", "defensive_aura", "ring_of_healing", "soul_harvest"],
    fury: ["fury", "earthquake"],
};
const idsFor = (id: string) => ALIAS[id] ?? [id];

// ----------------------------- Types ------------------------------------------

type Lane = "top" | "mid" | "bot";
type Faction = "human" | "orc";
type HeroClass = "melee" | "ranged" | "mage";

interface Ability { id: string; level: number; cooldownRemaining?: number; }
interface ScoreEntry {
    name: string; faction: Faction; heroClass: HeroClass; lane: Lane;
    level: number; hp: number; maxHp: number; alive: boolean;
    abilities: Ability[]; abilityChoices?: string[]; recallCooldownMs?: number;
    respawnTimer?: number; skin?: string | null;
}
interface Snapshot {
    tick: number; units: any[]; buildings: any[];
    winner: Faction | null; heroScoreboard?: any[];
}
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
interface LaneStat {
    lane: Lane; friendly: number; enemy: number; adv: number; enemyHeroesHere: number;
    ownTowerAlive: boolean; ownTowerHp: number; enemyTowerAlive: boolean; enemyTowerHp: number;
    frontline: number;
}
interface MeView {
    faction: Faction; lane: Lane; level: number; hp: number; maxHp: number; alive: boolean;
    heroClass: HeroClass; abilities: Ability[]; abilityChoices?: string[]; skin?: string | null;
}

// ----------------------------- State ------------------------------------------

let ws: WebSocket | null = null;
let wsHostIdx = 0;
let wsFrames = 0;
let frameDecodeFailLogged = false;
let frameSampleWritten = false;
let lastWsSnapshotAt = 0;
let snap: Snapshot | null = null;
let sbUsable = false; // heroScoreboard entries are objects with .name

let rest: RestState | null = null;
let restPolling = false;
let macroBusy = false;

let myFaction: Faction | null = null;
let serverLane: Lane | null = null;
let currentLaneTarget: Lane = CFG.homeLane;
let lastLaneCmdAt = 0;

let deploySentAt = 0;
let deployInFlight = false;
let deployFails = 0;
let lastDeployFailAt = 0;
let joinedConfirmed = false;
let itemIdx = 0;
let prefIdx = 0;
let skinGranted = true; // requested skin not contradicted by a warning
let roundOverAt = 0;

let lastWsSendAt = 0;
let lastRestPostAt = 0;
let restBackoffUntil = 0;

let lastPickPostAt = 0;
let lastPickId = "";
let lastRecallAt = 0;
let lastDbgAt = 0;

const hpHistory: { t: number; hp: number; maxHp: number }[] = [];
const cd = { recall: 0, sprint: 0, stroll: 0 };

const now = () => Date.now();
const ready = (k: keyof typeof cd) => now() >= cd[k];
const live = () => now() - lastWsSnapshotAt < CFG.liveFreshMs;
const sbLive = () => live() && sbUsable;
const channelingRecall = () => now() - lastRecallAt < CFG.recallChannelMs;
const dbg = (msg: string) => { if (DEBUG) console.log(`[think] ${msg}`); };

// ----------------------------- Small helpers ----------------------------------

const enemyOf = (f: Faction): Faction => (f === "human" ? "orc" : "human");
const hpFracOf = (m: { hp: number; maxHp: number }) => (m.maxHp ? m.hp / m.maxHp : 1);
const myAdvance = (frontline: number) => (myFaction === "human" ? frontline : -frontline);

function recordHp(hp: number, maxHp: number) {
    const t = now();
    hpHistory.push({ t, hp, maxHp });
    while (hpHistory.length && t - hpHistory[0].t > 8000) hpHistory.shift();
}

function hpLostOver(windowMs: number): number {
    if (hpHistory.length < 2) return 0;
    const b = hpHistory[hpHistory.length - 1];
    let a = hpHistory[0];
    for (let i = hpHistory.length - 1; i >= 0; i--) {
        if (b.t - hpHistory[i].t >= windowMs) { a = hpHistory[i]; break; }
    }
    return Math.max(0, a.hp - b.hp);
}

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

// ----------------------------- Frame codec ------------------------------------

type Codec = { name: string; offset: number; fn: (b: Buffer) => Buffer };
let codec: Codec | null = null;
const CODEC_CANDIDATES: [string, (b: Buffer) => Buffer][] = [
    ["gzip", (b) => zlib.gunzipSync(b)],
    ["deflate", (b) => zlib.inflateSync(b)],
    ["deflate-raw", (b) => zlib.inflateRawSync(b)],
    ["brotli", (b) => zlib.brotliDecompressSync(b)],
];

function decodeFrame(raw: Buffer): string | null {
    if (raw.length === 0) return null;
    if (raw[0] === 0x7b) return raw.toString("utf8");
    if (codec) {
        try { return codec.fn(raw.subarray(codec.offset)).toString("utf8"); }
        catch { codec = null; }
    }
    for (const offset of [0, 1, 2, 3, 4]) {
        for (const [name, fn] of CODEC_CANDIDATES) {
            try {
                const out = fn(raw.subarray(offset)).toString("utf8");
                if (out.startsWith("{")) {
                    codec = { name, offset, fn };
                    console.log(`[ws] frame codec detected: ${name}${offset ? ` (+${offset}B header)` : ""} — fast feed unlocked`);
                    return out;
                }
            } catch { /* next */ }
        }
    }
    return null;
}

// ----------------------------- Unified world views ----------------------------

function sbEntries(): ScoreEntry[] {
    return sbUsable && snap?.heroScoreboard ? (snap.heroScoreboard as ScoreEntry[]) : [];
}

function meView(): MeView | null {
    if (sbLive()) {
        const s = sbEntries().find((h) => h.name === AGENT_NAME);
        if (s) return { ...s, heroClass: s.heroClass };
    }
    const r = rest?.heroes?.find((h) => h.name === AGENT_NAME);
    if (!r) return null;
    return {
        faction: r.faction, lane: r.lane, level: r.level, hp: r.hp, maxHp: r.maxHp,
        alive: r.alive, heroClass: r.class, abilities: r.abilities ?? [],
        abilityChoices: r.abilityChoices, skin: undefined,
    };
}

function enemyHeroesInLane(lane: Lane): number {
    const ef = enemyOf(myFaction!);
    if (sbLive()) return sbEntries().filter((h) => h.faction === ef && h.alive && h.lane === lane).length;
    return rest?.heroes?.filter((h) => h.faction === ef && h.alive && h.lane === lane).length ?? 0;
}

// Creep counts / frontlines / towers come from REST (WS units are unmapped arrays).
function laneStats(): LaneStat[] {
    if (!rest || !myFaction) return [];
    const ef = enemyOf(myFaction);
    return (["top", "mid", "bot"] as Lane[]).map((lane) => {
        const l = rest!.lanes[lane];
        const friendly = l?.[myFaction!] ?? 0;
        const enemy = l?.[ef] ?? 0;
        const et = rest!.towers.find((t) => t.faction === ef && t.lane === lane);
        const ot = rest!.towers.find((t) => t.faction === myFaction && t.lane === lane);
        return {
            lane, friendly, enemy, adv: friendly - enemy,
            enemyHeroesHere: enemyHeroesInLane(lane),
            ownTowerAlive: !!ot?.alive, ownTowerHp: ot?.hp ?? 0,
            enemyTowerAlive: !!et?.alive, enemyTowerHp: et?.hp ?? 0,
            frontline: l?.frontline ?? 0,
        };
    });
}

function enemyPhysicalShare(): number {
    const ef = myFaction ? enemyOf(myFaction) : null;
    const roster: { faction: Faction; heroClass: HeroClass }[] =
        rest?.heroes?.map((h) => ({ faction: h.faction, heroClass: h.class })) ??
        sbEntries().map((h) => ({ faction: h.faction, heroClass: h.heroClass }));
    const es = roster.filter((h) => h.faction === ef);
    if (!es.length) return 0.5;
    return es.filter((h) => h.heroClass === "melee" || h.heroClass === "ranged").length / es.length;
}

// ----------------------------- REST I/O ----------------------------------------

interface RestResult { ok: boolean; status: number; text: string; warning?: string; }

function syncCooldownFromWarning(warning: string) {
    const m = warning.match(/(\d+)s remaining/);
    const secs = m ? parseInt(m[1], 10) : null;
    if (!secs) return;
    if (/recall/i.test(warning)) cd.recall = Math.max(cd.recall, now() + secs * 1000);
    if (/sprint/i.test(warning)) cd.sprint = Math.max(cd.sprint, now() + secs * 1000);
    if (/stroll/i.test(warning)) cd.stroll = Math.max(cd.stroll, now() + secs * 1000);
}

async function restPost(body: Record<string, any>, opts: { urgent?: boolean } = {}): Promise<RestResult> {
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
        let warning: string | undefined;
        if (r.status === 429) { restBackoffUntil = now() + 10_000; console.log("[rest] 429 — backing off 10s"); }
        else if (!r.ok) console.log(`[rest] deploy ${r.status}: ${text}`);
        else {
            try {
                const j = JSON.parse(text);
                if (j.warning) { warning = String(j.warning); console.log(`[rest] warning: ${warning}`); syncCooldownFromWarning(warning); }
            } catch { }
        }
        return { ok: r.ok, status: r.status, text, warning };
    } catch (e) {
        console.log("[rest] POST error:", (e as Error).message);
        return { ok: false, status: 0, text: String(e) };
    }
}

const actionRejected = (res: RestResult, action: string) =>
    !res.ok || (!!res.warning && new RegExp(action, "i").test(res.warning));

async function pollRest() {
    if (restPolling) return;
    restPolling = true;
    try {
        const r = await fetch(`${REST_BASE}/api/game/state?game=${GAME_ID}`);
        if (!r.ok) { console.log(`[rest] state ${r.status}`); return; }
        rest = (await r.json()) as RestState;

        if (rest.winner) {
            if (!roundOverAt) {
                roundOverAt = now();
                console.log(`[round] over — ${rest.winner} won. Rejoining next round…`);
            }
            resetRoundState();
            return;
        }
        if (roundOverAt && now() - roundOverAt < CFG.rejoinDelayMs) return;
        roundOverAt = 0;

        const meR = rest.heroes.find((h) => h.name === AGENT_NAME);
        if (meR) {
            if (!joinedConfirmed) {
                const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
                console.log(`[join] confirmed: ${meR.faction} ${meR.class} in ${meR.lane} (requested: ${pref.label}${skinGranted ? "" : ", skin refused"})`);
            }
            joinedConfirmed = true;
            myFaction = meR.faction;
            if (now() - lastLaneCmdAt > 3500) serverLane = meR.lane;
            if (typeof meR.recallCooldownMs === "number" && meR.recallCooldownMs > 0)
                cd.recall = Math.max(cd.recall, now() + meR.recallCooldownMs);
            if (!sbLive() && meR.alive) recordHp(meR.hp, meR.maxHp);
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
    const pick = nextAbilityPick(meR.class, meR.abilities as Ability[], choices);
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
    if (now() - lastDeployFailAt < 4000) return; // brief pause after a rejection
    const retryDue = deploySentAt > 0 && now() - deploySentAt > CFG.joinConfirmGraceMs;
    if (deploySentAt > 0 && !retryDue) return;
    deployInFlight = true;
    try {
        const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
        const item = CFG.itemPreference[itemIdx] ?? null;
        const body: Record<string, any> = { heroClass: pref.heroClass, heroLane: CFG.homeLane, message: "bot online" };
        if (pref.skin) body.skin = pref.skin;
        if (item) body.equippedItem = item;
        console.log(`[join] deploying ${pref.label} @ ${CFG.homeLane}${item ? ` +${item}` : ""}${retryDue ? " (retry)" : ""}`);
        const res = await restPost(body, { urgent: true });
        if (res.ok) {
            deploySentAt = now();
            lastLaneCmdAt = now();
            currentLaneTarget = CFG.homeLane;
            deployFails = 0;
            if (res.warning && /skin/i.test(res.warning)) skinGranted = false;
        } else if (res.status >= 400 && res.status < 500) {
            lastDeployFailAt = now();
            deployFails++;
            // Order matters: skin errors mention "equip this skin", so check skin FIRST.
            if (/skin|class|wallet/i.test(res.text) && prefIdx < DEPLOY_PREFS.length - 1) {
                prefIdx++; deployFails = 0;
                console.log(`[join] skin/class rejected, falling back to ${DEPLOY_PREFS[prefIdx].label}`);
            } else if (/item|equip/i.test(res.text) && itemIdx < CFG.itemPreference.length - 1) {
                itemIdx++; deployFails = 0;
                console.log(`[join] item rejected, falling back to ${CFG.itemPreference[itemIdx] ?? "no item"}`);
            } else if (/full/i.test(res.text)) {
                deploySentAt = now(); // game full: wait a full grace period
            } else if (deployFails >= 3) {
                // Failsafe: never wedge on an unrecognized rejection.
                deployFails = 0;
                if (prefIdx < DEPLOY_PREFS.length - 1) {
                    prefIdx++;
                    console.log(`[join] repeated rejections, falling back to ${DEPLOY_PREFS[prefIdx].label}`);
                } else if (itemIdx < CFG.itemPreference.length - 1) {
                    itemIdx++;
                    console.log(`[join] repeated rejections, dropping item to ${CFG.itemPreference[itemIdx] ?? "none"}`);
                } else {
                    console.log("[join] repeated rejections on the plainest deploy — backing off 30s");
                    lastDeployFailAt = now() + 26_000; // + the 4s gate = ~30s pause
                }
            }
        }
    } finally {
        deployInFlight = false;
    }
}

function resetRoundState() {
    joinedConfirmed = false;
    deploySentAt = 0;
    deployFails = 0;
    lastDeployFailAt = 0;
    serverLane = null;
    currentLaneTarget = CFG.homeLane;
    hpHistory.length = 0;
    cd.recall = 0; cd.sprint = 0; cd.stroll = 0;
    lastPickId = "";
    lastRecallAt = 0;
    // keep: prefIdx / itemIdx (learned rejections), codec, wsHostIdx
}

// ----------------------------- WebSocket ---------------------------------------

function wsConnect() {
    const host = WS_HOSTS[wsHostIdx];
    let gotSnapshotThisConn = false;
    frameDecodeFailLogged = false;
    ws = new WebSocket(`${host}/?game=${GAME_ID}`);

    const watchdog = setTimeout(() => {
        if (!gotSnapshotThisConn) {
            console.log(`[ws] no decodable snapshots from ${host} — rotating host (REST keeps playing)`);
            wsHostIdx = (wsHostIdx + 1) % WS_HOSTS.length;
            try { ws?.close(); } catch { }
        }
    }, CFG.wsWatchdogMs);

    ws.on("open", () => {
        console.log(`[ws] connected: ${host}`);
        try { ws!.send(JSON.stringify({ type: "auth", token: API_KEY })); } catch { }
    });

    ws.on("message", (data: WebSocket.RawData) => {
        wsFrames++;
        const buf = Array.isArray(data) ? Buffer.concat(data as Buffer[])
            : Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
        const text = decodeFrame(buf);
        if (text === null) {
            if (!frameDecodeFailLogged) {
                frameDecodeFailLogged = true;
                console.log(`[ws] undecodable frame (len=${buf.length}, head hex: ${buf.subarray(0, 8).toString("hex")})`);
            }
            return;
        }
        try {
            const s = JSON.parse(text) as Snapshot;
            if (s && Array.isArray(s.units)) {
                gotSnapshotThisConn = true;
                clearTimeout(watchdog);
                onWsSnapshot(s, text);
            }
        } catch { /* not a snapshot */ }
    });

    ws.on("close", () => {
        clearTimeout(watchdog);
        if (!gotSnapshotThisConn) wsHostIdx = (wsHostIdx + 1) % WS_HOSTS.length;
        setTimeout(wsConnect, 1500);
    });
    ws.on("error", (e) => console.log("[ws] error:", (e as Error).message));
}

let schemaDumped = false;
function onWsSnapshot(s: Snapshot, rawText: string) {
    snap = s;
    lastWsSnapshotAt = now();

    const sb0 = s.heroScoreboard?.[0];
    sbUsable = !!sb0 && typeof sb0 === "object" && !Array.isArray(sb0) && "name" in sb0;

    if (!schemaDumped) {
        schemaDumped = true;
        const unitsAreArrays = Array.isArray(s.units[0]);
        console.log(`[schema] fast feed on — units=${unitsAreArrays ? "ARRAYS (positional mode disabled until mapped)" : "objects"}, scoreboard=${sbUsable ? "objects (20Hz hero data ON)" : "unusable"}`);
        if (sbUsable) console.log("[schema] scoreboard sample:", JSON.stringify(sb0).slice(0, 300));
        if (!frameSampleWritten) {
            frameSampleWritten = true;
            try {
                fs.writeFileSync("frame-sample.json", rawText);
                console.log("[schema] wrote frame-sample.json — send this file back to map the unit array format (unlocks positional play)");
            } catch (e) { console.log("[schema] could not write frame-sample.json:", (e as Error).message); }
        }
    }

    if (sbUsable) {
        const me = sbEntries().find((h) => h.name === AGENT_NAME);
        if (me) {
            myFaction = me.faction;
            if (now() - lastLaneCmdAt > 3500) serverLane = me.lane;
            if (typeof me.recallCooldownMs === "number" && me.recallCooldownMs > 0)
                cd.recall = Math.max(cd.recall, now() + me.recallCooldownMs);
            if (me.alive) recordHp(me.hp, me.maxHp);
        }
    }
    reflex();
}

// ----------------------------- Actions -----------------------------------------

async function sendMovement(kind: "sprint" | "stroll"): Promise<boolean> {
    if (!ready(kind) || channelingRecall()) return false;
    const prev = cd[kind];
    cd[kind] = now() + (kind === "sprint" ? CFG.sprintCdMs : CFG.strollCdMs);
    if (live() && ws?.readyState === WebSocket.OPEN && now() - lastWsSendAt >= CFG.wsSendGapMs) {
        ws.send(JSON.stringify({ type: kind })); lastWsSendAt = now();
        console.log(`[act] ${kind}`);
        return true;
    }
    const res = await restPost({ action: kind });
    if (actionRejected(res, kind)) {
        if (!res.warning) cd[kind] = prev;
        else if (!/remaining/.test(res.warning)) cd[kind] = now() + 2500;
        return false;
    }
    console.log(`[act] ${kind}`);
    return true;
}

async function sendRecall(reason: string): Promise<boolean> {
    if (!ready("recall")) return false;
    const prev = cd.recall;
    cd.recall = now() + CFG.recallCdMs;
    lastRecallAt = now();
    if (live() && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "recall" }));
        console.log(`[act] recall (${reason})`);
        return true;
    }
    const res = await restPost({ action: "recall" }, { urgent: true });
    if (actionRejected(res, "recall")) {
        if (!res.warning) { cd.recall = prev; lastRecallAt = 0; }
        return false;
    }
    console.log(`[act] recall (${reason})`);
    return true;
}

async function commandLane(lane: Lane, reason: string, opts: { sprint?: boolean; emergency?: boolean; allowRepeat?: boolean } = {}) {
    const committed = now() - lastLaneCmdAt < 3500 ? currentLaneTarget : (serverLane ?? currentLaneTarget);
    if (lane === committed && !opts.allowRepeat) {
        if (opts.sprint) void sendMovement("sprint");
        return;
    }
    const gate = opts.emergency ? CFG.escapeSpamMs : CFG.laneHoldMs;
    if (now() - lastLaneCmdAt < gate) return;

    const prevTarget = currentLaneTarget, prevAt = lastLaneCmdAt;
    currentLaneTarget = lane;
    lastLaneCmdAt = now();

    const wantSprint = !!opts.sprint && ready("sprint") && !channelingRecall();
    let ok = false;
    if (live() && ws?.readyState === WebSocket.OPEN && now() - lastWsSendAt >= CFG.wsSendGapMs) {
        ws.send(JSON.stringify({ type: "switchLane", lane })); lastWsSendAt = now(); ok = true;
        if (wantSprint) setTimeout(() => { void sendMovement("sprint"); }, 250);
    } else {
        const body: Record<string, any> = { heroLane: lane };
        if (wantSprint) { body.action = "sprint"; cd.sprint = now() + CFG.sprintCdMs; }
        const res = await restPost(body, { urgent: !!opts.emergency });
        ok = res.ok;
    }
    if (ok) {
        console.log(`[act] lane ${committed}->${lane} (${reason})${wantSprint ? " +sprint" : ""}`);
    } else {
        currentLaneTarget = prevTarget;
        lastLaneCmdAt = prevAt;
    }
}

// ----------------------------- Ability builds ----------------------------------

function currentSkin(me: MeView | null): string | null {
    if (me && me.skin !== undefined) return me.skin ?? null;      // scoreboard truth
    const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
    return skinGranted ? pref.skin ?? null : null;                 // best assumption
}

function buildWants(heroClass: HeroClass, skin: string | null): [string, number][] {
    if (heroClass === "melee") {
        if (skin === "treant") {
            // Earthquake (fury slot) ASAP -> Divine 1 -> Cleave 1 -> pump Fortitude -> Thorns
            return [["fury", 4], ["divine_shield", 1], ["cleave", 1], ["fortitude", 4], ["thorns", 4]];
        }
        // Classic aura build (also fine for plain melee)
        const physical = enemyPhysicalShare() >= 0.5;
        const base: [string, number][] = [
            ["fortitude", 1], ["cleave", 1], ["divine_shield", 1], ["fortitude", 4], ["thorns", 1],
        ];
        return physical ? [...base, ["thorns", 4], ["fury", 4]] : [...base, ["fury", 4], ["thorns", 2]];
    }
    if (heroClass === "mage") {
        if (skin === "pixagreen_mage") {
            // Fireball first, Heal (fortitude slot) 1 early, FB4 > Tornado4 > Skeleton1, pump Heal
            return [["fireball", 1], ["fortitude", 1], ["fireball", 4], ["tornado", 4], ["raise_skeleton", 1], ["fortitude", 4]];
        }
        return [["fireball", 1], ["fireball", 4], ["tornado", 4], ["raise_skeleton", 1], ["fortitude", 4]];
    }
    // ranged (unplanned fallback)
    return [["volley", 4], ["fortitude", 4], ["critical_strike", 4], ["fury", 4]];
}

function nextAbilityPick(heroClass: HeroClass, abilities: Ability[], offered: string[]): string | null {
    const lvlOf = (id: string) => {
        for (const alias of idsFor(id)) {
            const a = abilities.find((x) => x.id === alias);
            if (a) return a.level;
        }
        return 0;
    };
    const offeredIdFor = (id: string) => idsFor(id).find((alias) => offered.includes(alias)) ?? null;
    const MAX = 4;

    const me = meView();
    const wants = buildWants(heroClass, currentSkin(me));
    for (const [id, target] of wants) {
        const off = offeredIdFor(id);
        if (off && lvlOf(id) < Math.min(target, MAX)) return off;
    }
    // Fallback: anything non-maxed; melee never takes a 2nd Cleave.
    return offered.find((id) => {
        const lvl = abilities.find((a) => a.id === id)?.level ?? 0;
        if (lvl >= MAX) return false;
        if (heroClass === "melee" && id === "cleave" && lvl >= 1) return false;
        return true;
    }) ?? null;
}

// ----------------------------- Recall / escape ----------------------------------

function divineCovers(me: MeView): boolean {
    if (me.heroClass !== "melee") return false;
    const ds = me.abilities.find((a) => a.id === "divine_shield");
    if (!ds || ds.level <= 0) return false;
    // Only trust a REAL cooldown value; REST omits it (unknown != covered).
    return typeof ds.cooldownRemaining === "number" && ds.cooldownRemaining <= 0;
}

function reflex() {
    if (!joinedConfirmed || !myFaction || !sbLive()) return;
    const me = meView();
    if (!me || !me.alive) return;
    if (divineCovers(me)) return;

    const projected = incomingDps(CFG.dpsWindowLiveMs) * (CFG.predictLookaheadMs / 1000);
    const lethalNext = me.hp - projected <= me.maxHp * 0.02 && hpLostOver(1000) > 0;
    const floor = hpFracOf(me) <= CFG.recallFloorLive;
    if ((lethalNext || floor) && enemyHeroesInLane(serverLane ?? me.lane) >= 1) {
        void sendRecall(`fast: ${(hpFracOf(me) * 100) | 0}% hp, dps=${incomingDps(CFG.dpsWindowLiveMs) | 0}`);
    }
}

// ----------------------------- Macro --------------------------------------------

async function macro() {
    if (!joinedConfirmed || !myFaction || roundOverAt) return;
    const me = meView();
    if (!me) return;
    const lanes = laneStats();
    if (!lanes.length) return;
    const effLane: Lane = serverLane ?? currentLaneTarget;
    const here = lanes.find((l) => l.lane === effLane)!;

    if (DEBUG && now() - lastDbgAt > 5000) {
        lastDbgAt = now();
        dbg(lanes.map((l) => `${l.lane}: adv=${l.adv} fl=${myAdvance(l.frontline) | 0} eh=${l.enemyHeroesHere}`).join(" | ") + ` | me: ${effLane} ${(hpFracOf(me) * 100) | 0}%`);
    }

    // Dead: pre-set respawn toward the lane with the most incoming enemy heroes.
    if (!me.alive) {
        const threat = lanes
            .filter((l) => l.enemyHeroesHere >= 1 && myAdvance(l.frontline) <= CFG.interceptNear)
            .sort((a, b) => b.enemyHeroesHere - a.enemyHeroesHere)[0];
        if (threat) await commandLane(threat.lane, "pre-set respawn: intercept", { emergency: true });
        return;
    }

    // 1) Slow-path recall (fast path is reflex()): wide buffer + one-poll projection.
    if (!sbLive() && ready("recall")) {
        const recentDrop = hpLostOver(2000);
        const dieNextPoll = me.hp - recentDrop * 1.4 <= 0;
        if (here.enemyHeroesHere >= 1 && hpFracOf(me) <= CFG.recallFloorRest && (recentDrop > 0 || dieNextPoll)) {
            await sendRecall(`slow: ${(hpFracOf(me) * 100) | 0}% hp, dropped ${recentDrop | 0}`);
            return;
        }
    }

    // 2) Escape when dying with recall down: committed forward peel (deliberate re-issue).
    {
        const dyingFast = sbLive()
            ? me.hp - incomingDps(CFG.dpsWindowLiveMs) * (CFG.predictLookaheadMs / 1000) <= me.maxHp * 0.02 && hpLostOver(1000) > 0
            : hpFracOf(me) <= CFG.recallFloorRest && hpLostOver(2000) > 0;
        if (dyingFast && here.enemyHeroesHere >= 1 && !ready("recall") && !divineCovers(me)) {
            const rl = lanes.filter((l) => l.lane !== effLane && l.ownTowerAlive && l.adv >= 0).sort((a, b) => b.adv - a.adv)[0];
            if (rl) { await commandLane(rl.lane, "!escape (recall down)", { sprint: true, emergency: true, allowRepeat: true }); return; }
        }
    }

    // 3) RECALL-DEFEND: enemy heroes + creeps AT our base, and we're not busy winning.
    {
        const atBase = lanes.filter((l) => l.enemyHeroesHere >= 1 && myAdvance(l.frontline) <= CFG.interceptAtBase && l.enemy >= CFG.defendCreepCount);
        if (atBase.length && ready("recall")) {
            const weAreAtTheirBase = myAdvance(here.frontline) >= CFG.atEnemyBaseAdvance;
            const finishingTower = here.enemyTowerAlive && here.enemyTowerHp <= CFG.towerFinishHp && here.adv >= 0;
            if (!weAreAtTheirBase && !finishingTower) {
                const target = atBase.sort((a, b) => b.enemyHeroesHere - a.enemyHeroesHere)[0];
                if (await sendRecall(`defend base: ${target.enemyHeroesHere} heroes + creeps at ${target.lane}`)) {
                    // Steer the channel so the teleport drops us on the besieged lane.
                    setTimeout(() => { void commandLane(target.lane, "!defend landing", { emergency: true }); }, 600);
                }
                return;
            }
            dbg(`base threatened but holding: atTheirBase=${weAreAtTheirBase} finishingTower=${finishingTower}`);
        }
    }

    // 4) INTERCEPT: heroes COMING (not past), more of them than face us here, and the
    //    fight point is reachable moving forward.
    {
        const coming = lanes.filter((l) =>
            l.lane !== effLane &&
            l.enemyHeroesHere > here.enemyHeroesHere &&
            myAdvance(l.frontline) <= CFG.interceptNear &&
            myAdvance(l.frontline) > CFG.interceptAtBase &&
            myAdvance(l.frontline) >= myAdvance(here.frontline) - CFG.reachSlack
        ).sort((a, b) => b.enemyHeroesHere - a.enemyHeroesHere)[0];
        if (coming) { await commandLane(coming.lane, `intercept ${coming.enemyHeroesHere} heroes`, { sprint: true }); return; }
    }

    // 5) FIGHT-LOCK: enemy hero engaged with us -> hold and swing.
    if (here.enemyHeroesHere >= 1 && hpLostOver(CFG.combatWindowMs) > 0) { dbg("fight-lock"); return; }

    // 6) Mid-stack with hysteresis; siege-hold so siege & restack can't oscillate.
    const mid = lanes.find((l) => l.lane === "mid")!;
    const siegeQualifies = (l: LaneStat | undefined) => !!l && l.adv >= 2 && (!l.enemyTowerAlive || l.enemyTowerHp < 400);
    if (effLane === "mid") {
        if (mid.adv <= CFG.midBailAdv) {
            const side = lanes.filter((l) => l.lane !== "mid")
                .map((l) => ({ l, s: l.adv + (l.ownTowerAlive ? 1 : 0) - l.enemyHeroesHere }))
                .sort((a, b) => b.s - a.s)[0];
            if (side) await commandLane(side.l.lane, "mid lost, rotate", { sprint: true });
        }
        return; // otherwise hold mid and swing
    }
    if (siegeQualifies(here)) { dbg("siege-hold"); return; }
    if (mid.adv >= CFG.midRestackAdv) { await commandLane("mid", "restack mid"); return; }
    if (me.level >= 7) {
        const push = lanes.filter((l) => l.lane !== effLane && siegeQualifies(l)).sort((a, b) => b.adv - a.adv)[0];
        if (push) await commandLane(push.lane, "siege", { sprint: true });
    }
}

// ----------------------------- Heartbeat & boot ---------------------------------

setInterval(() => {
    if (!joinedConfirmed) return;
    const me = meView();
    if (!me) return;
    const mode = sbLive() ? "FAST(20Hz sb)" : live() ? "WS-up/REST-logic" : "REST(1.5s)";
    console.log(
        `[status] mode=${mode} class=${me.heroClass}${currentSkin(me) ? `/${currentSkin(me)}` : ""} lane=${serverLane ?? "?"} lvl=${me.level} hp=${(hpFracOf(me) * 100) | 0}%` +
        ` recall=${ready("recall") ? "ready" : Math.ceil((cd.recall - now()) / 1000) + "s"} wsFrames=${wsFrames}`
    );
}, 30_000);

console.log(`[boot] ${AGENT_NAME} | game ${GAME_ID} | prefs: ${DEPLOY_PREFS.map((p) => p.label).join(" > ")} | items: ${CFG.itemPreference.join(" > ")}`);
wsConnect();
setInterval(pollRest, CFG.restPollMs);
setInterval(() => {
    if (macroBusy) return;
    macroBusy = true;
    macro().catch((e) => console.log("[macro] error:", (e as Error).message)).finally(() => { macroBusy = false; });
}, CFG.macroMs);
void pollRest();