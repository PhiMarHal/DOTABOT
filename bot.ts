/**
 * Defense of the Agents — Melee Classic bot (v0.4)
 * =================================================
 * Melee + "classic" skin (Defensive Aura), Game 3 (AI ranked), mid stack.
 * Items: cat_ears first (confirmed working in the field!), ring_of_regen fallback.
 *
 * FIELD-TEST FIXES IN THIS VERSION:
 *  1. WS frames are COMPRESSED binary (they were arriving fine — 20/s — just
 *     undecoded). A codec auto-detector now tries gzip / deflate / deflate-raw /
 *     brotli at several byte offsets, caches the winner, and unlocks LIVE mode.
 *  2. Reentrancy: the async macro overlapped itself via setInterval, double-
 *     firing every action (all [act] lines appeared in pairs). Now guarded, and
 *     all cooldown/lane state is updated optimistically BEFORE the network call.
 *  3. Base defense was trigger-happy (any 1-HP base chip or a -60 frontline),
 *     which yanked the hero out of fights, burned Recall on fake emergencies
 *     ("recall as a lane change"), and left Recall on cooldown when actually
 *     dying. It now requires a real siege: sustained base damage or a frontline
 *     essentially AT our base with an enemy hero on it.
 *  4. Fight-lock: enemy hero in our lane + we're actively trading HP => HOLD AND
 *     SWING. No restack/siege rotation may interrupt a live hero fight.
 *  5. Server warnings (which arrive with HTTP 200!) are now parsed: "on cooldown
 *     (Ns remaining)" syncs our clocks; "can't be used while channeling" marks
 *     the action failed instead of silently burning our client-side cooldown.
 *
 * Run:  npx tsx bot.ts   (reads DOTA_API_KEY / DOTA_AGENT_NAME from .env)
 */

import "dotenv/config";
import WebSocket from "ws";
import zlib from "node:zlib";

// ----------------------------- Config ----------------------------------------

const REST_BASE = "https://game.defenseoftheagents.com";
const WS_HOSTS = [
    "wss://game.defenseoftheagents.com",
    "wss://wc2-agentic-dev-3o6un.ondigitalocean.app",
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

    // Recall — LIVE (20 Hz measured damage; press late, channel is invuln)
    recallFloorLive: 0.06,
    predictLookaheadMs: 500,
    dpsWindowLiveMs: 600,
    xpRange: 350,

    // Recall — REST (coarse 1.5s samples: leave a real buffer or we die between polls)
    recallFloorRest: 0.25,
    combatWindowMs: 5_000,      // "actively trading" = lost HP within this window

    // Base defense (REST): a real siege, not a scratch
    baseSiegeLossHp: 45,        // base HP lost over the window below
    baseSiegeWindowMs: 6_000,
    baseLowFrac: 0.5,           // or below half AND still dropping
    deepFrontline: 85,          // |frontline| >= this on our side + enemy hero = at our door
    farForwardAdvance: 45,      // we're this deep in enemy territory -> recall home to defend

    // Lanes (hysteresis so we hold and attack instead of ping-ponging)
    midBailAdv: -3,
    midRestackAdv: 2,
    laneHoldMs: 6_000,
    escapeSpamMs: 1_500,

    // Geometry (LIVE only)
    threatRadius: 230,
    heroFightRadius: 350,
    nearBaseRadius: 650,

    // Cooldown clocks (ms)
    recallCdMs: 120_000,
    sprintCdMs: 25_000,
    strollCdMs: 25_000,
    recallChannelMs: 2_600,     // don't try sprint/stroll during the ~2s channel
};

const AURA_IDS = ["fortitude", "defensive_aura"];

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
    frontline?: number;
}
interface MeView {
    faction: Faction; lane: Lane; level: number; hp: number; maxHp: number; alive: boolean;
    abilities: Ability[]; abilityChoices?: string[]; recallCooldownMs?: number;
}

// ----------------------------- State ------------------------------------------

let ws: WebSocket | null = null;
let wsHostIdx = 0;
let wsFrames = 0;
let frameDecodeFailLogged = false;
let lastWsSnapshotAt = 0;
let snap: Snapshot | null = null;

let rest: RestState | null = null;
let restPolling = false;
let macroBusy = false;

let myFaction: Faction | null = null;
let serverLane: Lane | null = null;
let currentLaneTarget: Lane = CFG.homeLane;
let lastLaneCmdAt = 0;

let deploySentAt = 0;
let deployInFlight = false;
let joinedConfirmed = false;
let itemIdx = 0;
let roundOverAt = 0;

let lastWsSendAt = 0;
let lastRestPostAt = 0;
let restBackoffUntil = 0;

let lastPickPostAt = 0;
let lastPickId = "";
let lastRecallAt = 0;

const hpHistory: { t: number; hp: number; maxHp: number }[] = [];
const baseHpHist: { t: number; hp: number; maxHp: number }[] = [];

const cd = { recall: 0, sprint: 0, stroll: 0 };
const now = () => Date.now();
const ready = (k: keyof typeof cd) => now() >= cd[k];
const live = () => now() - lastWsSnapshotAt < CFG.liveFreshMs;
const channelingRecall = () => now() - lastRecallAt < CFG.recallChannelMs;

// ----------------------------- Small helpers ----------------------------------

const enemyOf = (f: Faction): Faction => (f === "human" ? "orc" : "human");
const dist = (a: { x: number; y: number }, b: { x: number; y: number }) => Math.hypot(a.x - b.x, a.y - b.y);
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
// The live feed sends compressed binary frames. Detect the codec once, then reuse.

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
    if (raw[0] === 0x7b /* '{' */) return raw.toString("utf8");
    if (codec) {
        try { return codec.fn(raw.subarray(codec.offset)).toString("utf8"); }
        catch { codec = null; /* re-detect below */ }
    }
    for (const offset of [0, 1, 2, 3, 4]) {
        for (const [name, fn] of CODEC_CANDIDATES) {
            try {
                const out = fn(raw.subarray(offset)).toString("utf8");
                if (out.startsWith("{")) {
                    codec = { name, offset, fn };
                    console.log(`[ws] frame codec detected: ${name}${offset ? ` (skipping ${offset}-byte header)` : ""} — LIVE mode unlocked`);
                    return out;
                }
            } catch { /* try next */ }
        }
    }
    return null;
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

function myUnit(): Unit | undefined {
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
            const friendly = u.filter((x) => x.faction === myFaction).length;
            const enemy = u.filter((x) => x.faction === ef).length;
            return {
                lane, friendly, enemy, adv: friendly - enemy,
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

interface RestResult { ok: boolean; status: number; text: string; warning?: string; }

function syncCooldownFromWarning(warning: string) {
    const m = warning.match(/(\d+)s remaining/);
    const secs = m ? parseInt(m[1], 10) : null;
    if (/recall/i.test(warning) && secs) cd.recall = Math.max(cd.recall, now() + secs * 1000);
    if (/sprint/i.test(warning) && secs) cd.sprint = Math.max(cd.sprint, now() + secs * 1000);
    if (/stroll/i.test(warning) && secs) cd.stroll = Math.max(cd.stroll, now() + secs * 1000);
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
                if (j.warning) { warning = String(j.warning); console.log(`[rest] warning: ${warning}`); syncCooldownFromWarning(warning!); }
            } catch { }
        }
        return { ok: r.ok, status: r.status, text, warning };
    } catch (e) {
        console.log("[rest] POST error:", (e as Error).message);
        return { ok: false, status: 0, text: String(e) };
    }
}

// Did the server actually execute the action, or 200-with-excuse?
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
            if (!joinedConfirmed) console.log(`[join] confirmed in game as ${meR.faction} ${meR.class} (${meR.lane})`);
            joinedConfirmed = true;
            myFaction = meR.faction;
            if (now() - lastLaneCmdAt > 3500) serverLane = meR.lane;
            if (typeof meR.recallCooldownMs === "number" && meR.recallCooldownMs > 0)
                cd.recall = Math.max(cd.recall, now() + meR.recallCooldownMs);
            if (!live() && meR.alive) recordHp(meR.hp, meR.maxHp);

            // Track our base HP over time (REST siege detection).
            const base = rest.bases[myFaction];
            if (base) {
                baseHpHist.push({ t: now(), hp: base.hp, maxHp: base.maxHp });
                while (baseHpHist.length && now() - baseHpHist[0].t > 15_000) baseHpHist.shift();
            }

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
    if (deploySentAt > 0 && !retryDue) return;
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
            lastLaneCmdAt = now();
            currentLaneTarget = CFG.homeLane;
        } else if (res.status === 400 && /item|equip/i.test(res.text) && itemIdx < CFG.itemPreference.length - 1) {
            itemIdx++;
            console.log(`[join] item rejected, falling back to ${CFG.itemPreference[itemIdx] ?? "no item"}`);
        } else if (res.status === 400 && /full/i.test(res.text)) {
            deploySentAt = now();
        }
    } finally {
        deployInFlight = false;
    }
}

function resetRoundState() {
    joinedConfirmed = false;
    deploySentAt = 0;
    serverLane = null;
    currentLaneTarget = CFG.homeLane;
    hpHistory.length = 0;
    baseHpHist.length = 0;
    cd.recall = 0; cd.sprint = 0; cd.stroll = 0;
    lastPickId = "";
    lastRecallAt = 0;
}

// ----------------------------- WebSocket (accelerator) ------------------------

function wsConnect() {
    const host = WS_HOSTS[wsHostIdx];
    const url = `${host}/?game=${GAME_ID}`;
    let gotSnapshotThisConn = false;
    frameDecodeFailLogged = false;
    ws = new WebSocket(url);

    const watchdog = setTimeout(() => {
        if (!gotSnapshotThisConn) {
            console.log(`[ws] no decodable snapshots from ${host} after ${CFG.wsWatchdogMs / 1000}s — rotating host (REST keeps playing)`);
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
                console.log(`[ws] undecodable frame (len=${buf.length}, first bytes: ${buf.subarray(0, 8).toString("hex")}) — will keep trying; REST mode continues`);
            }
            return;
        }
        try {
            const s = JSON.parse(text) as Snapshot;
            if (s && Array.isArray(s.units)) {
                gotSnapshotThisConn = true;
                clearTimeout(watchdog);
                onWsSnapshot(s);
            }
        } catch { /* not a snapshot frame */ }
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
    reflex();
}

// ----------------------------- Actions (choke points, reentrancy-safe) --------

async function sendMovement(kind: "sprint" | "stroll"): Promise<boolean> {
    if (!ready(kind) || channelingRecall()) return false;
    const prev = cd[kind];
    cd[kind] = now() + (kind === "sprint" ? CFG.sprintCdMs : CFG.strollCdMs); // optimistic
    if (live() && ws?.readyState === WebSocket.OPEN && now() - lastWsSendAt >= CFG.wsSendGapMs) {
        ws.send(JSON.stringify({ type: kind })); lastWsSendAt = now();
        console.log(`[act] ${kind}`);
        return true;
    }
    const res = await restPost({ action: kind });
    if (actionRejected(res, kind)) {
        // Warning already synced real cooldowns; otherwise brief lockout to avoid hammering.
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
    cd.recall = now() + CFG.recallCdMs; // optimistic
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

    // Optimistic: claim the command slot BEFORE the network call (kills double-fire).
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
        if (ok && wantSprint && res.warning && /sprint/i.test(res.warning)) {
            // lane switch fine, sprint refused — cooldown already synced from warning
        }
    }
    if (ok) {
        console.log(`[act] lane ${committed}->${lane} (${reason})${wantSprint ? " +sprint" : ""}`);
    } else {
        currentLaneTarget = prevTarget;
        lastLaneCmdAt = prevAt;
    }
}

// ----------------------------- Strategy: abilities -----------------------------

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

// ----------------------------- Strategy: LIVE reflex (20 Hz) ------------------

function enemyHeroesInXpRange(u: Unit): number {
    const ef = enemyOf(myFaction!);
    return snap!.units.filter((x) => x.isHero && x.faction === ef && dist(x, u) <= CFG.xpRange).length;
}

function threatCount(u: Unit): number {
    const ef = enemyOf(myFaction!);
    return snap!.units.filter((x) => x.faction === ef && dist(x, u) <= CFG.threatRadius).length;
}

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

// ----------------------------- Strategy: shared judgement ----------------------

// Are we plausibly mid-fight with an enemy hero? Then we HOLD AND SWING.
function inHeroFight(me: MeView, lanes: LaneStat[]): boolean {
    if (live()) {
        const u = myUnit();
        if (!u) return false;
        const ef = enemyOf(myFaction!);
        const heroClose = snap!.units.some((x) => x.isHero && x.faction === ef && dist(x, u) <= CFG.heroFightRadius);
        return heroClose;
    }
    const hereHeroes = lanes.find((l) => l.lane === (serverLane ?? me.lane))?.enemyHeroesHere ?? 0;
    return hereHeroes >= 1 && hpLostOver(CFG.combatWindowMs) > 0;
}

// REST siege detector: the base is genuinely being hit, not scratched.
function baseUnderSiege(): boolean {
    if (baseHpHist.length < 2) return false;
    const cur = baseHpHist[baseHpHist.length - 1];
    let past = baseHpHist[0];
    for (let i = baseHpHist.length - 1; i >= 0; i--) {
        if (cur.t - baseHpHist[i].t >= CFG.baseSiegeWindowMs) { past = baseHpHist[i]; break; }
    }
    const loss = past.hp - cur.hp;
    return loss >= CFG.baseSiegeLossHp || (hpFracOf(cur) < CFG.baseLowFrac && loss > 0);
}

function baseThreatLaneRest(lanes: LaneStat[]): { lane: Lane; siege: boolean } | null {
    const siege = baseUnderSiege();
    const atDoor = lanes.filter(
        (l) => l.frontline !== undefined && myAdvance(l.frontline) <= -CFG.deepFrontline && l.enemyHeroesHere >= 1
    );
    if (!siege && !atDoor.length) return null;
    const pick = (atDoor.length ? atDoor : lanes)
        .slice()
        .sort((a, b) => myAdvance(a.frontline ?? 0) - myAdvance(b.frontline ?? 0))[0];
    return pick ? { lane: pick.lane, siege } : null;
}

function ownBase(): Building | undefined {
    return snap?.buildings.find((b) => b.faction === myFaction && b.type !== "tower");
}

function baseThreatLaneLive(): { lane: Lane; siege: boolean } | null {
    const base = ownBase();
    if (!base || !snap) return null;
    const ef = enemyOf(myFaction!);
    const nearE = snap.units.filter((x) => x.faction === ef && dist(x, base) <= 550);
    if (hpFracOf(base) < 0.5 || nearE.length >= 6 || nearE.some((x) => x.isHero)) {
        const byLane = (l: Lane) => nearE.filter((x) => x.lane === l).length;
        const lane = (["top", "mid", "bot"] as Lane[]).sort((a, b) => byLane(b) - byLane(a))[0];
        return { lane, siege: true };
    }
    return null;
}

function retreatLane(lanes: LaneStat[], exclude: Lane): Lane | null {
    const cands = lanes.filter((l) => l.lane !== exclude && l.ownTowerAlive && l.adv >= 0).sort((a, b) => b.adv - a.adv);
    return cands.length ? cands[0].lane : null;
}

function siegeQualifies(l: LaneStat | undefined): boolean {
    return !!l && l.adv >= 2 && (!l.enemyTowerAlive || l.enemyTowerHp < 400);
}

// ----------------------------- Strategy: macro ---------------------------------

async function macro() {
    if (!joinedConfirmed || !myFaction || roundOverAt) return;
    const me = meView();
    if (!me) return;
    const lanes = laneStats();
    if (!lanes.length) return;
    const effLane: Lane = serverLane ?? currentLaneTarget;

    // Dead: pre-set respawn lane only for a REAL base threat.
    if (!me.alive) {
        const def = live() ? baseThreatLaneLive() : baseThreatLaneRest(lanes);
        if (def) await commandLane(def.lane, "pre-set respawn: defend", { emergency: true });
        return;
    }

    const isLive = live();
    const u = isLive ? myUnit() : undefined;

    // 1) LIVE escape: about to die, recall down -> committed peel (deliberate re-issue).
    if (isLive && u) {
        const projected = incomingDps(CFG.dpsWindowLiveMs) * (CFG.predictLookaheadMs / 1000);
        const dying = (me.hp - projected <= me.maxHp * 0.02 || hpFracOf(me) <= CFG.recallFloorLive) && threatCount(u) > 0;
        if (dying && !divineCovers(me, u) && !ready("recall")) {
            const rl = retreatLane(lanes, effLane);
            if (rl) { await commandLane(rl, "!escape (recall down)", { sprint: true, emergency: true, allowRepeat: true }); return; }
        }
    }

    // 2) REST recall: coarse data => wide buffer + one-poll death projection.
    //    Only to deny a bounty (enemy hero in our lane); otherwise dying is cheaper.
    if (!isLive && ready("recall")) {
        const hereHeroes = lanes.find((l) => l.lane === effLane)?.enemyHeroesHere ?? 0;
        const recentDrop = hpLostOver(2000); // ~last poll or two
        const dieNextPoll = me.hp - recentDrop * 1.4 <= 0;
        if (hereHeroes >= 1 && (hpFracOf(me) <= CFG.recallFloorRest && (recentDrop > 0 || dieNextPoll))) {
            await sendRecall(`rest: ${(hpFracOf(me) * 100) | 0}% hp, dropped ${recentDrop | 0} recently, enemy heroes in lane=${hereHeroes}`);
            return;
        }
    }

    // 3) Base defense — only for a REAL siege now.
    const def = isLive ? baseThreatLaneLive() : baseThreatLaneRest(lanes);
    if (def) {
        const farForward = isLive
            ? (u && ownBase() ? dist(u, ownBase()!) > CFG.nearBaseRadius * 1.6 : false)
            : myAdvance(lanes.find((l) => l.lane === effLane)?.frontline ?? 0) > CFG.farForwardAdvance;
        if (def.siege && farForward && ready("recall")) { await sendRecall("defend base (teleport home)"); return; }
        await commandLane(def.lane, "!defend base", { sprint: true, emergency: true });
        return;
    }

    // 4) FIGHT-LOCK: enemy hero engaged with us -> hold and swing. No rotations.
    if (inHeroFight(me, lanes)) return;

    // 5) Mid-stack with hysteresis; siege-hold so siege & restack can't oscillate.
    const mid = lanes.find((l) => l.lane === "mid")!;
    const here = lanes.find((l) => l.lane === effLane);
    if (effLane === "mid") {
        if (mid.adv <= CFG.midBailAdv) {
            const side = lanes.filter((l) => l.lane !== "mid")
                .map((l) => ({ l, s: l.adv + (l.ownTowerAlive ? 1 : 0) - l.enemyHeroesHere }))
                .sort((a, b) => b.s - a.s)[0];
            if (side) await commandLane(side.l.lane, "mid lost, rotate", { sprint: true });
        }
        // else hold mid and swing.
        return;
    }
    // Not in mid:
    if (siegeQualifies(here)) return; // we're profitably sieging here — stay on the tower.
    if (mid.adv >= CFG.midRestackAdv) { await commandLane("mid", "restack mid"); return; }
    if (me.level >= 7) {
        const push = lanes.filter((l) => l.lane !== effLane && siegeQualifies(l)).sort((a, b) => b.adv - a.adv)[0];
        if (push) await commandLane(push.lane, "siege", { sprint: true });
    }
}

// ----------------------------- Heartbeat & boot --------------------------------

setInterval(() => {
    if (!joinedConfirmed) return;
    const me = meView();
    if (!me) return;
    const mode = live() ? "LIVE(20Hz)" : `REST(1.5s)${codec ? "" : " [WS frames undecoded]"}`;
    console.log(
        `[status] mode=${mode} lane=${serverLane ?? "?"} lvl=${me.level} hp=${(hpFracOf(me) * 100) | 0}%` +
        ` recall=${ready("recall") ? "ready" : Math.ceil((cd.recall - now()) / 1000) + "s"} wsFrames=${wsFrames}`
    );
}, 30_000);

console.log(`[boot] ${AGENT_NAME} | game ${GAME_ID} | melee/classic | stack ${CFG.homeLane} | items: ${CFG.itemPreference.join(" > ")}`);
wsConnect();
setInterval(pollRest, CFG.restPollMs);
setInterval(() => {
    if (macroBusy) return;
    macroBusy = true;
    macro().catch((e) => console.log("[macro] error:", (e as Error).message)).finally(() => { macroBusy = false; });
}, CFG.macroMs);
void pollRest();