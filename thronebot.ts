/**
 * Throne Wars bot (thronebot.ts) — fork of our Defense-of-the-Agents bot v0.6
 * ==========================================================================
 * Same engine, same strategy brain (unfair-fight lane utility, mage kiting,
 * tower juke, base dance, recall doctrine — all inherited unchanged). Only the
 * NETWORKING differs, because Throne Wars (thronewars.gg) is web2 with NO login:
 *
 *   - Identity is SERVER-ASSIGNED (e.g. "SilentDrake137"). There is no API key.
 *     We create a session on first contact, keep its cookie, and read back our
 *     assigned name from the game state (whoever appears that we didn't already
 *     know = us, confirmed by matching our chosen lane/class).
 *   - Endpoints are AUTO-DISCOVERED at boot: the player-facing docs don't list
 *     them, so we probe a set of candidate REST bases / paths / WS hosts (all
 *     built from the same dev's DoA shapes) and lock onto whatever answers.
 *   - Rooms use ?room=N. For an 18-bot party, force one room so they fill it.
 *
 * Because the API is unverified (alpha, no bot docs), the discovery layer logs
 * exactly what it found; if a probe path is wrong, the log tells us what to fix.
 *
 * ENV (all optional):
 *   TW_BASE         override REST base (skip discovery), e.g. https://thronewars.gg
 *   TW_WS           override WS url,   e.g. wss://thronewars.gg
 *   TW_ROOM         force a room number (all instances -> same room = a party)
 *   TW_NAME         desired name hint (server may override / ignore)
 *   TW_CLASS        mage | melee | ranged   (default mage)
 *   TW_SKIN         skin id (default: none — base mage)
 *   TW_ITEM         item id (default: ring_of_regen; free for everyone here)
 *   TW_LANE         top | mid | bot  (default mid)
 *   TW_INSTANCES    launch N in-process bots from ONE command (default 1)
 *   TW_JOIN_STAGGER ms between staggered instance joins (default 1500)
 *   DEBUG=1 / --debug
 *
 * Run one:   npx tsx thronebot.ts
 * Run a party of 18 (this file re-launches itself as 18 child processes):
 *            TW_INSTANCES=18 TW_ROOM=1 npx tsx thronebot.ts
 * (Each child is its own OS process = its own session/name, exactly how the game
 *  sees 18 real players. You can also launch 18 terminals by hand with same TW_ROOM.)
 */

import "dotenv/config";
import WebSocket from "ws";
import zlib from "node:zlib";
import fs from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

// ESM has no __filename; derive it so we can re-launch this file as children.
const SELF_PATH = fileURLToPath(import.meta.url);

// ===== Multi-instance fan-out: if asked for N>1 and we're the parent, spawn N
// children (each a normal single bot) with staggered joins, then do nothing else.
const _INSTANCES = Math.max(1, parseInt(process.env.TW_INSTANCES ?? "1", 10) || 1);
const _STAGGER = parseInt(process.env.TW_JOIN_STAGGER ?? "1500", 10) || 1500;
const _IS_PARENT = _INSTANCES > 1 && !process.env.TW_CHILD;
if (_IS_PARENT) {
    console.log(`[party] launching ${_INSTANCES} bots into room ${process.env.TW_ROOM ?? "(auto)"} …`);
    for (let i = 0; i < _INSTANCES; i++) {
        setTimeout(() => {
            const botNumber = i + 1;
            const auth = process.env[`TW_AUTH_${botNumber}`] || process.env.TW_AUTH || "";
            const name = process.env[`TW_NAME_${botNumber}`] || process.env.TW_NAME || "";
            if (!auth) {
                console.log(`[party] bot #${botNumber} has NO token (set TW_AUTH_${botNumber}=Bearer <uuid> in .env) — skipping`);
                return;
            }
            if (!name) {
                console.log(`[party] WARN bot #${botNumber} has no TW_NAME_${botNumber} — falling back to fuzzy self-ID, which is unreliable when bots share a lane/class.`);
            }
            const child = spawn(process.execPath, ["--import", "tsx", SELF_PATH], {
                stdio: "inherit",
                env: {
                    ...process.env,
                    TW_CHILD: String(botNumber),
                    TW_INSTANCES: "1",
                    // Each bot gets ITS OWN token + server-assigned username, so the
                    // in-game identity is exact (no class/lane guessing collisions).
                    TW_AUTH: auth,
                    TW_NAME: name,
                },
            });
            child.on("exit", (c) => console.log(`[party] bot #${botNumber} (${name || "unnamed"}) exited (${c})`));
        }, i * _STAGGER);
    }
}

// ----------------------------- Config ----------------------------------------

const CHILD_ID = process.env.TW_CHILD ?? "1";
const DEBUG = process.env.DEBUG === "1" || process.env.DOTA_DEBUG === "1" || process.argv.includes("--debug");

// Candidate hosts/paths probed at boot (same-dev DoA shapes are the template).
const REST_CANDIDATES = [process.env.TW_BASE, "https://thronewars.gg", "https://api.thronewars.gg", "https://game.thronewars.gg", "https://server.thronewars.gg"].filter(Boolean) as string[];
const WS_CANDIDATES = [process.env.TW_WS, "wss://thronewars.gg", "wss://api.thronewars.gg", "wss://game.thronewars.gg", "wss://server.thronewars.gg"].filter(Boolean) as string[];
const STATE_PATHS = ["/api/game/state", "/api/state", "/api/room/state", "/state"];
const DEPLOY_PATHS = ["/api/strategy/deployment", "/api/deployment", "/api/deploy", "/api/play"];

const FORCED_ROOM = process.env.TW_ROOM ? parseInt(process.env.TW_ROOM, 10) : null;
const DEF_CLASS = (process.env.TW_CLASS as HeroClass) || "mage";
const DEF_SKIN = process.env.TW_SKIN || undefined;
const DEF_ITEM = process.env.TW_ITEM || "ring_of_regen";
const DEF_LANE = (process.env.TW_LANE as Lane) || "mid";

// Discovered/session state (filled by discover()).
let REST_BASE = process.env.TW_BASE || "";
let WS_URL = process.env.TW_WS || "";
let STATE_PATH = "/api/game/state";
let DEPLOY_PATH = "/api/strategy/deployment";
let COOKIE = "";                        // session cookie jar for this process
let AGENT_NAME = process.env.TW_NAME || ""; // learned from state if server-assigned
let ROOM: number | null = FORCED_ROOM;
const GAME_ID = 0; // unused in TW (room-based); kept so shared code compiles

interface DeployPref { heroClass: HeroClass; skin?: string; label: string; }
const SKIN_PREFS: DeployPref[] = DEF_SKIN ? [{ heroClass: DEF_CLASS, skin: DEF_SKIN, label: `${DEF_CLASS} ${DEF_SKIN}` }] : [];
const FARCASTER: DeployPref = { heroClass: DEF_CLASS, label: `${DEF_CLASS}` };
const BASE_PREF: DeployPref = { heroClass: DEF_CLASS, label: `base ${DEF_CLASS}` };
// Throne Wars: everything is Silver-unlocked (no wallet). Try the chosen skin (if
// any) first, then fall back to the base class. Cat Ears/Ring both free here.
const DEPLOY_PREFS: DeployPref[] = [...SKIN_PREFS, BASE_PREF];

const CFG = {
    itemPreference: [DEF_ITEM, "ring_of_regen", null] as (string | null)[],
    homeLane: DEF_LANE,

    // Cadence
    restPollMs: 1500,
    macroMs: 350,
    wsSendGapMs: 300,
    restPostGapMs: 900,
    joinConfirmGraceMs: 25_000,
    rejoinDelayMs: 8_000,
    wsWatchdogMs: 8_000,
    liveFreshMs: 1_200,

    // Recall
    recallFloorLive: 0.06,
    recallFloorRest: 0.25,
    predictLookaheadMs: 500,
    dpsWindowLiveMs: 600,
    xpRange: 350,             // enemy hero this close banks our death bounty
    combatWindowMs: 5_000,

    // Base defense (positional): heroes + creeps actually AT our base
    baseDefendRadius: 650,
    defendCreepCount: 3,
    atEnemyBaseAdv: 75,       // our advance beyond this => we're racing, don't recall
    towerFinishHp: 300,       // enemy tower nearly dead in our lane => keep hitting it

    // Unfair-fight lane utility
    switchMargin: 1.6,        // candidate must beat current lane by this (switching costs a push)
    stickiness: 1.5,
    midBias: 1.5,
    towerEdge: 4,             // fighting near OUR live tower = this much hero-level equity
    unfairForUs: 2,           // level-equity >= this with enemies present = take the fight
    unfairAgainstUs: -2,      // <= this = avoid that lane
    reachSlack: 8,            // forward-only reachability slack (advance units)
    fightLockRadius: 350,     // enemy hero this close = we're in a fight, hold

    // Mage kiting (conservative): step away ONLY when a melee/ranged hero is in
    // actual attack reach of us AND we're visibly bleeding HP AND we don't clearly
    // out-HP them. No spells learned yet = never kite (nothing casts while walking).
    kiteMeleeReach: 90,       // a melee foe within this is actually swinging at us
    kiteRangedReach: 190,     // a ranged foe within this is actually shooting us
    kiteHpEdge: 0.25,         // our HP-frac lead at/above this -> stand and win the trade
    kiteGapMs: 1700,          // re-tap cadence (vertical peel is ~1.5s)
    kiteDirHoldMs: 4500,      // keep stepping ONE direction this long before flipping
    kiteSecureFoeFrac: 0.35,  // foe below this (and below us) -> stand and finish them

    // Tower juke: one tap perpendicular the moment the tower hits us -> quick step,
    // creeps inherit aggro, we re-aggro the tower. (top tower: tap bot; bot: top; mid: top)
    jukeCdMs: 7000,           // at most one juke per aggro cycle
    jukeStepMs: 1600,         // restore our lane assignment after the step
    jukeMinCreeps: 3,         // need creeps present to inherit the aggro
    jukeHitHp: 40,            // only building-sized hits trigger a juke (creeps ~10)
    deepAdvance: 55,          // past the enemy tower line: pathing warps, kite differently

    // Base dance: at the enemy nexus, spam our own lane's tap to bob out of the
    // base arrow's reach while creeps catch aggro.
    baseDanceReach: 340,      // we're "at the base" within this distance
    baseArrowDmg: 60,         // nexus arrow damage (3-hit death rule)

    // Lane command discipline
    laneHoldMs: 6_000,
    escapeSpamMs: 1_500,

    // Cooldowns (ms)
    recallCdMs: 120_000,
    sprintCdMs: 25_000,
    strollCdMs: 25_000,
    recallChannelMs: 2_600,
};

// ----------------------------- Types & enums ----------------------------------

type Lane = "top" | "mid" | "bot";
type Faction = "human" | "orc";
type HeroClass = "melee" | "ranged" | "mage";
const LANES: Lane[] = ["top", "mid", "bot"];
const FACTIONS: Faction[] = ["human", "orc"];
const CLASSES: HeroClass[] = ["melee", "ranged", "mage"];

interface Ability { id: string; level: number; cooldownRemaining?: number; cooldownTotal?: number; activeRemaining?: number; }
interface U {
    id: number; type: number; faction: Faction; x: number; y: number;
    hp: number; maxHp: number; lane: Lane; isHero: boolean;
    ownerName?: string; level?: number; skin?: string | null;
}
interface Bld { id: number; faction: Faction; isTower: boolean; x: number; y: number; hp: number; maxHp: number; lane: Lane | null; }
interface ScoreEntry {
    name: string; faction: Faction; heroClass: HeroClass; lane: Lane;
    level: number; hp: number; maxHp: number; alive: boolean; respawnTimer?: number;
    abilities: Ability[]; abilityChoices?: string[]; recallCooldownMs?: number; skin?: string | null;
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
    lane: Lane; friendly: number; enemy: number; adv: number;
    enemyHeroesHere: number; allyHeroesHere: number;      // heroes excl. me
    enemyHeroLvls: number; allyHeroLvls: number;          // level sums (alive only)
    ownTowerAlive: boolean; ownTowerHp: number; enemyTowerAlive: boolean; enemyTowerHp: number;
    frontline: number; // human convention: -100 human base .. +100 orc base
}
interface MeView {
    faction: Faction; lane: Lane; level: number; hp: number; maxHp: number; alive: boolean;
    heroClass: HeroClass; abilities: Ability[]; abilityChoices?: string[]; skin?: string | null;
}

// ----------------------------- Frame adapters ----------------------------------

function adaptUnit(raw: any): U | null {
    if (Array.isArray(raw)) {
        const isHero = typeof raw[11] === "string";
        return {
            id: raw[0], type: raw[1], faction: FACTIONS[raw[2]] ?? "human",
            x: raw[3], y: raw[4], hp: raw[5], maxHp: raw[6],
            lane: LANES[raw[9]] ?? "mid", isHero,
            ownerName: isHero ? raw[11] : undefined,
            level: isHero ? raw[13] : undefined,
            skin: isHero ? raw[18] ?? null : undefined,
        };
    }
    if (raw && typeof raw === "object") {
        return {
            id: raw.id, type: 0, faction: raw.faction, x: raw.x, y: raw.y,
            hp: raw.hp, maxHp: raw.maxHp, lane: raw.lane, isHero: !!raw.isHero,
            ownerName: raw.ownerName, level: raw.heroLevel, skin: raw.skin ?? null,
        };
    }
    return null;
}

function adaptBuilding(raw: any): Bld | null {
    if (Array.isArray(raw)) {
        return {
            id: raw[0], faction: FACTIONS[raw[1]] ?? "human", isTower: raw[2] === 1,
            x: raw[3], y: raw[4], hp: raw[5], maxHp: raw[6],
            lane: raw[7] >= 0 ? LANES[raw[7]] ?? null : null,
        };
    }
    if (raw && typeof raw === "object") {
        return { id: raw.id, faction: raw.faction, isTower: raw.type === "tower", x: raw.x, y: raw.y, hp: raw.hp, maxHp: raw.maxHp, lane: raw.lane ?? null };
    }
    return null;
}

function adaptAbility(p: any): Ability {
    if (Array.isArray(p)) {
        return {
            id: p[0], level: p[1],
            cooldownRemaining: typeof p[2] === "number" && p[2] >= 0 ? p[2] : undefined,
            cooldownTotal: typeof p[3] === "number" && p[3] >= 0 ? p[3] : undefined,
            activeRemaining: typeof p[4] === "number" && p[4] >= 0 ? p[4] : undefined,
        };
    }
    return { id: p.id, level: p.level, cooldownRemaining: p.cooldownRemaining, cooldownTotal: p.cooldownTotal, activeRemaining: p.activeRemaining };
}

function adaptScore(raw: any): ScoreEntry | null {
    if (Array.isArray(raw)) {
        return {
            name: raw[0], faction: FACTIONS[raw[1]] ?? "human",
            heroClass: CLASSES[raw[2]] ?? "melee", lane: LANES[raw[3]] ?? "mid",
            level: raw[4], hp: raw[7], maxHp: raw[8],
            alive: raw[10] === 1 || raw[10] === true,
            respawnTimer: typeof raw[11] === "number" && raw[11] >= 0 ? raw[11] : undefined,
            abilities: Array.isArray(raw[13]) ? raw[13].map(adaptAbility) : [],
            recallCooldownMs: typeof raw[16] === "number" && raw[16] > 0 ? raw[16] : 0,
            abilityChoices: Array.isArray(raw[19]) ? raw[19] : undefined,
            skin: raw[23] ?? null,
        };
    }
    if (raw && typeof raw === "object" && "name" in raw) {
        return { ...raw, abilities: (raw.abilities ?? []).map(adaptAbility) } as ScoreEntry;
    }
    return null;
}

// ----------------------------- State ------------------------------------------

let ws: WebSocket | null = null;
let wsHostIdx = 0;
let wsFrames = 0;
let frameDecodeFailLogged = false;
let frameSampleWritten = false;
let lastWsSnapshotAt = 0;

// Adapted world (rebuilt every WS frame)
let W: { units: U[]; blds: Bld[]; sb: ScoreEntry[]; winner: Faction | null } | null = null;

let rest: RestState | null = null;
let restPolling = false;
let macroBusy = false;
let pickBusy = false;

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
let skinGranted = true;
let roundOverAt = 0;

let lastWsSendAt = 0;
let lastRestPostAt = 0;
let restBackoffUntil = 0;

let lastPickPostAt = 0;
let lastPickId = "";
let lastRecallAt = 0;
let lastDbgAt = 0;
let lastKiteAt = 0;
let kiting = false;
let kiteReturnLane: Lane | null = null;
let kiteDir: Lane | null = null;
let kiteDirUntil = 0;
let lastJukeAt = 0;
let jukeHomeLane: Lane | null = null;
let jukeRestoreAt = 0;
let lastSay = "";
const baseHpHist: { t: number; hp: number }[] = [];

const hpHistory: { t: number; hp: number; maxHp: number }[] = [];
const cd = { recall: 0, sprint: 0, stroll: 0 };

const now = () => Date.now();
const ready = (k: keyof typeof cd) => now() >= cd[k];
const live = () => now() - lastWsSnapshotAt < CFG.liveFreshMs && !!W;
const channelingRecall = () => now() - lastRecallAt < CFG.recallChannelMs;
const dbg = (msg: string) => { if (DEBUG) console.log(`[think] ${msg}`); };
// Announce hold-states / intentions once per change, so decisions are auditable
// while spectating even without --debug.
function say(msg: string) { if (msg !== lastSay) { lastSay = msg; console.log(`[hold] ${msg}`); } }

// ----------------------------- Small helpers ----------------------------------

const enemyOf = (f: Faction): Faction => (f === "human" ? "orc" : "human");
const dist = (a: { x: number; y: number }, b: { x: number; y: number }) => Math.hypot(a.x - b.x, a.y - b.y);
const hpFracOf = (m: { hp: number; maxHp: number }) => (m.maxHp ? m.hp / m.maxHp : 1);
const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v));

// Frontlines use the HUMAN convention (-100 = human base, +100 = orc base).
const myAdvance = (frontlineHuman: number) => (myFaction === "human" ? frontlineHuman : -frontlineHuman);
const humanAdvOfX = (x: number) => clamp(((x - 1600) / 1400) * 100, -100, 100);

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

// Largest single-sample HP drop within the window. At 20 Hz one tower/base arrow
// is one big step (70/60), while creep hits are ~10 — clean source separation.
function biggestHitWithin(windowMs: number): number {
    let max = 0;
    const t = now();
    for (let i = hpHistory.length - 1; i > 0; i--) {
        if (t - hpHistory[i].t > windowMs) break;
        const d = hpHistory[i - 1].hp - hpHistory[i].hp;
        if (d > max) max = d;
    }
    return max;
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

// Track OUR base's HP so "threatened" and "actually being damaged" are distinct.
function trackBaseHp() {
    let hp: number | null = null;
    const b = myBase();
    if (b) hp = b.hp;
    else if (rest && myFaction) hp = rest.bases[myFaction]?.hp ?? null;
    if (hp === null) return;
    const t = now();
    baseHpHist.push({ t, hp });
    while (baseHpHist.length && t - baseHpHist[0].t > 6000) baseHpHist.shift();
}

function baseTakingDamage(windowMs = 3000): boolean {
    if (baseHpHist.length < 2) return false;
    const b = baseHpHist[baseHpHist.length - 1];
    for (let i = baseHpHist.length - 1; i >= 0; i--) {
        if (b.t - baseHpHist[i].t >= windowMs) return baseHpHist[i].hp - b.hp > 0;
    }
    return baseHpHist[0].hp - b.hp > 0;
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
                    console.log(`[ws] frame codec: ${name}${offset ? ` (+${offset}B header)` : ""} — fast feed unlocked`);
                    return out;
                }
            } catch { /* next */ }
        }
    }
    return null;
}

// ----------------------------- World views ------------------------------------

function myUnit(): U | undefined {
    return W?.units.find((u) => u.isHero && u.ownerName === AGENT_NAME);
}

function mySb(): ScoreEntry | undefined {
    return W?.sb.find((h) => h.name === AGENT_NAME);
}

function meView(): MeView | null {
    if (live()) {
        const s = mySb();
        if (s) return s;
    }
    const r = rest?.heroes?.find((h) => h.name === AGENT_NAME);
    if (!r) return null;
    return {
        faction: r.faction, lane: r.lane, level: r.level, hp: r.hp, maxHp: r.maxHp,
        alive: r.alive, heroClass: r.class, abilities: r.abilities ?? [],
        abilityChoices: r.abilityChoices, skin: undefined,
    };
}

// Roster of heroes (name/faction/class/lane/level/alive) from best source.
function roster(): { name: string; faction: Faction; heroClass: HeroClass; lane: Lane; level: number; alive: boolean }[] {
    if (live() && W!.sb.length) return W!.sb.map((h) => ({ name: h.name, faction: h.faction, heroClass: h.heroClass, lane: h.lane, level: h.level, alive: h.alive }));
    return rest?.heroes?.map((h) => ({ name: h.name, faction: h.faction, heroClass: h.class, lane: h.lane, level: h.level, alive: h.alive })) ?? [];
}

function laneStats(): LaneStat[] {
    if (!myFaction) return [];
    const ef = enemyOf(myFaction);
    const heroes = roster();

    const heroBits = (lane: Lane) => {
        const enemies = heroes.filter((h) => h.faction === ef && h.alive && h.lane === lane);
        const allies = heroes.filter((h) => h.faction === myFaction && h.alive && h.lane === lane && h.name !== AGENT_NAME);
        return {
            enemyHeroesHere: enemies.length, allyHeroesHere: allies.length,
            enemyHeroLvls: enemies.reduce((s, h) => s + h.level, 0),
            allyHeroLvls: allies.reduce((s, h) => s + h.level, 0),
        };
    };

    if (live()) {
        return LANES.map((lane) => {
            const inLane = W!.units.filter((u) => u.lane === lane);
            const friendlyU = inLane.filter((u) => u.faction === myFaction);
            const enemyU = inLane.filter((u) => u.faction === ef);
            const et = W!.blds.find((b) => b.isTower && b.faction === ef && b.lane === lane);
            const ot = W!.blds.find((b) => b.isTower && b.faction === myFaction && b.lane === lane);
            // Clash point: midpoint between each side's leading edge (human marches +x).
            const humanU = myFaction === "human" ? friendlyU : enemyU;
            const orcU = myFaction === "human" ? enemyU : friendlyU;
            const humanFront = humanU.length ? Math.max(...humanU.map((u) => u.x)) : 300;
            const orcFront = orcU.length ? Math.min(...orcU.map((u) => u.x)) : 2900;
            const frontline = humanAdvOfX((humanFront + orcFront) / 2);
            return {
                lane, friendly: friendlyU.length, enemy: enemyU.length, adv: friendlyU.length - enemyU.length,
                ...heroBits(lane),
                ownTowerAlive: !!ot && ot.hp > 0, ownTowerHp: ot?.hp ?? 0,
                enemyTowerAlive: !!et && et.hp > 0, enemyTowerHp: et?.hp ?? 0,
                frontline,
            };
        });
    }

    if (rest) {
        return LANES.map((lane) => {
            const l = rest!.lanes[lane];
            const friendly = l?.[myFaction!] ?? 0;
            const enemy = l?.[ef] ?? 0;
            const et = rest!.towers.find((t) => t.faction === ef && t.lane === lane);
            const ot = rest!.towers.find((t) => t.faction === myFaction && t.lane === lane);
            return {
                lane, friendly, enemy, adv: friendly - enemy,
                ...heroBits(lane),
                ownTowerAlive: !!ot?.alive, ownTowerHp: ot?.hp ?? 0,
                enemyTowerAlive: !!et?.alive, enemyTowerHp: et?.hp ?? 0,
                frontline: l?.frontline ?? 0,
            };
        });
    }
    return [];
}

function myBase(): Bld | undefined {
    return W?.blds.find((b) => !b.isTower && b.faction === myFaction);
}

function enemyBase(): Bld | undefined {
    return myFaction ? W?.blds.find((b) => !b.isTower && b.faction === enemyOf(myFaction!)) : undefined;
}

// Our own advance position (-100 our base .. +100 their base).
function myAdv(here: LaneStat): number {
    const u = myUnit();
    if (u) return myAdvance(humanAdvOfX(u.x));
    return myAdvance(here.frontline);
}

function enemyPhysicalShare(): number {
    const ef = myFaction ? enemyOf(myFaction) : null;
    const es = roster().filter((h) => h.faction === ef);
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
    const gap = opts.urgent ? 400 : CFG.restPostGapMs; //
    if (now() < restBackoffUntil || now() - lastRestPostAt < gap) return { ok: false, status: 0, text: "throttled" }; //
    lastRestPostAt = now(); //
    try {
        // Room-based, cookie-authenticated with support for custom Authorization tokens
        const payload = ROOM !== null ? { room: ROOM, ...body } : body; //

        // Assemble the headers context
        const headers: Record<string, string> = {
            "Content-Type": "application/json"
        };

        // Include fallback for vanilla session cookies
        if (COOKIE) {
            headers["Cookie"] = COOKIE;
        }

        // Inject explicit Authorization header extracted from browser
        if (process.env.TW_AUTH) {
            headers["Authorization"] = process.env.TW_AUTH;
        }

        const r = await fetch(`${REST_BASE}${DEPLOY_PATH}`, { //
            method: "POST",
            headers: headers,
            body: JSON.stringify(payload), //
        });
        captureCookie(r); //
        const text = await r.text(); //
        let warning: string | undefined; //
        if (r.status === 429) { restBackoffUntil = now() + 10_000; console.log("[rest] 429 — backing off 10s"); } //
        else if (!r.ok) console.log(`[rest] deploy ${r.status}: ${text}`); //
        else {
            try {
                const j = JSON.parse(text); //
                if (j.warning) { warning = String(j.warning); console.log(`[rest] warning: ${warning}`); syncCooldownFromWarning(warning); } //
            } catch { }
        }
        return { ok: r.ok, status: r.status, text, warning }; //
    } catch (e) {
        console.log("[rest] POST error:", (e as Error).message); //
        return { ok: false, status: 0, text: String(e) }; //
    }
}

// Keep the session cookie the server hands us (this is our "login" in web2 TW).
function captureCookie(r: Response) {
    const sc = r.headers.get("set-cookie");
    if (sc) COOKIE = sc.split(";")[0] + (COOKIE && !COOKIE.includes(sc.split("=")[0]) ? "; " + COOKIE : "");
}

const actionRejected = (res: RestResult, action: string) =>
    !res.ok || (!!res.warning && new RegExp(action, "i").test(res.warning));

async function pollRest() {
    if (restPolling) return;
    restPolling = true;
    try {
        const q = ROOM !== null ? `?room=${ROOM}` : "";
        const r = await fetch(`${REST_BASE}${STATE_PATH}${q}`, { headers: COOKIE ? { Cookie: COOKIE } : {} });
        captureCookie(r);
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

        // Identity. If TW_NAME was provided (the real server-assigned username for
        // this token), it is AUTHORITATIVE — never let fuzzy class+lane guessing
        // override it, which is what caused bots to swap identities on relaunch.
        if (!AGENT_NAME && !process.env.TW_NAME) {
            const mine = identifySelf(rest.heroes);
            if (mine) { AGENT_NAME = mine; console.log(`[id] self-identified (fuzzy) as: ${AGENT_NAME}`); }
        } else if (!joinedConfirmed && AGENT_NAME && !rest.heroes.some((h) => h.name === AGENT_NAME)) {
            // Named but not yet on the board — just waiting for our deploy to land.
        }

        const meR = rest.heroes.find((h) => h.name === AGENT_NAME);
        if (meR) {
            if (!joinedConfirmed) {
                const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
                console.log(`[join] confirmed: ${meR.faction} ${meR.class} in ${meR.lane} as ${AGENT_NAME} (requested: ${pref.label})`);
            }
            joinedConfirmed = true;
            myFaction = meR.faction;
            if (now() - lastLaneCmdAt > 3500) serverLane = meR.lane;
            if (typeof meR.recallCooldownMs === "number" && meR.recallCooldownMs > 0)
                cd.recall = Math.max(cd.recall, now() + meR.recallCooldownMs);
            if (!live() && meR.alive) recordHp(meR.hp, meR.maxHp);
            await pickIfPending(meR.class, meR.abilities as Ability[], meR.abilityChoices);
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

// Roster of hero names present the moment BEFORE we deployed — anyone new after is us.
let preDeployNames: Set<string> | null = null;
function identifySelf(heroes: RestHero[]): string | null {
    if (AGENT_NAME && heroes.some((h) => h.name === AGENT_NAME)) return AGENT_NAME;
    if (!preDeployNames) return null; // haven't deployed yet
    const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
    const fresh = heroes.filter((h) => !preDeployNames!.has(h.name));
    // Prefer a fresh hero matching our class + chosen lane; else any fresh hero.
    return (fresh.find((h) => h.class === pref.heroClass && h.lane === DEF_LANE)
        ?? fresh.find((h) => h.class === pref.heroClass)
        ?? fresh[0])?.name ?? null;
}

async function pickIfPending(heroClass: HeroClass, abilities: Ability[], choices?: string[]) {
    if (!choices?.length) { lastPickId = ""; return; }
    if (pickBusy) return;
    const pick = nextAbilityPick(heroClass, abilities, choices);
    if (!pick) return;
    if (pick === lastPickId && now() - lastPickPostAt < 5000) return;
    pickBusy = true;
    try {
        const res = await restPost({ abilityChoice: pick }, { urgent: true });
        if (res.ok) {
            lastPickId = pick; lastPickPostAt = now();
            console.log(`[act] ability -> ${pick}   (choices were: ${choices.join(", ")})`);
        }
    } finally {
        pickBusy = false;
    }
}

async function maybeDeploy() {
    if (deployInFlight) return;
    if (now() - lastDeployFailAt < 4000) return;
    const retryDue = deploySentAt > 0 && now() - deploySentAt > CFG.joinConfirmGraceMs;
    if (deploySentAt > 0 && !retryDue) return;
    deployInFlight = true;
    try {
        const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
        const item = CFG.itemPreference[itemIdx] ?? null;
        // Snapshot who's already in the room so we can recognise our new hero after.
        if (rest?.heroes) preDeployNames = new Set(rest.heroes.map((h) => h.name));
        const body: Record<string, any> = { heroClass: pref.heroClass, heroLane: CFG.homeLane, message: "bot online" };
        // Identity comes from the auth token (TW_NAME is only our local lookup key),
        // so we do NOT send a name in the deploy body — a mismatch could confuse the server.
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
            if (/skin|class|wallet/i.test(res.text) && prefIdx < DEPLOY_PREFS.length - 1) {
                prefIdx++; deployFails = 0;
                console.log(`[join] skin/class rejected, falling back to ${DEPLOY_PREFS[prefIdx].label}`);
            } else if (/item|equip/i.test(res.text) && itemIdx < CFG.itemPreference.length - 1) {
                itemIdx++; deployFails = 0;
                console.log(`[join] item rejected, falling back to ${CFG.itemPreference[itemIdx] ?? "no item"}`);
            } else if (/full/i.test(res.text)) {
                deploySentAt = now();
            } else if (deployFails >= 3) {
                deployFails = 0;
                if (prefIdx < DEPLOY_PREFS.length - 1) {
                    prefIdx++;
                    console.log(`[join] repeated rejections, falling back to ${DEPLOY_PREFS[prefIdx].label}`);
                } else if (itemIdx < CFG.itemPreference.length - 1) {
                    itemIdx++;
                    console.log(`[join] repeated rejections, dropping item to ${CFG.itemPreference[itemIdx] ?? "none"}`);
                } else {
                    console.log("[join] repeated rejections on the plainest deploy — backing off 30s");
                    lastDeployFailAt = now() + 26_000;
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
    kiting = false;
    kiteReturnLane = null;
    kiteDir = null;
    kiteDirUntil = 0;
    lastJukeAt = 0;
    jukeHomeLane = null;
    jukeRestoreAt = 0;
    lastSay = "";
    baseHpHist.length = 0;
}

// ----------------------------- WebSocket ---------------------------------------

function wsConnect() {
    if (!WS_URL) { setTimeout(wsConnect, 1000); return; } // wait for discovery
    const host = WS_URL;
    const q = ROOM !== null ? `?room=${ROOM}` : "";
    let gotSnapshotThisConn = false;
    frameDecodeFailLogged = false;
    ws = new WebSocket(`${host}/${q}`, COOKIE ? { headers: { Cookie: COOKIE } } : undefined);

    const watchdog = setTimeout(() => {
        if (!gotSnapshotThisConn) {
            console.log(`[ws] no decodable snapshots from ${host} — retrying (REST keeps playing)`);
            try { ws?.close(); } catch { }
        }
    }, CFG.wsWatchdogMs);

    ws.on("open", () => {
        console.log(`[ws] connected: ${host}${q}`);
        // TW is cookie-authenticated; send a room subscribe in case the server wants one.
        try { ws!.send(JSON.stringify({ type: "subscribe", room: ROOM })); } catch { }
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
            const s = JSON.parse(text);
            if (s && Array.isArray(s.units)) {
                gotSnapshotThisConn = true;
                clearTimeout(watchdog);
                onWsSnapshot(s, text);
            }
        } catch { /* not a snapshot */ }
    });

    ws.on("close", () => {
        clearTimeout(watchdog);
        setTimeout(wsConnect, 1500);
    });
    ws.on("error", (e) => console.log("[ws] error:", (e as Error).message));
}

let schemaDumped = false;
function onWsSnapshot(s: any, rawText: string) {
    const units = (s.units as any[]).map(adaptUnit).filter((u): u is U => !!u);
    const blds = ((s.buildings ?? []) as any[]).map(adaptBuilding).filter((b): b is Bld => !!b);
    const sb = ((s.heroScoreboard ?? []) as any[]).map(adaptScore).filter((e): e is ScoreEntry => !!e);
    W = { units, blds, sb, winner: s.winner ?? null };
    lastWsSnapshotAt = now();

    if (!schemaDumped) {
        schemaDumped = true;
        console.log(`[schema] fast feed on — adapted ${units.length} units, ${blds.length} buildings, ${sb.length} scoreboard entries (positional mode ${units.some((u) => u.isHero) ? "ON" : "limited"})`);
        if (!frameSampleWritten) {
            frameSampleWritten = true;
            try { fs.writeFileSync("frame-sample.json", rawText); } catch { }
        }
    }

    const me = mySb();
    if (me) {
        myFaction = me.faction;
        if (now() - lastLaneCmdAt > 3500) serverLane = me.lane;
        if (typeof me.recallCooldownMs === "number" && me.recallCooldownMs > 0)
            cd.recall = Math.max(cd.recall, now() + me.recallCooldownMs);
        if (me.alive) recordHp(me.hp, me.maxHp);
        // Instant ability picks from the 20 Hz feed.
        if (me.abilityChoices?.length) void pickIfPending(me.heroClass, me.abilities, me.abilityChoices);
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

const ALIAS: Record<string, string[]> = {
    fortitude: ["fortitude", "defensive_aura", "ring_of_healing", "soul_harvest"],
    fury: ["fury", "earthquake"],
};
const idsFor = (id: string) => ALIAS[id] ?? [id];

function currentSkin(me: MeView | null): string | null {
    if (me && me.skin !== undefined) return me.skin ?? null;
    const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
    return skinGranted ? pref.skin ?? null : null;
}

function buildWants(heroClass: HeroClass, skin: string | null): [string, number][] {
    if (heroClass === "melee") {
        if (skin === "treant") {
            return [["fury", 4], ["divine_shield", 1], ["cleave", 1], ["fortitude", 4], ["thorns", 4]];
        }
        const physical = enemyPhysicalShare() >= 0.5;
        const base: [string, number][] = [
            ["fortitude", 1], ["cleave", 1], ["divine_shield", 1], ["fortitude", 4], ["thorns", 1],
        ];
        return physical ? [...base, ["thorns", 4], ["fury", 4]] : [...base, ["fury", 4], ["thorns", 2]];
    }
    if (heroClass === "mage") {
        // Rule (field-coached): Skeleton 1 is ALWAYS picked before Fortitude/Heal 1.
        if (skin === "pixagreen_mage") {
            // FB1 -> Nado1 -> Skel1 -> Heal1 -> max FB -> max Nado -> max Heal -> Skel
            return [
                ["fireball", 1],
                ["tornado", 1],
                ["raise_skeleton", 1],
                ["fortitude", 1],
                ["fireball", 4],
                ["tornado", 4],
                ["fortitude", 4],
                ["raise_skeleton", 4],
            ];
        }
        // Base mage: same early spread, Fortitude only after the core is online.
        return [["fireball", 1], ["tornado", 1], ["raise_skeleton", 1], ["fireball", 4], ["tornado", 4], ["fortitude", 4]];
    }
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

    const wants = buildWants(heroClass, currentSkin(meView()));
    for (const [id, target] of wants) {
        const off = offeredIdFor(id);
        if (off && lvlOf(id) < Math.min(target, MAX)) return off;
    }
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
    return typeof ds.cooldownRemaining === "number" && ds.cooldownRemaining <= 0;
}

function enemyHeroesNearMe(radius: number): number {
    const u = myUnit();
    if (!u || !W) return 0;
    const ef = enemyOf(myFaction!);
    return W.units.filter((x) => x.isHero && x.faction === ef && dist(x, u) <= radius).length;
}

function reflex() {
    if (!joinedConfirmed || !myFaction || !live()) return;
    const me = meView();
    if (!me || !me.alive) return;
    if (divineCovers(me)) return;

    const projected = incomingDps(CFG.dpsWindowLiveMs) * (CFG.predictLookaheadMs / 1000);
    const lethalNext = me.hp - projected <= me.maxHp * 0.02 && hpLostOver(1000) > 0;
    const floor = hpFracOf(me) <= CFG.recallFloorLive;
    if ((lethalNext || floor) && enemyHeroesNearMe(CFG.xpRange) >= 1) {
        void sendRecall(`fast: ${(hpFracOf(me) * 100) | 0}% hp, dps=${incomingDps(CFG.dpsWindowLiveMs) | 0}`);
    }
}

// ----------------------------- Unfair-fight lane utility -----------------------

interface LaneEval { l: LaneStat; score: number; urgency: number; bias: number; }

function evalLane(l: LaneStat, me: MeView, isCurrent: boolean): LaneEval {
    const adv = myAdvance(l.frontline);
    let s = 0;

    if (l.lane === "mid") s += CFG.midBias;
    if (isCurrent) s += CFG.stickiness;

    // Defense urgency: enemy heroes pushing our side; deeper = more urgent.
    let urgency = 0;
    if (l.enemyHeroesHere > 0 && adv < -20) {
        urgency = Math.min(3, (-adv - 20) / 25) * Math.min(l.enemyHeroesHere, 3);
        s += urgency;
    }

    // Fight winnability: ally hero levels (+me) + our-tower edge vs enemy levels.
    let towerEdge = 0;
    if (l.ownTowerAlive && adv < -20) towerEdge += CFG.towerEdge;      // fight near OUR tower
    if (l.enemyTowerAlive && adv > 20) towerEdge -= CFG.towerEdge;     // diving THEIR tower
    const bias = l.allyHeroLvls + me.level + towerEdge - l.enemyHeroLvls;
    if (l.enemyHeroesHere > 0) {
        if (bias >= CFG.unfairForUs) s += 3;                              // unfair for us: take it
        else if (bias <= CFG.unfairAgainstUs) s -= 5;                     // unfair against us: avoid
    }

    // Objectives: a live enemy tower is a goal; a towerless lane only pays near their base.
    if (l.enemyTowerAlive) {
        s += 1;
        if (l.enemyTowerHp < 400 && l.adv >= 0) s += 2.5;
    } else {
        s += adv >= 60 ? 1.5 : -1;
    }

    // Creep feasts: big enemy stacks = AOE farm, extra if deep on our side (hop & dispatch).
    if (l.enemy >= 8) s += 1.5 + (l.enemy >= 12 ? 1 : 0) + (adv < -40 ? 1 : 0);

    return { l, score: s, urgency, bias };
}

// Human-readable reason for choosing a lane (for spectate-audit logs).
function describe(e: LaneEval): string {
    const p: string[] = [];
    const adv = myAdvance(e.l.frontline);
    if (e.urgency > 0) p.push(`${e.l.enemyHeroesHere} enemy hero${e.l.enemyHeroesHere > 1 ? "es" : ""} pushing us (front ${adv | 0})`);
    if (e.l.enemyHeroesHere > 0 && e.bias >= CFG.unfairForUs) p.push(`unfair fight FOR us (+${e.bias})`);
    if (e.l.enemyTowerAlive && e.l.enemyTowerHp < 400 && e.l.adv >= 0) p.push(`their tower at ${e.l.enemyTowerHp | 0}hp`);
    if (e.l.enemy >= 8) p.push(`${e.l.enemy}-creep feast`);
    if (!p.length && e.l.lane === "mid") p.push("restack mid");
    if (!p.length && e.l.enemyTowerAlive) p.push("tower to push");
    return p.slice(0, 2).join(", ") || "better lane";
}

// Forward-only reachability: the target's fight point must not be behind us.
function reachable(target: LaneStat, me: MeView, here: LaneStat): boolean {
    if (!me.alive) return true;
    const u = myUnit();
    const base = myBase();
    if (u && base && dist(u, base) < 600) return true; // near home, every lane opens up
    return myAdvance(target.frontline) >= myAdv(here) - CFG.reachSlack;
}

// A lane switch drops us at the target's frontline. Landing in the enemy tower's
// shadow scales with OUR level (coached anchors: lvl<=4 needs a fat wave and no
// hero; lvl 8 needs ~4 creeps with uninterrupted flow; lvl 14 can walk at a tower
// alone as long as no enemy hero is inbound on that lane).
function landingSafe(l: LaneStat, me: MeView): boolean {
    const adv = myAdvance(l.frontline);
    const towerZone = l.enemyTowerAlive && adv > 35;
    if (!towerZone) {
        // Only hard ban: landing utterly alone into their wave on their side.
        return !(l.friendly === 0 && l.enemy >= 4 && adv > 0);
    }
    if (me.level <= 4) return l.friendly >= 8 && l.enemyHeroesHere === 0;
    if (me.level <= 7) return l.friendly >= 6 && l.enemyHeroesHere === 0;
    if (me.level <= 13) return l.friendly >= 4 && l.adv >= 0; // flow uninterrupted
    return l.enemyHeroesHere === 0 || l.friendly >= 4;        // 14+: solo ok if no hero inbound
}

// ----------------------------- Mage kiting -------------------------------------
// Physical attackers (melee/ranged) must stand still to deal damage; a moving
// mage keeps full spell output. Human tactic: tap a vertical lane change (top if
// in mid/bot, bot if in mid/top) for as long as melee/ranged are close — unless
// the foe is nearly dead, in which case stand still and secure the kill.

const LANE_Y: Record<Lane, number> = { top: 420, mid: 1200, bot: 1980 };

// Tower-juke step direction (coached): top tower -> tap bot; bot -> tap top; mid -> tap top.
const jukeDirFor = (lane: Lane): Lane => (lane === "top" ? "bot" : "top");

async function mageKite(me: MeView, lanes: LaneStat[]): Promise<"stand" | "kited" | "wait"> {
    const u = myUnit();
    if (!u || !W) return "stand";

    // No spells yet (early levels) -> kiting is pointless; stand and auto-attack.
    const hasSpell = me.abilities.some((a) => (a.id === "fireball" || a.id === "tornado") && a.level >= 1);
    if (!hasSpell) return "stand";

    // Condition 1: a melee/ranged hero is IN ATTACK REACH of us — actually fighting
    // us, not poking creeps from across the wave.
    const ef = enemyOf(myFaction!);
    const attackers = W.units.filter((x) => {
        if (!x.isHero || x.faction !== ef) return false;
        const r = roster().find((h) => h.name === x.ownerName);
        if (!r || r.heroClass === "mage") return false;
        const reach = r.heroClass === "melee" ? CFG.kiteMeleeReach : CFG.kiteRangedReach;
        return dist(x, u) <= reach;
    });
    if (!attackers.length) return "stand";

    // Condition 2: they're actively damaging US. A foe busy with creeps, tanked by
    // an ally — or held by our cat charm — isn't, and then we stand and burst them.
    if (hpLostOver(1200) <= 0) return "stand";

    // Nearly-dead attacker below our own fraction -> stand and finish them.
    const weakest = [...attackers].sort((a, b) => hpFracOf(a) - hpFracOf(b))[0];
    if (hpFracOf(weakest) < CFG.kiteSecureFoeFrac && hpFracOf(weakest) < hpFracOf(me)) return "stand";

    // Condition 4: our HP significantly higher than theirs -> stand and win the trade.
    const strongest = [...attackers].sort((a, b) => hpFracOf(b) - hpFracOf(a))[0];
    if (hpFracOf(me) >= hpFracOf(strongest) + CFG.kiteHpEdge) return "stand";

    if (!kiting) { kiting = true; kiteReturnLane = serverLane ?? currentLaneTarget; }
    if (now() - lastKiteAt < CFG.kiteGapMs) return "wait";

    // PAST THE ENEMY TOWERS the pathing warps: a vertical tap takes a couple of
    // steps then walks FORWARD (into buildings). Kite differently there:
    //   top/bot -> bob our OWN lane (like the base dance); mid -> one juke, no spam.
    const myAdvNow = myAdvance(humanAdvOfX(u.x));
    if (myAdvNow > CFG.deepAdvance) {
        const cur = serverLane ?? currentLaneTarget;
        if (cur === "mid") {
            if (now() - lastJukeAt > CFG.jukeCdMs) {
                lastJukeAt = now();
                jukeHomeLane = cur;
                jukeRestoreAt = now() + CFG.jukeStepMs;
                await commandLane(jukeDirFor(cur), "deep-mid juke (single step)", { emergency: true });
                return "kited";
            }
            return "stand"; // juke spent: hold and trade rather than warp forward
        }
        lastKiteAt = now();
        await commandLane(cur, `deep kite ${cur} (same-lane bob)`, { emergency: true, allowRepeat: true });
        return "kited";
    }

    // Keep ONE direction for a few seconds (zig-zagging eats extra hits). Flip when
    // the hold expires or we've arrived at that corridor's y.
    const threat = [...attackers].sort((a, b) => dist(a, u) - dist(b, u))[0];
    const arrived = kiteDir && Math.abs(u.y - LANE_Y[kiteDir]) < 160;
    if (!kiteDir || arrived || now() > kiteDirUntil) {
        kiteDir = pickKiteDir(u, threat, lanes);
        kiteDirUntil = now() + CFG.kiteDirHoldMs;
    }
    lastKiteAt = now();
    await commandLane(kiteDir, `kite ${kiteDir}`, { emergency: true, allowRepeat: true });
    return "kited";
}

// Choose the vertical step direction: away from the threat, never off the map
// edge — and never toward a lane whose frontline sits at/past their tower, because
// our own creeps being beyond the tower warps the tap's pathing INTO tower range.
function pickKiteDir(u: U, threat: U, lanes: LaneStat[]): Lane {
    const cur = serverLane ?? currentLaneTarget;
    let cands = LANES.filter((l) => l !== cur);
    if (u.y < 620) cands = cands.filter((l) => LANE_Y[l] > u.y + 150);   // top edge: only down
    if (u.y > 1780) cands = cands.filter((l) => LANE_Y[l] < u.y - 150);  // bottom edge: only up
    const notDeep = cands.filter((l) => {
        const s = lanes.find((x) => x.lane === l);
        return !s || myAdvance(s.frontline) <= 50;
    });
    const pool = notDeep.length ? notDeep : cands.length ? cands : LANES.filter((l) => l !== cur);
    return [...pool].sort((a, b) => Math.abs(LANE_Y[b] - threat.y) - Math.abs(LANE_Y[a] - threat.y))[0]
        ?? (cur === "top" ? "bot" : "top");
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
    trackBaseHp();

    const evals = lanes.map((l) => evalLane(l, me, l.lane === effLane));
    const hereEval = evals.find((e) => e.l.lane === effLane)!;

    if (DEBUG && now() - lastDbgAt > 5000) {
        lastDbgAt = now();
        dbg(evals.map((e) => `${e.l.lane}:${e.score.toFixed(1)}(u${e.urgency.toFixed(1)},b${e.bias})`).join(" ") +
            ` | me@${effLane} adv=${myAdv(here) | 0} hp=${(hpFracOf(me) * 100) | 0}%${live() ? " LIVE" : " REST"}`);
    }

    // Dead: pre-position for the best lane (reachability waived — we respawn at base).
    if (!me.alive) {
        kiting = false; kiteDir = null; kiteReturnLane = null; jukeHomeLane = null;
        const best = [...evals].sort((a, b) => b.score - a.score)[0];
        if (best && best.l.lane !== effLane) await commandLane(best.l.lane, `pre-set respawn (${best.score.toFixed(1)})`, { emergency: true });
        return;
    }

    // Channeling recall: we're invulnerable and committed. Issue NOTHING — a lane
    // command now would redirect the teleport, and "escape" makes no sense mid-channel.
    // (The deliberate defend-landing steer runs outside the macro and still works.)
    if (channelingRecall()) { dbg("channeling recall — silent"); return; }

    // Mid-juke window: we tapped perpendicular at a tower. Stay silent for the step,
    // then restore our real lane assignment so all lane logic stays coherent.
    if (jukeHomeLane) {
        if (now() < jukeRestoreAt) return;
        const back = jukeHomeLane;
        jukeHomeLane = null;
        if ((serverLane ?? currentLaneTarget) !== back) {
            await commandLane(back, "juke re-aggro", { emergency: true, allowRepeat: true });
            return;
        }
    }

    // 1) Slow-path recall (REST mode; fast path is reflex()).
    if (!live() && ready("recall")) {
        const recentDrop = hpLostOver(2000);
        const dieNextPoll = me.hp - recentDrop * 1.4 <= 0;
        if (here.enemyHeroesHere >= 1 && hpFracOf(me) <= CFG.recallFloorRest && (recentDrop > 0 || dieNextPoll)) {
            await sendRecall(`slow: ${(hpFracOf(me) * 100) | 0}% hp, dropped ${recentDrop | 0}`);
            return;
        }
    }

    // 2) Escape when dying with recall down: committed forward peel.
    {
        const dyingFast = live()
            ? me.hp - incomingDps(CFG.dpsWindowLiveMs) * (CFG.predictLookaheadMs / 1000) <= me.maxHp * 0.02 && hpLostOver(1000) > 0
            : hpFracOf(me) <= CFG.recallFloorRest && hpLostOver(2000) > 0;
        const threatened = live() ? enemyHeroesNearMe(CFG.xpRange) >= 1 : here.enemyHeroesHere >= 1;
        if (dyingFast && threatened && !ready("recall") && !divineCovers(me)) {
            const rl = lanes.filter((l) => l.lane !== effLane && l.ownTowerAlive && l.adv >= 0).sort((a, b) => b.adv - a.adv)[0];
            if (rl) { await commandLane(rl.lane, "!escape (recall down)", { sprint: true, emergency: true, allowRepeat: true }); return; }
        }
    }

    // 3) RECALL-DEFEND: heroes + creeps physically AT our base, and we're not racing.
    if (ready("recall")) {
        let defendLane: Lane | null = null;
        if (live() && myBase()) {
            const base = myBase()!;
            const ef = enemyOf(myFaction);
            const nearBase = W!.units.filter((x) => x.faction === ef && dist(x, base) <= CFG.baseDefendRadius);
            const heroesAtBase = nearBase.filter((x) => x.isHero);
            if (heroesAtBase.length >= 1 && nearBase.length - heroesAtBase.length >= CFG.defendCreepCount) {
                // The real threat lane = where the attacking MASS is, not a hero's stale
                // lane assignment. Majority vote among all attackers; geometric tiebreak.
                const byLane = new Map<Lane, number>();
                for (const x of nearBase) byLane.set(x.lane, (byLane.get(x.lane) ?? 0) + 1);
                defendLane = [...byLane.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;
                if (!defendLane) {
                    const meanY = nearBase.reduce((s, x) => s + x.y, 0) / nearBase.length;
                    defendLane = meanY < 900 ? "top" : meanY > 1500 ? "bot" : "mid";
                }
            }
        } else {
            const atDoor = lanes.filter((l) => l.enemyHeroesHere >= 1 && myAdvance(l.frontline) <= -85 && l.enemy >= CFG.defendCreepCount);
            if (atDoor.length) defendLane = atDoor.sort((a, b) => b.enemyHeroesHere - a.enemyHeroesHere)[0].lane;
        }
        if (defendLane) {
            // Already at/near our base (e.g. fresh respawn)? Recalling is nonsense —
            // just make sure we're on the besieged lane; base auto-defense engages.
            const u = myUnit();
            const base = myBase();
            const alreadyHome = u && base ? dist(u, base) <= 700 : myAdv(here) <= -70;
            if (alreadyHome) {
                await commandLane(defendLane, "!defend (already home)", { emergency: true });
                return;
            }
            const racing = myAdv(here) >= CFG.atEnemyBaseAdv;
            const finishingTower = here.enemyTowerAlive && here.enemyTowerHp <= CFG.towerFinishHp && here.adv >= 0;
            // Confident siege (lvl 8+, creeps with us, at their tower, nobody contesting):
            // taking that tower outweighs a base SCARE. Only break off once enemy heroes
            // are ALREADY damaging the base — not merely 10 seconds away from it.
            const confidentSiege =
                me.level >= 8 && here.enemyTowerAlive && myAdv(here) > 40 && here.friendly >= 4 &&
                (live() ? enemyHeroesNearMe(420) === 0 : here.enemyHeroesHere === 0);
            if (confidentSiege && !baseTakingDamage()) {
                say(`base threatened but siege is confident (${here.enemyTowerHp | 0}hp tower) — pressing on`);
            } else if (!racing && !finishingTower) {
                const target = defendLane;
                if (await sendRecall(`defend base (${target})`)) {
                    setTimeout(() => { void commandLane(target, "!defend landing", { emergency: true }); }, 600);
                }
                return;
            }
            dbg(`base threatened but racing=${racing} finishing=${finishingTower} — holding forward`);
        }
    }

    // 4) FIGHT-LOCK / KITE: an enemy hero is on us and we're not dying.
    //    Warrior: hold and swing. Mage: sustained vertical stepping.
    const engaged = live()
        ? enemyHeroesNearMe(CFG.fightLockRadius) >= 1
        : here.enemyHeroesHere >= 1 && hpLostOver(CFG.combatWindowMs) > 0;
    if (engaged) {
        if (me.heroClass === "mage" && live()) {
            const k = await mageKite(me, lanes);
            if (k !== "stand") return; // kited or waiting for the next step
        }
        say(`fight-lock in ${effLane}`);
        return;
    }
    // Threat gone: snap back to the lane we were holding before the kite dance.
    if (kiting) {
        kiting = false;
        kiteDir = null;
        const back = kiteReturnLane;
        kiteReturnLane = null;
        if (back && back !== effLane) { await commandLane(back, "kite return", { emergency: true }); return; }
    }

    // 5a) BASE SIEGE: we're at their nexus — hold it, and dance the base arrow.
    //     The dance: spam OUR lane's tap; the vertical bob breaks arrow range for a
    //     beat while creeps catch aggro. Stop once allies have the aggro — unless
    //     three more arrows would kill us, then keep dancing regardless.
    {
        const eb = enemyBase();
        const u = myUnit();
        if (live() && eb && u && dist(u, eb) <= CFG.baseDanceReach) {
            const suddenDeath = (rest?.tick ?? 0) > 18_000; // 15 min in: nexus stops shooting
            const bleeding = biggestHitWithin(1000) >= CFG.jukeHitHp; // the nexus arrow (60), not creep chip
            const wouldDie3 = me.hp <= CFG.baseArrowDmg * 3 + 10;
            const closerAllies = W!.units.filter(
                (x) => x.faction === myFaction && x.id !== u.id && dist(x, eb) < dist(u, eb) - 25
            ).length;
            const dance = !suddenDeath && bleeding && (closerAllies < 2 || wouldDie3);
            if (dance) {
                await commandLane(effLane, "base dance (dodge nexus arrow)", { emergency: true, allowRepeat: true });
                return;
            }
            say(`hitting enemy base${suddenDeath ? " (sudden death)" : ""} — holding`);
            return;
        }
    }

    // 5b) SIEGE-HOLD: hitting a tower uncontested is a primary objective. Never
    //     wander off it for a merely "better-scored" lane. The moment it hits us,
    //     juke ONCE perpendicular so creeps inherit the aggro, then re-aggro.
    const advHere = myAdv(here);
    const uncontestedHere = live() ? enemyHeroesNearMe(420) === 0 : here.enemyHeroesHere === 0;
    if (here.enemyTowerAlive && advHere > 40 && uncontestedHere) {
        const divineTank = me.heroClass === "melee" && divineCovers(me); // shield up: facetank instead
        if (
            live() && biggestHitWithin(900) >= CFG.jukeHitHp && !divineTank &&
            here.friendly >= CFG.jukeMinCreeps && now() - lastJukeAt > CFG.jukeCdMs
        ) {
            lastJukeAt = now();
            jukeHomeLane = effLane;
            jukeRestoreAt = now() + CFG.jukeStepMs;
            await commandLane(jukeDirFor(effLane), "tower juke (creeps take aggro)", { emergency: true });
            return;
        }
        say(`sieging ${effLane} tower (${here.enemyTowerHp | 0}hp) — holding`);
        return;
    }

    // 6) TOWER-DEFEND: our tower is under enemy-hero attack and we can still get
    //    there moving forward — always go.
    {
        const cand = lanes
            .filter((l) => l.ownTowerAlive && l.enemyHeroesHere >= 1 && myAdvance(l.frontline) <= -40)
            .sort((a, b) => b.enemyHeroesHere - a.enemyHeroesHere)[0];
        if (cand) {
            if (cand.lane === effLane) {
                say(`defending ${effLane} tower (${cand.enemyHeroesHere} heroes on it)`);
                return;
            }
            if (reachable(cand, me, here)) {
                await commandLane(cand.lane, `defend ${cand.lane} tower (${cand.enemyHeroesHere} heroes on it)`, { sprint: true });
                return;
            }
            dbg(`${cand.lane} tower sieged but unreachable (behind us)`);
        }
    }

    // 7) Unfair-fight utility: go where the best-biased fight / objective / feast is.
    //    Switching has a real cost (we stop pushing), so it must clearly pay AND land safely.
    const best = evals
        .filter((e) => e.l.lane !== effLane && reachable(e.l, me, here) && landingSafe(e.l, me))
        .sort((a, b) => b.score - a.score)[0];
    if (best && best.score > hereEval.score + CFG.switchMargin) {
        const sprint = best.urgency > 0.5 || best.score - hereEval.score > 3;
        await commandLane(best.l.lane, `${describe(best)} [${best.score.toFixed(1)} vs ${hereEval.score.toFixed(1)}]`, { sprint });
    }
}

// ----------------------------- Heartbeat & boot ---------------------------------

setInterval(() => {
    if (!joinedConfirmed) return;
    const me = meView();
    if (!me) return;
    const mode = live() ? "LIVE(20Hz+pos)" : "REST(1.5s)";
    console.log(
        `[status] mode=${mode} class=${me.heroClass}${currentSkin(me) ? `/${currentSkin(me)}` : ""} lane=${serverLane ?? "?"} lvl=${me.level} hp=${(hpFracOf(me) * 100) | 0}%` +
        ` recall=${ready("recall") ? "ready" : Math.ceil((cd.recall - now()) / 1000) + "s"} wsFrames=${wsFrames}`
    );
}, 30_000);

// ----------------------------- Discovery + boot --------------------------------

// Find a REST base + state path that returns a game-state-shaped JSON, and a
// working WS url. Falls back to overrides (TW_BASE/TW_WS) without probing.
async function discover(): Promise<boolean> {
    // If a room isn't forced, we still try each base's state with no room (server
    // may return a default/lobby); once deployed the state reflects our room.
    for (const base of REST_CANDIDATES) {
        for (const path of STATE_PATHS) {
            for (const q of ROOM !== null ? [`?room=${ROOM}`] : ["", "?room=1"]) {
                try {
                    const r = await fetch(`${base}${path}${q}`, { headers: COOKIE ? { Cookie: COOKIE } : {} });
                    captureCookie(r);
                    if (!r.ok) continue;
                    const j: any = await r.json().catch(() => null);
                    if (j && (Array.isArray(j.heroes) || Array.isArray(j.units) || j.lanes)) {
                        REST_BASE = base; STATE_PATH = path;
                        if (ROOM === null && /room=(\d+)/.test(q)) ROOM = parseInt(RegExp.$1, 10);
                        console.log(`[discover] state: ${REST_BASE}${STATE_PATH}${q}`);
                        // Pick a deploy path: try each with a harmless probe (expect !404).
                        for (const dp of DEPLOY_PATHS) {
                            try {
                                const pr = await fetch(`${REST_BASE}${dp}`, {
                                    method: "POST", headers: { "Content-Type": "application/json", ...(COOKIE ? { Cookie: COOKIE } : {}) },
                                    body: JSON.stringify({ ping: true }),
                                });
                                captureCookie(pr);
                                if (pr.status !== 404) { DEPLOY_PATH = dp; break; }
                            } catch { }
                        }
                        console.log(`[discover] deploy: ${REST_BASE}${DEPLOY_PATH}`);
                        // WS: use override, else derive from base host, else probe candidates.
                        WS_URL = process.env.TW_WS || base.replace(/^http/, "ws");
                        console.log(`[discover] ws: ${WS_URL} (will fall back through candidates if silent)`);
                        if (ROOM === null) ROOM = FORCED_ROOM ?? 1;
                        return true;
                    }
                } catch { /* next */ }
            }
        }
    }
    // Overrides provided but probing failed to confirm — trust them anyway.
    if (process.env.TW_BASE) {
        REST_BASE = process.env.TW_BASE; WS_URL = process.env.TW_WS || REST_BASE.replace(/^http/, "ws");
        if (ROOM === null) ROOM = FORCED_ROOM ?? 1;
        console.log(`[discover] probing failed; using overrides ${REST_BASE} / ${WS_URL}`);
        return true;
    }
    return false;
}

async function main() {
    console.log(`[boot] thronebot #${CHILD_ID}${AGENT_NAME ? ` "${AGENT_NAME}"` : ""} | room ${ROOM ?? "(discover)"} | ${DEF_CLASS}${DEF_SKIN ? "/" + DEF_SKIN : ""} @ ${DEF_LANE} | item ${DEF_ITEM} | token ${process.env.TW_AUTH ? "set" : "MISSING"}${DEBUG ? " | DEBUG" : ""}`);
    const ok = await discover();
    if (!ok) {
        console.error("[boot] could not discover a Throne Wars API endpoint. Set TW_BASE (and TW_WS) explicitly — open thronewars.gg devtools → Network to find the host, e.g. TW_BASE=https://thronewars.gg");
        process.exit(1);
    }
    wsConnect();
    // Every bot must poll: pollRest is where THIS bot deploys itself, confirms its
    // own identity, and picks its own abilities. (Sharing one poll across children
    // is why only bot #1 ever joined — the others never deployed.) The poll is light;
    // the 20 Hz WS feed remains the primary world model for decisions.
    setInterval(pollRest, CFG.restPollMs);
    setInterval(() => {
        if (macroBusy) return;
        macroBusy = true;
        macro().catch((e) => console.log("[macro] error:", (e as Error).message)).finally(() => { macroBusy = false; });
    }, CFG.macroMs);
    void pollRest();
}

// Entrypoint dispatch — runs LAST, so every top-level const above is initialized.
// Parent (multi-instance supervisor) already spawned children and does nothing here.
if (!_IS_PARENT) void main();