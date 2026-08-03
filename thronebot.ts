/**
 * Throne Wars bot (thronebot.ts) — fork of our Defense-of-the-Agents bot v0.6
 * ==========================================================================
 * Same engine, same strategy brain (unfair-fight lane utility, mage kiting,
 * tower juke, base dance, recall doctrine — all inherited unchanged). Only the
 * NETWORKING differs, because Throne Wars (thronewars.gg) is web2 with NO login:
 *
 * - Identity is SERVER-ASSIGNED (e.g. "SilentDrake137"). There is no API key.
 * We create a session on first contact, keep its cookie, and read back our
 * assigned name from the game state (whoever appears that we didn't already
 * know = us, confirmed by matching our chosen lane/class).
 * - Endpoints are AUTO-DISCOVERED at boot: the player-facing docs don't list
 * them, so we probe a set of candidate REST bases / paths / WS hosts (all
 * built from the same dev's DoA shapes) and lock onto whatever answers.
 * - Rooms use ?room=N. For an 18-bot party, force one room so they fill it.
 *
 * Because the API is unverified (alpha, no bot docs), the discovery layer logs
 * exactly what it found; if a probe path is wrong, the log tells us what to fix.
 *
 * ENV (all optional):
 * TW_BASE         override REST base (skip discovery), e.g. https://thronewars.gg
 * TW_WS           override WS url,   e.g. wss://thronewars.gg
 * TW_ROOM         force a room number (all instances -> same room = a party)
 * TW_NAME         desired name hint (server may override / ignore)
 * TW_CLASS        mage | melee | ranged   (default mage)
 * TW_SKIN         skin id (default: none — base mage)
 * TW_ITEM         item id (default: ring_of_regen; free for everyone here)
 * TW_LANE         top | mid | bot  (default mid)
 * TW_INSTANCES    launch N in-process bots from ONE command (default 1)
 * TW_JOIN_STAGGER ms between staggered instance joins (default 1500)
 * DEBUG=1 / --debug
 *
 * Run one:   npx tsx thronebot.ts
 * Run a party of 18 (this file re-launches itself as 18 child processes):
 * TW_INSTANCES=18 TW_ROOM=1 npx tsx thronebot.ts
 * (Each child is its own OS process = its own session/name, exactly how the game
 * sees 18 real players. You can also launch 18 terminals by hand with same TW_ROOM.)
 */

import "dotenv/config";
import WebSocket from "ws";
import zlib from "node:zlib";
import fs from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";

// ESM has no __filename. We used to derive it from import.meta.url, but that
// forces a "module": "es2020"+ tsconfig on anyone who opens this file, and the
// only thing SELF_PATH is for is re-spawning this script as child processes in
// party mode. argv[1] is exactly that path, and needs no compiler options.
const SELF_PATH = process.argv[1] ? path.resolve(process.argv[1]) : "thronebot.ts";

const _INSTANCES = Math.max(1, parseInt(process.env.TW_INSTANCES ?? "1", 10) || 1);
const _STAGGER = parseInt(process.env.TW_JOIN_STAGGER ?? "1500", 10) || 1500;
const _IS_PARENT = _INSTANCES > 1 && !process.env.TW_CHILD;

// ----------------------------- Config ----------------------------------------

const CHILD_ID = process.env.TW_CHILD ?? "1";
const DEBUG = process.env.DEBUG === "1" || process.env.DOTA_DEBUG === "1" || process.argv.includes("--debug");

// --- FIX 1: credential resolution -------------------------------------------
// The old build only read TW_AUTH / TW_NAME, which the *parent* sets for its
// children out of TW_AUTH_<n>. Running a single bot (the normal case) therefore
// went out with NO Authorization header and an EMPTY name: the server happily
// created an anonymous session and spawned a hero under a name we never learned,
// so we could never find ourselves in the state and re-deployed forever.
// Resolve here, once, with fallbacks, and use these everywhere.
const AUTH_TOKEN = (
    process.env.TW_AUTH ||
    process.env[`TW_AUTH_${CHILD_ID}`] ||
    process.env.TW_AUTH_1 ||
    ""
).trim();
// A raw UUID is accepted too — we add the "Bearer " prefix if it's missing.
const AUTH_HEADER = AUTH_TOKEN
    ? (/^bearer\s/i.test(AUTH_TOKEN) ? AUTH_TOKEN : `Bearer ${AUTH_TOKEN}`)
    : "";
const NAME_HINT = (
    process.env.TW_NAME ||
    process.env[`TW_NAME_${CHILD_ID}`] ||
    process.env.TW_NAME_1 ||
    ""
).trim();

// Headers used on every REST call (and the WS handshake).
function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
    const h: Record<string, string> = { ...extra };
    if (AUTH_HEADER) h["Authorization"] = AUTH_HEADER;
    if (COOKIE) h["Cookie"] = COOKIE;
    return h;
}

// Candidate hosts/paths probed at boot (same-dev DoA shapes are the template).
const REST_CANDIDATES = [process.env.TW_BASE, "https://thronewars.gg", "https://api.thronewars.gg", "https://game.thronewars.gg", "https://server.thronewars.gg"].filter(Boolean) as string[];
const WS_CANDIDATES = [process.env.TW_WS, "wss://thronewars.gg", "wss://api.thronewars.gg", "wss://game.thronewars.gg", "wss://server.thronewars.gg"].filter(Boolean) as string[];
const STATE_PATHS = ["/api/game/state", "/api/state", "/api/room/state", "/state"];
const DEPLOY_PATHS = ["/api/strategy/deployment", "/api/deployment", "/api/deploy", "/api/play"];

// ==============================================================================
//  LOADOUT — THE SOURCE OF TRUTH. Edit it here, in the file.
//  .env holds secrets only (TW_AUTH_n / TW_NAME_n). Nothing below needs env.
// ==============================================================================
const LOADOUT = {
    heroClass: "mage" as HeroClass,     // melee | ranged | mage
    lane: "mid" as Lane,                // top | mid | bot — the lane we stack

    // Skin. The ability build adapts to it automatically (see buildWants).
    // Throne of Bones turns Fury into Skeleton Archers; free for everyone.
    // Set to null for the plain class sprite.
    //
    // Skin IDS ARE NOT DOCUMENTED anywhere, so this is a list of spellings to
    // try, best guess first. If one is silently ignored by the server the bot
    // notices at join time and moves to the next on the following round, so a
    // wrong guess costs one round, not the session.
    skin: ["throne_of_bones", "throneofbones", "throne-of-bones", "bones"] as string[] | null,

    // Item, in preference order. Cat Ears is the strongest thing available (2s
    // full lockout on the nearest enemy hero, every 20s) but costs 1,600 Silver;
    // Ring of Regen is free, so it's the floor. The deploy handler walks this
    // list on an item rejection, and re-tries from the top every round — so the
    // bot picks up Cat Ears by itself the round after you buy it.
    items: ["cat_ears", "ring_of_regen", null] as (string | null)[],
};

// Per-class skin defaults, used only if LOADOUT.skin is left null but you still
// want the free skin for whatever class you're running.
//   melee -> Vanguard (Defensive Aura), ranged -> Centaur (Bramble Patch)

const FORCED_ROOM = process.env.TW_ROOM ? parseInt(process.env.TW_ROOM, 10) : null;
// In party mode the parent tells us which room its shared feed is watching.
const PARTY_ROOM = process.env.TW_PARTY_ROOM ? parseInt(process.env.TW_PARTY_ROOM, 10) : FORCED_ROOM;
const DEF_CLASS = LOADOUT.heroClass;
const SKIN_CANDIDATES: string[] = LOADOUT.skin ?? [];
const DEF_SKIN: string | undefined = SKIN_CANDIDATES[0];
const DEF_ITEM = LOADOUT.items[0] ?? null;
// The JSON field the server wants the skin under isn't documented either. If the
// [join] loadout line shows the skin never applying under any id above, this is
// the other knob to turn: "heroSkin", "skinId", "cosmetic".
const SKIN_KEY = "skin";
const DEF_LANE = LOADOUT.lane;

// Discovered/session state (filled by discover()).
let REST_BASE = process.env.TW_BASE || "";
let WS_URL = process.env.TW_WS || "";
let STATE_PATH = process.env.TW_STATE_PATH || "/api/game/state";
let DEPLOY_PATH = process.env.TW_DEPLOY_PATH || "/api/strategy/deployment";
let COOKIE = "";                        // session cookie jar for this process
let AGENT_NAME = NAME_HINT;             // learned from state if server-assigned
let nameLearned = false;                // true once we've *seen* AGENT_NAME in a state
let ROOM: number | null = FORCED_ROOM;
const GAME_ID = 0; // unused in TW (room-based); kept so shared code compiles

interface DeployPref { heroClass: HeroClass; skin?: string; label: string; }
const SKIN_PREFS: DeployPref[] = SKIN_CANDIDATES.map((sk) => ({ heroClass: DEF_CLASS, skin: sk, label: `${DEF_CLASS} ${sk}` }));
const FARCASTER: DeployPref = { heroClass: DEF_CLASS, label: `${DEF_CLASS}` };
const BASE_PREF: DeployPref = { heroClass: DEF_CLASS, label: `base ${DEF_CLASS}` };
// Throne Wars: everything is Silver-unlocked (no wallet). Try the chosen skin (if
// any) first, then fall back to the base class. Cat Ears/Ring both free here.
const DEPLOY_PREFS: DeployPref[] = [...SKIN_PREFS, BASE_PREF];

const CFG = {
    itemPreference: [...LOADOUT.items] as (string | null)[],
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
    spawnStrollWindowMs: 12_000,   // how long after entering we'll still bother
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

// FIX 5: the scoreboard used to be read at hard-coded array offsets (name=0,
// level=4, abilities=13, abilityChoices=19...). Any field the dev inserts shifts
// everything after it, and the first casualty is abilityChoices — which is
// exactly "the bot stopped picking skills". We now LOCATE the structural fields
// by shape on the first frame and log what we found.
interface SbLayout { name: number; abilities: number; choices: number | null; detected: boolean; }
let SB_LAYOUT: SbLayout | null = null;

const looksLikeAbility = (v: any) =>
    (Array.isArray(v) && typeof v[0] === "string" && typeof v[1] === "number") ||
    (v && typeof v === "object" && typeof v.id === "string" && typeof v.level === "number");

function detectScoreLayout(raw: any[]): SbLayout {
    const l: SbLayout = { name: 0, abilities: 13, choices: 19, detected: false };
    const nameIdx = raw.findIndex((v) => typeof v === "string");
    const abilIdx = raw.findIndex((v) => Array.isArray(v) && v.length > 0 && v.every(looksLikeAbility));
    // Ability choices: an array of plain strings (ability ids) that isn't the ability list.
    const choiceIdx = raw.findIndex((v, i) => i !== abilIdx && Array.isArray(v) && v.length > 0 && v.every((x) => typeof x === "string"));
    if (nameIdx >= 0) l.name = nameIdx;
    if (abilIdx >= 0) l.abilities = abilIdx;
    l.choices = choiceIdx >= 0 ? choiceIdx : null;
    l.detected = nameIdx >= 0;
    console.log(`[schema] scoreboard layout: name=${l.name} abilities=${l.abilities} choices=${l.choices ?? "none-in-this-frame"}` +
        `${l.name !== 0 || l.abilities !== 13 ? "  <-- DRIFTED from the old build's offsets" : ""}`);
    return l;
}

function adaptScore(raw: any): ScoreEntry | null {
    if (Array.isArray(raw)) {
        if (!SB_LAYOUT) SB_LAYOUT = detectScoreLayout(raw);
        const L = SB_LAYOUT;
        // Both of these arrays are EMPTY early in a round (no abilities learned, no
        // offer pending), so a single detection pass on the first frame can lock in
        // the wrong index. Re-locate them per entry, per frame; the cached layout is
        // only a fallback.
        const abilIdx = raw.findIndex((v: any) => Array.isArray(v) && v.length > 0 && v.every(looksLikeAbility));
        const abilities = abilIdx >= 0 ? raw[abilIdx] : (Array.isArray(raw[L.abilities]) ? raw[L.abilities] : []);
        if (abilIdx >= 0 && abilIdx !== L.abilities) {
            console.log(`[schema] abilities live at index ${abilIdx} (old build assumed ${L.abilities}) — corrected`);
            L.abilities = abilIdx;
        }
        const choiceIdx = raw.findIndex((v: any, i: number) =>
            i !== abilIdx && Array.isArray(v) && v.length > 0 && v.every((x: any) => typeof x === "string"));
        return {
            name: raw[L.name], faction: FACTIONS[raw[1]] ?? "human",
            heroClass: CLASSES[raw[2]] ?? "melee", lane: LANES[raw[3]] ?? "mid",
            level: raw[4], hp: raw[7], maxHp: raw[8],
            alive: raw[10] === 1 || raw[10] === true,
            respawnTimer: typeof raw[11] === "number" && raw[11] >= 0 ? raw[11] : undefined,
            abilities: abilities.map(adaptAbility),
            recallCooldownMs: typeof raw[16] === "number" && raw[16] > 0 ? raw[16] : 0,
            abilityChoices: choiceIdx >= 0 ? raw[choiceIdx] : undefined,
            skin: raw[23] ?? null,
        };
    }
    if (raw && typeof raw === "object" && "name" in raw) {
        return {
            ...raw,
            heroClass: raw.heroClass ?? raw.class,
            abilities: (raw.abilities ?? []).map(adaptAbility),
            abilityChoices: raw.abilityChoices ?? raw.choices ?? raw.pendingAbilities,
        } as ScoreEntry;
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
let lastRawSb: any[] = [];   // undecoded scoreboard, for schema-proof lookups

let rest: RestState | null = null;
let restAt = 0;              // FIX 8: when `rest` was last refreshed
let restPolling = false;
// A REST snapshot older than this is a lie, not a fallback. Without this the
// bot re-confirmed itself off the boot-time snapshot for the whole session.
const restFresh = () => !!rest && now() - restAt < Math.max(6_000, CFG.restPollMs * 4);
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
// Party mode: children normally consume the parent's single WS/REST feed over
// IPC instead of opening their own. That only works while every bot is in the
// SAME room — if quick-join scatters us, a child has to go self-sufficient.
let ORACLE = !!process.env.TW_ORACLE;
function defectFromOracle(reason: string) {
    if (!ORACLE) return;
    ORACLE = false;
    console.log(`[party] leaving the shared feed: ${reason}. Opening our own socket.`);
    wsConnect();
}

let prefIdx = 0;
// prefIdx is reset to this every round (FIX 7). It only moves when we LEARN
// something durable — e.g. that a skin id is silently ignored by the server.
let prefFloor = 0;

// Find our own row in the raw (undecoded) scoreboard. Works whether entries are
// positional arrays or objects, and doesn't care which index anything lives at.
function rawSelfEntry(): any | null {
    if (!AGENT_NAME) return null;
    return lastRawSb.find((e: any) =>
        Array.isArray(e) ? e.includes(AGENT_NAME) : e?.name === AGENT_NAME) ?? null;
}

// Did the skin we asked for actually apply? Rather than trust a positional index
// that may have drifted, just look for the id anywhere in our raw row.
// null = can't tell (no row yet), true/false = definitive.
function skinApplied(want: string): boolean | null {
    const raw = rawSelfEntry();
    if (!raw) return null;
    const norm = (x: string) => x.toLowerCase().replace(/[^a-z0-9]/g, "");
    return norm(JSON.stringify(raw)).includes(norm(want));
}
let skinGranted = true;
let roundOverAt = 0;

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
    if (live() && !sbScalarsSuspect) {
        // The 20 Hz feed is fresh, so it IS the truth: if we're not in its
        // scoreboard we are not in the game. Falling through to REST here let a
        // few-seconds-old snapshot resurrect a hero the fast feed had already
        // dropped, which showed up as a spurious confirm after every rollover.
        return mySb() ?? null;
    }
    if (live() && sbScalarsSuspect && !rest) {
        const s = mySb();      // nothing better available — lane/abilities still usable
        if (s) return s;
    }
    const r = restFresh() ? rest!.heroes?.find((h) => h.name === AGENT_NAME) : undefined;
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
    if (!restFresh()) return [];
    return rest!.heroes?.map((h) => ({ name: h.name, faction: h.faction, heroClass: h.class, lane: h.lane, level: h.level, alive: h.alive })) ?? [];
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

    if (restFresh()) {
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

        // Auth token first, session cookie as the fallback (see authHeaders()).
        const headers = authHeaders({ "Content-Type": "application/json" });

        const r = await fetch(`${REST_BASE}${DEPLOY_PATH}`, { //
            method: "POST",
            headers: headers,
            body: JSON.stringify(payload), //
        });
        captureCookie(r); //
        const text = await r.text(); //
        let warning: string | undefined; //
        if (r.status === 429) { restBackoffUntil = now() + 10_000; console.log("[rest] 429 — backing off 10s"); } //
        else if (!r.ok) {
            console.log(`[rest] deploy ${r.status}: ${text}`); //
            // The server is telling us, in plain words, that it does not consider
            // us deployed — our action payloads (heroLane / action / ability) all
            // hit this same endpoint and are only valid for a live hero. Whatever
            // we believed about our own state is wrong; drop it and re-join.
            if (/first deployment requires heroClass/i.test(text) && joinedConfirmed) {
                console.log("[round] server says we're not deployed — resetting and re-joining");
                resetRoundState();
            }
        }
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

async function slowRestFallback() {
    if (ORACLE) return; // Parent Oracle handles REST completely
    if (restPolling) return;
    // FIX 8: was a hard `if (live()) return`, so once the socket came up REST
    // was never polled again and `rest` froze at its boot value forever. Keep a
    // slow trickle going so the fallback is actually a fallback.
    if (live() && now() - restAt < 10_000) return;

    restPolling = true;
    try {
        const q = ROOM !== null ? `?room=${ROOM}` : "";
        const r = await fetch(`${REST_BASE}${STATE_PATH}${q}`, { headers: authHeaders() });
        captureCookie(r);
        if (!r.ok) { console.log(`[rest] state ${r.status}`); return; }
        rest = (await r.json()) as RestState;
        restAt = now();
        processRestSideEffects();
    } catch (e) {
        console.log("[rest] poll error:", (e as Error).message);
    } finally {
        restPolling = false;
    }
}

// FIX 6: round-over used to be detected ONLY here, from the REST state — but
// slowRestFallback() bails out whenever the WebSocket is live, so in LIVE mode
// (the normal mode!) `rest` stays null and a finished round was never noticed.
// The bot went on believing it was deployed, kept firing {heroLane} at the
// deploy endpoint for the next round, and the server answered:
//   400 "First deployment requires heroClass"
// Both feeds now funnel into the same handler.
// FIX 8: the winner frame still contains last round's scoreboard, so for the
// several seconds it lingers, meView() happily returns our dead level-11 hero
// and lifecycleTick() re-confirms the join. That un-gates macro(), which starts
// posting lane commands for a hero that no longer exists -> the 400. Nothing may
// treat itself as deployed while this window is open.
function betweenRounds(): boolean {
    if (!roundOverAt) return false;
    const elapsed = now() - roundOverAt;
    // Don't sit out the full delay if the next round has visibly started and
    // we're not in it — that's our cue to deploy, not to wait.
    if (elapsed > 1_500 && live() && !W!.winner && !mySb()) { roundOverAt = 0; return false; }
    if (elapsed < CFG.rejoinDelayMs) return true;
    roundOverAt = 0;              // window elapsed; also self-heals if the feed dies
    return false;
}

function noteRoundOver(winner: Faction, source: string) {
    if (roundOverAt) return;      // the winner flag sits there for several seconds
    roundOverAt = now();          // at 20 Hz — reset once, not 100 times
    console.log(`[round] over — ${winner} won (via ${source}). Rejoining next round…`);
    resetRoundState();
}

function processRestSideEffects() {
    if (restFresh() && rest!.winner) { noteRoundOver(rest!.winner!, "REST"); return; }
    if (betweenRounds()) return;
}

async function lifecycleTick() {
    // Between rounds: the old scoreboard is still on the wire. Touch nothing.
    if (betweenRounds()) return;

    // FIX 2: before asking "where am I?", make sure we know WHO we are. The old
    // build assumed AGENT_NAME was correct and never recovered when it wasn't.
    if (!nameLearned) resolveIdentity();

    // Determine our hero from whichever data source is currently active (WS or REST)
    const me = meView();

    if (me) {
        if (!joinedConfirmed) {
            const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
            console.log(`[join] confirmed: ${me.faction} ${me.heroClass} in ${me.lane} as ${AGENT_NAME} (room ${ROOM ?? "?"})`);
            spawnedAt = now();
            pacedThisSpawn = false;      // arm the one opening Stroll
            // FIX 7: say out loud what we asked for vs what we got. A skin that
            // silently doesn't apply is otherwise invisible until you notice the
            // wrong sprite on the spectator view.
            const want = pref.skin ?? "(none)";
            const got = me.skin === undefined ? "(not reported by this feed)" : me.skin ?? "(none)";
            console.log(`[join] loadout: requested skin=${want} via "${SKIN_KEY}", item=${CFG.itemPreference[itemIdx] ?? "none"} | server reports skin=${got}`);
            if (pref.skin) {
                const took = skinApplied(pref.skin);
                if (took === false && prefIdx < SKIN_PREFS.length - 1) {
                    // The id was accepted with a 200 and then ignored — almost
                    // certainly the wrong spelling. Try the next one next round.
                    prefIdx++; prefFloor = prefIdx;
                    console.log(`[join] skin "${pref.skin}" did not apply — trying "${DEPLOY_PREFS[prefIdx].skin}" next round`);
                } else if (took === false) {
                    // Out of spellings. Don't demote to the base sprite on this
                    // evidence alone (the feed may simply not carry the field) —
                    // just say so, loudly, once.
                    console.log(`[join] WARNING none of the skin ids applied: ${SKIN_CANDIDATES.join(", ")}. ` +
                        `The field name may be wrong — try SKIN_KEY = "heroSkin" or "skinId" in thronebot.ts.`);
                } else if (took) {
                    console.log(`[join] skin "${pref.skin}" applied`);
                }
            }
            const rawSelf = W?.sb ? (W.sb as any[]).find((h) => h?.name === AGENT_NAME) : null;
            if (rawSelf) console.log(`[join] self entry: ${JSON.stringify(rawSelf).slice(0, 300)}`);
        }
        joinedConfirmed = true;
        myFaction = me.faction;
        if (now() - lastLaneCmdAt > 3500) serverLane = me.lane;

        // Sync abilities if pending
        if (me.abilityChoices && me.abilityChoices.length > 0) {
            await pickIfPending(me.heroClass, me.abilities, me.abilityChoices);
        }
    } else {
        joinedConfirmed = false;
        // FIX 3: don't just blindly re-POST a deploy (each retry carries heroLane,
        // which the server broadcasts as a "moved to mid" call). First say out loud
        // what we can see, then hunt for ourselves in other rooms, and only then
        // consider an actual re-deploy.
        reportUnconfirmed();
        if (deploySentAt > 0) await scanRoomsForSelf();
        await maybeDeploy();
    }
}

// --- FIX 2 (cont.): identity resolution --------------------------------------
// Three ladders, cheapest first:
//   a) the name we were given in .env actually appears in the state -> done
//   b) the deploy response told us our name (captured in adoptServerIdentity)
//   c) diff the roster against the pre-deploy snapshot: whoever is new is us
function resolveIdentity(): void {
    const heroes = roster();
    if (!heroes.length) return;

    if (AGENT_NAME && heroes.some((h) => h.name === AGENT_NAME)) {
        nameLearned = true;
        console.log(`[id] identity confirmed: "${AGENT_NAME}"`);
        return;
    }
    // Case-insensitive rescue: the server may normalise display names.
    if (AGENT_NAME) {
        const ci = heroes.find((h) => h.name.toLowerCase() === AGENT_NAME.toLowerCase());
        if (ci) {
            console.log(`[id] name case mismatch: env "${AGENT_NAME}" -> server "${ci.name}"`);
            AGENT_NAME = ci.name; nameLearned = true;
            return;
        }
    }
    if (!preDeployNames) return;             // we haven't deployed yet — nothing to diff

    const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
    const fresh = heroes.filter((h) => !preDeployNames!.has(h.name));
    const guess = fresh.find((h) => h.heroClass === pref.heroClass && h.lane === CFG.homeLane)
        ?? fresh.find((h) => h.heroClass === pref.heroClass)
        ?? fresh[0];
    if (guess) {
        console.log(`[id] adopting server-assigned identity "${guess.name}" (${guess.heroClass} @ ${guess.lane})`);
        if (NAME_HINT && guess.name !== NAME_HINT)
            console.log(`[id] NOTE: this differs from TW_NAME="${NAME_HINT}" — the token's account is probably named "${guess.name}"`);
        AGENT_NAME = guess.name; nameLearned = true;
    }
}

// Anything the deploy response tells us about who/where we are, we take.
function adoptServerIdentity(text: string): void {
    let j: any; try { j = JSON.parse(text); } catch { return; }
    if (!j || typeof j !== "object") return;
    const nested = j.player ?? j.hero ?? j.agent ?? j.you ?? j.self ?? {};
    const name = j.playerName ?? j.name ?? j.agentName ?? j.displayName ?? nested.name ?? nested.playerName;
    if (typeof name === "string" && name && name !== AGENT_NAME) {
        console.log(`[id] deploy response named us "${name}"`);
        AGENT_NAME = name;
    }
    const room = j.room ?? j.roomId ?? j.roomNumber ?? j.gameId ?? nested.room;
    const n = typeof room === "string" ? parseInt(room, 10) : room;
    if (typeof n === "number" && Number.isFinite(n) && ORACLE && n !== PARTY_ROOM)
        defectFromOracle(`quick-join put us in room ${n}, the party is in ${PARTY_ROOM ?? "another room"}`);
    if (typeof n === "number" && Number.isFinite(n) && n !== ROOM) {
        console.log(`[id] server placed us in room ${n} (we were watching ${ROOM ?? "none"}) — following`);
        ROOM = n;
        try { ws?.close(); } catch { }   // reconnect the feed to the right room
    }
    if (j.queued || j.waiting || /queue|waiting|partner/i.test(String(j.status ?? j.message ?? "")))
        console.log(`[join] server says we're QUEUED, not spawned yet: ${JSON.stringify(j).slice(0, 200)}`);
}

// FIX 4: quick-join can drop us in ANY room; the old build polled room 1 forever.
// If we've deployed but can't find ourselves, sweep the rooms and follow the one
// our hero is actually in.
let lastRoomScanAt = 0;
async function scanRoomsForSelf(): Promise<void> {
    if (!AGENT_NAME) return;                       // nothing to search for yet
    if (now() - lastRoomScanAt < 15_000) return;
    lastRoomScanAt = now();
    // Rooms are NOT small numbers — live traffic has us in room 49. Sweeping
    // 1..8 would never find us, so search the neighbourhood of the room we think
    // we're in first, then the low-numbered ones.
    const span = parseInt(process.env.TW_ROOM_SCAN ?? "8", 10) || 8;
    const near = ROOM ? Array.from({ length: 9 }, (_, i) => ROOM! - 4 + i).filter((r) => r > 0) : [];
    const low = Array.from({ length: span }, (_, i) => i + 1);
    const candidates = [...new Set([...near, ...low])];
    for (const r of candidates) {
        if (r === ROOM) continue;
        try {
            const res = await fetch(`${REST_BASE}${STATE_PATH}?room=${r}`, { headers: authHeaders() });
            if (!res.ok) continue;
            const j: any = await res.json().catch(() => null);
            const heroes: any[] = j?.heroes ?? j?.heroScoreboard ?? [];
            if (Array.isArray(heroes) && heroes.some((h: any) => h?.name === AGENT_NAME)) {
                console.log(`[room] found "${AGENT_NAME}" in room ${r} — switching from room ${ROOM ?? "none"}`);
                ROOM = r;
                try { ws?.close(); } catch { }
                return;
            }
        } catch { /* next room */ }
    }
}

// Loud, throttled explanation of why we think we're not in the game. This is the
// single most useful log line when the server's shape changes again.
let lastUnconfirmedLogAt = 0;
function reportUnconfirmed(): void {
    if (now() - lastUnconfirmedLogAt < 10_000) return;
    lastUnconfirmedLogAt = now();
    const names = roster().map((h) => h.name);
    console.log(
        `[diag] not confirmed in game | looking for "${AGENT_NAME || "(no name yet)"}" | room=${ROOM ?? "?"} | ` +
        `source=${live() ? "WS" : rest ? "REST" : "NONE"} | roster(${names.length}): ${names.join(", ") || "(empty)"}`
    );
    if (!AUTH_HEADER) console.log(`[diag] WARNING: no auth token resolved — set TW_AUTH (or TW_AUTH_${CHILD_ID}) in .env`);
}

// Roster of hero names present the moment BEFORE we deployed — anyone new after is us.
let preDeployNames: Set<string> | null = null;
// (identifySelf lived here — it was never called. Replaced by resolveIdentity().)

// The field name the server accepts for an ability pick. We start with the one
// the old build used and fall through the alternatives on rejection, then stick
// with whatever worked. Silent failure here was the second half of "it never
// picks skills" — a 400 used to produce no output at all.
const PICK_KEYS = ["abilityChoice", "ability", "abilityId", "chooseAbility", "levelUpAbility"];
let pickKeyIdx = 0;
let pickKeyLocked: string | null = null;
let pickRetryAfter = 0;

async function pickIfPending(heroClass: HeroClass, abilities: Ability[], choices?: string[]) {
    if (!joinedConfirmed) return;            // FIX 6
    if (!choices?.length) { lastPickId = ""; return; }
    if (pickBusy) return;
    const pick = nextAbilityPick(heroClass, abilities, choices);
    if (!pick) return;
    if (pick === lastPickId && now() - lastPickPostAt < 5000) return;
    pickBusy = true;
    try {
        // One key per invocation — restPost self-throttles, so a tight retry loop
        // here would just collect "throttled" and never reach the next candidate.
        if (pickKeyLocked === null && now() < pickRetryAfter) return;
        const key = pickKeyLocked ?? PICK_KEYS[pickKeyIdx % PICK_KEYS.length];
        const res = await restPost({ [key]: pick }, { urgent: true });
        if (res.status === 0) return;                     // throttled — next tick
        if (res.ok && !/unknown|invalid|ignored/i.test(res.warning ?? "")) {
            if (pickKeyLocked === null) {
                pickKeyLocked = key;
                if (key !== PICK_KEYS[0]) console.log(`[act] ability-pick field is "${key}" (not "${PICK_KEYS[0]}") — locking it in`);
            }
            lastPickId = pick; lastPickPostAt = now();
            console.log(`[act] ability -> ${pick}   (offered: ${choices.join(", ")})`);
            return;
        }
        console.log(`[act] ability pick "${pick}" rejected via "${key}" (${res.status}): ${res.text.slice(0, 160)}`);
        if (pickKeyLocked === null) {
            pickKeyIdx++;
            if (pickKeyIdx >= PICK_KEYS.length) {         // exhausted — cool off, then cycle again
                pickKeyIdx = 0;
                pickRetryAfter = now() + 15_000;
                console.log(`[act] no ability-pick field accepted (tried: ${PICK_KEYS.join(", ")}) — retrying in 15s. Paste this line if it persists.`);
            }
        }
    } finally {
        pickBusy = false;
    }
}

let acceptedDeploys = 0;
async function maybeDeploy() {
    if (deployInFlight) return;
    if (now() - lastDeployFailAt < 4000) return;
    // Don't deploy into a round that's already decided — wait for the next one.
    if (betweenRounds()) return;
    const retryDue = deploySentAt > 0 && now() - deploySentAt > CFG.joinConfirmGraceMs;
    if (deploySentAt > 0 && !retryDue) return;
    // FIX 3 (cont.): if the server keeps ACCEPTING our deploys but we still can't
    // find ourselves, more deploys won't help — we're either queued for a partner
    // or looking in the wrong room. Stop hammering and say so.
    if (acceptedDeploys >= 3) {
        if (retryDue) {
            deploySentAt = now();
            console.log(`[join] ${acceptedDeploys} deploys accepted but never confirmed — holding off. ` +
                `Either ranked is holding us in the pairing queue, or our hero is in a room we're not watching ` +
                `(room=${ROOM ?? "?"}). Watch: https://thronewars.gg/?room=${ROOM ?? 1}${AGENT_NAME ? `&follow=${AGENT_NAME}` : ""}`);
        }
        return;
    }
    deployInFlight = true;
    try {
        const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
        const item = CFG.itemPreference[itemIdx] ?? null;
        // Snapshot who's already in the room so we can recognise our new hero after.
        // (Was REST-only; in LIVE mode rest is null and the diff never had a baseline.)
        const before = roster().map((h) => h.name);
        if (before.length || rest?.heroes) preDeployNames = new Set(before);
        const body: Record<string, any> = { heroClass: pref.heroClass, heroLane: CFG.homeLane, message: "bot online" };
        // Ask for a specific room when we know which one we want (party mode, or
        // TW_ROOM). Harmless if the server ignores it — it echoes back the room
        // it actually gave us, and adoptServerIdentity() follows that.
        if (ROOM !== null) body.room = ROOM;
        // FIX 3: a *retry* must not re-send heroLane — that's what was firing a
        // "moved to mid" broadcast every 25s while we sat there unidentified.
        if (retryDue) delete body.heroLane;
        // Identity comes from the auth token (TW_NAME is only our local lookup key),
        // so we do NOT send a name in the deploy body — a mismatch could confuse the server.
        if (pref.skin) body[SKIN_KEY] = pref.skin;
        if (item) body.equippedItem = item;
        console.log(`[join] deploying ${pref.label} @ ${CFG.homeLane}${item ? ` +${item}` : ""}${retryDue ? " (retry)" : ""}`);
        const res = await restPost(body, { urgent: true });
        if (res.ok) {
            console.log(`[join] deploy accepted: ${res.text.slice(0, 300) || "(empty body)"}`);
            adoptServerIdentity(res.text);   // name / room / queue status, if offered
            acceptedDeploys++;
            deploySentAt = now();
            lastLaneCmdAt = now();
            currentLaneTarget = CFG.homeLane;
            deployFails = 0;
            if (res.warning && /skin/i.test(res.warning)) skinGranted = false;
        } else if (res.status === 0) {
            lastDeployFailAt = now();          // transport error — back off, don't hammer
        } else if (res.status >= 400 && res.status < 500) {
            lastDeployFailAt = now();
            // The rollover 400 isn't a loadout problem — it's handled by the
            // round-reset path in restPost(). Don't let it count toward the
            // "3 strikes and drop the skin" escalation below.
            if (!/first deployment requires heroClass/i.test(res.text)) deployFails++;
            // FIX 7: this used to be /skin|class|wallet/i, which matches the word
            // "heroClass" — and the round-rollover 400 reads:
            //   First deployment requires heroClass: "melee", "ranged", or "mage".
            // So every rollover demoted us one rung down DEPLOY_PREFS, silently
            // dropping the skin and deploying as a base mage for the rest of the
            // process's life. Demote ONLY on a message that's actually about the
            // loadout being unavailable to us.
            const skinRejected = /skin/i.test(res.text)
                && !/first deployment requires heroClass/i.test(res.text)
                && /(unknown|invalid|locked|no such|purchase|must buy|not\s+(owned|unlocked|available)|(do(es)?\s+not|don'?t)\s+own)/i.test(res.text);
            if (skinRejected && prefIdx < DEPLOY_PREFS.length - 1) {
                prefIdx++; deployFails = 0;
                console.log(`[join] skin rejected (${res.text.slice(0, 120)}), falling back to ${DEPLOY_PREFS[prefIdx].label}`);
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
    // FIX 7: fall-backs are per-round, not permanent. If the skin or item was
    // unavailable last round (or we demoted by mistake), ask for the good
    // loadout again — ownership can also change mid-session once you buy it.
    prefIdx = prefFloor;
    itemIdx = 0;
    skinGranted = true;
    acceptedDeploys = 0;
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
    ws = new WebSocket(`${host}/${q}`, { headers: authHeaders() });

    const watchdog = setTimeout(() => {
        if (!gotSnapshotThisConn && !ORACLE) {
            console.log(`[ws] no decodable snapshots from ${host} — retrying (REST keeps playing)`);
            try { ws?.close(); } catch { }
        }
    }, CFG.wsWatchdogMs);

    ws.on("open", () => {
        console.log(`[ws] connected: ${host}${q} (Child #${CHILD_ID})`);

        if (ORACLE) clearTimeout(watchdog);

        // TW is cookie-authenticated; send a room subscribe in case the server wants one.
        // ORACLE FIX: Skip subscribing on children to radically slash game server bandwidth
        if (!ORACLE) {
            try { ws!.send(JSON.stringify({ type: "subscribe", room: ROOM })); } catch { }
        }
    });

    ws.on("message", (data: WebSocket.RawData) => {
        // ORACLE FIX: Let the centralized parent parse the feed. Save 19x zlib/JSON.parse cycles.
        if (ORACLE) return;

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
let sbScalarsChecked = false;
let sbScalarsSuspect = false;
function onWsSnapshot(s: any, rawText: string) {
    if (ORACLE) return; // Handled by IPC

    const units = (s.units as any[]).map(adaptUnit).filter((u): u is U => !!u);
    const blds = ((s.buildings ?? []) as any[]).map(adaptBuilding).filter((b): b is Bld => !!b);
    const sb = ((s.heroScoreboard ?? []) as any[]).map(adaptScore).filter((e): e is ScoreEntry => !!e);
    W = { units, blds, sb, winner: s.winner ?? null };
    lastRawSb = Array.isArray(s.heroScoreboard) ? s.heroScoreboard : [];
    lastWsSnapshotAt = now();

    if (!schemaDumped) {
        schemaDumped = true;
        console.log(`[schema] fast feed on — adapted ${units.length} units, ${blds.length} buildings, ${sb.length} scoreboard entries (positional mode ${units.some((u) => u.isHero) ? "ON" : "limited"})`);
        console.log(`[schema] top-level keys: ${Object.keys(s).join(", ")}`);
        if (Array.isArray(s.heroScoreboard) && s.heroScoreboard[0])
            console.log(`[schema] raw scoreboard[0]: ${JSON.stringify(s.heroScoreboard[0]).slice(0, 400)}`);
        if (!frameSampleWritten) {
            frameSampleWritten = true;
            try { fs.writeFileSync("frame-sample.json", rawText); console.log("[schema] wrote frame-sample.json (paste this if anything still looks wrong)"); } catch { }
        }
    }

    // Sanity-check the positionally-decoded scalars. If they're nonsense the
    // layout drifted further than we can infer, so stop trusting the fast feed
    // for our own stats and let REST drive instead of acting on garbage HP.
    if (!sbScalarsChecked && sb.length) {
        const bad = sb.find((h) => !(h.maxHp > 0) || h.hp > h.maxHp * 1.5 || !(h.level >= 1 && h.level <= 40));
        if (bad) {
            sbScalarsSuspect = true;
            console.log(`[schema] WARNING scoreboard scalars look wrong (${bad.name}: lvl=${bad.level} hp=${bad.hp}/${bad.maxHp}) — falling back to REST for self stats`);
        }
        sbScalarsChecked = true;
    }

    processWsSideEffects();
}

let missingSinceAt = 0;
let spawnedAt = 0;      // when our hero last entered the world
function processWsSideEffects() {
    // Round over, seen on the fast feed (see FIX 6).
    if (W?.winner) { noteRoundOver(W.winner, "WS"); return; }
    if (betweenRounds()) return;

    const me = mySb();
    if (me) {
        missingSinceAt = 0;
        myFaction = me.faction;
        if (now() - lastLaneCmdAt > 3500) serverLane = me.lane;
        if (typeof me.recallCooldownMs === "number" && me.recallCooldownMs > 0)
            cd.recall = Math.max(cd.recall, now() + me.recallCooldownMs);
        if (me.alive) recordHp(me.hp, me.maxHp);
        // Instant ability picks from the 20 Hz feed.
        if (me.abilityChoices?.length) void pickIfPending(me.heroClass, me.abilities, me.abilityChoices);
    } else if (joinedConfirmed && W) {
        // Belt and braces: a round can also roll over without us ever catching a
        // `winner` frame (we might be mid-reconnect). If we were confirmed and our
        // hero is simply gone from the scoreboard for 3s, we're out — stop acting
        // as though we're deployed and go through a clean re-join.
        if (!missingSinceAt) missingSinceAt = now();
        else if (now() - missingSinceAt > 3000) {
            console.log(`[round] our hero left the scoreboard — treating as a new round, re-deploying`);
            missingSinceAt = 0;
            resetRoundState();
        }
        return;
    }
    reflex();
}

// ----------------------------- Actions -----------------------------------------

async function sendMovement(kind: "sprint" | "stroll", reason = ""): Promise<boolean> {
    if (!joinedConfirmed) return false;      // FIX 6: no hero, no actions
    if (!ready(kind) || channelingRecall()) return false;
    const prev = cd[kind];
    cd[kind] = now() + (kind === "sprint" ? CFG.sprintCdMs : CFG.strollCdMs);

    // Explicitly stripping WS actions to avoid silent failures - properly routing to REST
    const res = await restPost({ action: kind });
    if (actionRejected(res, kind)) {
        if (!res.warning) cd[kind] = prev;
        else if (!/remaining/.test(res.warning)) cd[kind] = now() + 2500;
        return false;
    }
    console.log(`[act] ${kind}${reason ? `  (${reason})` : ""}`);
    return true;
}

async function sendRecall(reason: string): Promise<boolean> {
    if (!joinedConfirmed) return false;      // FIX 6
    if (!ready("recall")) return false;
    const prev = cd.recall;
    cd.recall = now() + CFG.recallCdMs;
    lastRecallAt = now();

    // Explicitly stripping WS actions to avoid silent failures - properly routing to REST
    const res = await restPost({ action: "recall" }, { urgent: true });
    if (actionRejected(res, "recall")) {
        if (!res.warning) { cd.recall = prev; lastRecallAt = 0; }
        return false;
    }
    console.log(`[act] recall (${reason})`);
    return true;
}

async function commandLane(lane: Lane, reason: string, opts: { sprint?: boolean; emergency?: boolean; allowRepeat?: boolean } = {}) {
    if (!joinedConfirmed) return;            // FIX 6: this is what produced the 400s
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

    // Explicitly stripping WS actions to avoid silent failures - properly routing to REST
    const body: Record<string, any> = { heroLane: lane };
    if (wantSprint) { body.action = "sprint"; cd.sprint = now() + CFG.sprintCdMs; }
    const res = await restPost(body, { urgent: !!opts.emergency });

    if (res.ok) {
        console.log(`[act] lane ${committed}->${lane} (${reason})${wantSprint ? " +sprint" : ""}`);
    } else {
        currentLaneTarget = prevTarget;
        lastLaneCmdAt = prevAt;
    }
}

// ----------------------------- Ability builds ----------------------------------

const ALIAS: Record<string, string[]> = {
    fortitude: ["fortitude", "defensive_aura", "ring_of_healing", "soul_harvest", "bramble_patch"],
    // Skin variants that REPLACE Fury. Throne of Bones turns it into the Skeleton
    // Archer summon; we don't know the exact server id, so accept the plausible
    // spellings — whichever one is actually offered will match.
    fury: ["fury", "earthquake", "skeleton_archer", "raise_skeleton_archer", "skeletal_archer", "bone_archer", "skeleton_archers"],
};
const idsFor = (id: string) => ALIAS[id] ?? [id];

// Skin ids aren't documented, so match loosely rather than by exact string.
const skinIs = (skin: string | null, re: RegExp) => !!skin && re.test(skin);

function currentSkin(me: MeView | null): string | null {
    // Trust a real value from the server. But a MISSING one means "this feed
    // doesn't carry the field" (REST heroes have no skin key at all, and the WS
    // index for it may have drifted) — not "we have no skin". Treating that as
    // null quietly swapped us onto the base-class ability build.
    if (me?.skin) return me.skin;
    const pref = DEPLOY_PREFS[Math.min(prefIdx, DEPLOY_PREFS.length - 1)];
    return skinGranted ? pref.skin ?? null : null;
}

function buildWants(heroClass: HeroClass, skin: string | null): [string, number][] {
    if (heroClass === "melee") {
        if (skinIs(skin, /treant/i)) {
            return [["fury", 4], ["divine_shield", 1], ["cleave", 1], ["fortitude", 4], ["thorns", 4]];
        }
        const physical = enemyPhysicalShare() >= 0.5;
        const base: [string, number][] = [
            ["fortitude", 1], ["cleave", 1], ["divine_shield", 1], ["fortitude", 4], ["thorns", 1],
        ];
        return physical ? [...base, ["thorns", 4], ["fury", 4]] : [...base, ["fury", 4], ["thorns", 2]];
    }
    if (heroClass === "mage") {
        // Throne of Bones: Fury becomes Skeleton Archer — a summon that outclasses
        // the vanilla Raise Skeleton (up to 3 alive, they follow you, they tank in
        // front at 130px range). So it gets picked BEFORE Raise Skeleton. Both stay
        // at rank 1 through the core: one archer set plus one skeleton is all the
        // body-blocking we need, and the damage lives in Fireball/Tornado.
        if (skinIs(skin, /bones|throne_of_bones|necro/i)) {
            return [
                ["fireball", 1],
                ["tornado", 1],
                ["fury", 1],             // -> Skeleton Archer, ahead of raise_skeleton
                ["raise_skeleton", 1],
                ["fireball", 4],
                ["tornado", 4],
                ["fortitude", 4],
                // Nothing capped here on purpose: once the core is maxed the generic
                // fallback pours the remaining picks into whatever is still under 4,
                // which is exactly the two summons. They scale late without ever
                // stealing an early pick from Fireball or Tornado.
            ];
        }
        // Rule (field-coached): Skeleton 1 is ALWAYS picked before Fortitude/Heal 1.
        if (skinIs(skin, /pixagreen|emerald/i)) {
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
        return;
    }

    // 8) OPENING STROLL — lowest-priority action, so it never costs us a rotation.
    await maybeSpawnStroll();
}

// One Stroll on entering the game, and that's it.
//
// We spawn and walk straight out ahead of the creeps, which leaves us as the
// only thing an early enemy hero can hit. Half speed for 5s lets the wave form
// up in front of us. Deliberately fire-once: the cooldown is 25s and it's worth
// far more held in reserve for a real moment than spent pacing.
let pacedThisSpawn = true;
async function maybeSpawnStroll(): Promise<boolean> {
    if (pacedThisSpawn) return false;
    // Give up rather than stroll at some random later point.
    if (!spawnedAt || now() - spawnedAt > CFG.spawnStrollWindowMs) { pacedThisSpawn = true; return false; }
    if (!ready("stroll") || channelingRecall()) return false;
    if (now() - lastLaneCmdAt < 2_000) return false;   // a peel's aggro immunity is for moving
    const sent = await sendMovement("stroll", "opening — letting the wave form up");
    if (sent) pacedThisSpawn = true;
    return sent;
}


// ----------------------------- Discovery + boot --------------------------------

// Find a REST base + state path that returns a game-state-shaped JSON, and a
// working WS url. Falls back to overrides (TW_BASE/TW_WS) without probing.
async function discover(): Promise<boolean> {
    if (ORACLE) {
        // Child instance utilizing Oracle. Endpoints are inherited via ENV.
        return true;
    }

    // If a room isn't forced, we still try each base's state with no room (server
    // may return a default/lobby); once deployed the state reflects our room.
    for (const base of REST_CANDIDATES) {
        for (const path of STATE_PATHS) {
            for (const q of ROOM !== null ? [`?room=${ROOM}`] : ["", "?room=1"]) {
                try {
                    const r = await fetch(`${base}${path}${q}`, { headers: authHeaders() });
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
                                    method: "POST", headers: authHeaders({ "Content-Type": "application/json" }),
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

// ----------------------------- Network Topology Core ---------------------------

async function runOracleParent() {
    console.log(`[party] discovering endpoints once for the party...`);
    const ok = await discover();
    if (!ok) {
        console.error("[party] discovery failed. Provide TW_BASE/TW_WS overrides.");
        process.exit(1);
    }
    console.log(`[party] launching ${_INSTANCES} bots into room ${ROOM ?? "(auto)"} …`);

    const children: import("child_process").ChildProcess[] = [];
    for (let i = 0; i < _INSTANCES; i++) {
        setTimeout(() => {
            const botNumber = i + 1;
            const auth = process.env[`TW_AUTH_${botNumber}`] || process.env.TW_AUTH || "";
            const name = process.env[`TW_NAME_${botNumber}`] || process.env.TW_NAME || "";

            if (!auth) {
                console.log(`[party] bot #${botNumber} has NO token — skipping`);
                return;
            }

            const child = spawn(process.execPath, ["--import", "tsx", SELF_PATH], {
                stdio: ["inherit", "inherit", "inherit", "ipc"],
                env: {
                    ...process.env,
                    TW_CHILD: String(botNumber),
                    TW_INSTANCES: "1",
                    TW_AUTH: auth,
                    TW_NAME: name,
                    TW_BASE: REST_BASE,
                    TW_WS: WS_URL,
                    TW_STATE_PATH: STATE_PATH,
                    TW_DEPLOY_PATH: DEPLOY_PATH,
                    TW_ORACLE: "1", // Triggers IPC listener mode in children
                    ...(ROOM !== null ? { TW_ROOM: String(ROOM), TW_PARTY_ROOM: String(ROOM) } : {}),
                },
            });
            child.on("exit", (c) => console.log(`[party] bot #${botNumber} (${name || "unnamed"}) exited (${c})`));
            children.push(child);
        }, i * _STAGGER);
    }

    // 1. Oracle REST Poller (Eliminates 18 redundant REST queries)
    setInterval(async () => {
        try {
            const q = ROOM !== null ? `?room=${ROOM}` : "";
            const r = await fetch(`${REST_BASE}${STATE_PATH}${q}`, { headers: authHeaders() });
            if (!r.ok) return;
            const restData = await r.json();
            children.forEach(c => c.send({ type: "rest", payload: restData }));
        } catch (e) { }
    }, CFG.restPollMs);

    // 2. Oracle WS Poller (Eliminates 18 redundant JSON.parse & gunzip cycles)
    let oracleWsConn: WebSocket | null = null;
    function connectOracleWs() {
        const host = WS_URL;
        const q = ROOM !== null ? `?room=${ROOM}` : "";
        oracleWsConn = new WebSocket(`${host}/${q}`, { headers: authHeaders() });

        oracleWsConn.on("open", () => {
            console.log(`[oracle] WS connected: ${host}${q} (State Oracle active)`);
            try { oracleWsConn!.send(JSON.stringify({ type: "subscribe", room: ROOM })); } catch { }
        });

        oracleWsConn.on("message", (data: WebSocket.RawData) => {
            const buf = Array.isArray(data) ? Buffer.concat(data as Buffer[])
                : Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
            const text = decodeFrame(buf);
            if (!text) return;
            try {
                const s = JSON.parse(text);
                if (s && Array.isArray(s.units)) {
                    const units = (s.units as any[]).map(adaptUnit).filter((u): u is U => !!u);
                    const blds = ((s.buildings ?? []) as any[]).map(adaptBuilding).filter((b): b is Bld => !!b);
                    const sb = ((s.heroScoreboard ?? []) as any[]).map(adaptScore).filter((e): e is ScoreEntry => !!e);

                    // Share deserialized javascript object; much cheaper IPC than massive JSON raw strings
                    // rawSb rides along so children can run the skin check, which
                    // string-searches the undecoded row.
                    const WPayload = { units, blds, sb, winner: s.winner ?? null, rawSb: Array.isArray(s.heroScoreboard) ? s.heroScoreboard : [] };
                    children.forEach(c => {
                        try { c.send({ type: "ws", payload: WPayload }); } catch { }
                    });
                }
            } catch { }
        });

        oracleWsConn.on("close", () => {
            setTimeout(connectOracleWs, 1500);
        });
        oracleWsConn.on("error", (e) => console.log("[oracle] ws error:", (e as Error).message));
    }
    connectOracleWs();
}

async function runBot() {
    console.log(`[boot] thronebot #${CHILD_ID}${AGENT_NAME ? ` "${AGENT_NAME}"` : ""} | room ${ROOM ?? "(discover)"} | ${DEF_CLASS}${DEF_SKIN ? "/" + DEF_SKIN : ""} @ ${DEF_LANE} | item ${DEF_ITEM} | token ${AUTH_HEADER ? `set (…${AUTH_TOKEN.slice(-6)})` : "MISSING"}${DEBUG ? " | DEBUG" : ""}`);
    if (!AUTH_HEADER) console.log("[boot] no TW_AUTH/TW_AUTH_1 found — the server will treat us as an anonymous guest and name us itself.");
    if (!NAME_HINT) console.log("[boot] no TW_NAME set — we'll learn our name from the game state after deploying.");

    const ok = await discover();
    if (!ok) {
        console.error("[boot] could not discover a Throne Wars API endpoint.");
        process.exit(1);
    }

    // Oracle architectural upgrade: 
    // If the Oracle is running, children don't even need to open a WebSocket connection!
    // This saves 18 unnecessary socket connections to the game server.
    if (!ORACLE) {
        wsConnect();
    }

    // 1. IPC Listener (replaces native network fetching if Oracle is running)
    if (process.env.TW_ORACLE) {
        process.on("message", (msg: any) => {
            if (msg.type === "ws") {
                wsFrames++;
                W = msg.payload;
                lastRawSb = msg.payload?.rawSb ?? [];
                lastWsSnapshotAt = now();
                processWsSideEffects();
            } else if (msg.type === "rest") {
                rest = msg.payload;
                restAt = now();
                processRestSideEffects();
            }
        });
    }

    // Generate unique numerical offset based on CHILD ID to naturally batch bursts across instances
    const childIdNum = parseInt(CHILD_ID, 10) || 1;

    // 2. Lifecycle Tick (Staggered to prevent 19x concurrent deploys/pick evaluations)
    const lifecycleJitter = (childIdNum * 123) % 1500;
    setTimeout(() => {
        setInterval(lifecycleTick, 1500);
        void lifecycleTick();
    }, lifecycleJitter);

    // 3. REST Backup Loop 
    const restJitter = (childIdNum * 311) % CFG.restPollMs;
    setTimeout(() => {
        setInterval(slowRestFallback, CFG.restPollMs);
        void slowRestFallback();
    }, restJitter);

    // 4. Macro Loop (Staggered micro-timing so logic evaluation doesn't overlap across bots)
    const macroJitter = (childIdNum * 47) % CFG.macroMs;
    setTimeout(() => {
        setInterval(() => {
            if (macroBusy) return;
            macroBusy = true;
            macro().catch((e) => console.log("[macro] error:", (e as Error).message)).finally(() => { macroBusy = false; });
        }, CFG.macroMs);
    }, macroJitter);

    // 5. Heartbeat Log
    setInterval(() => {
        if (!joinedConfirmed) return;
        const me = meView();
        if (!me) return;
        const mode = process.env.TW_ORACLE
            ? (live() ? "IPC-LIVE" : "IPC-REST")
            : (live() ? "LIVE(20Hz)" : "REST(1.5s)");
        console.log(
            `[status] mode=${mode} class=${me.heroClass}${currentSkin(me) ? `/${currentSkin(me)}` : ""} lane=${serverLane ?? "?"} lvl=${me.level} hp=${(hpFracOf(me) * 100) | 0}%` +
            ` recall=${ready("recall") ? "ready" : Math.ceil((cd.recall - now()) / 1000) + "s"} wsFrames=${wsFrames}`
        );
    }, 30_000);
}

// ==============================================================================
// ENTRYPOINT DISPATCH
// ==============================================================================

async function main() {
    if (_IS_PARENT) {
        await runOracleParent();
    } else {
        await runBot();
    }
}

void main();