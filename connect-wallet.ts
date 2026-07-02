/**
 * Connect a wallet to your Defense of the Agents agent WITHOUT putting a private
 * key on this machine. Two steps:
 *
 *  STEP 1 — print the exact message to sign (uses a fresh timestamp):
 *     WALLET_ADDRESS=0xYourAddr npx tsx connect-wallet.ts
 *   Copy the printed message, sign it in your own wallet (MetaMask "Sign Message",
 *   hardware wallet, etc.), and copy the resulting signature. Do this within 5 min.
 *
 *  STEP 2 — submit the signature you produced (reuse the SAME timestamp it printed):
 *     WALLET_ADDRESS=0xYourAddr WALLET_TIMESTAMP=1713200000000 \
 *     WALLET_SIGNATURE=0x... DOTA_API_KEY=wc2a_... npx tsx connect-wallet.ts
 *
 *  OPTIONAL (NOT recommended) — auto-sign with a BURNER key in one shot:
 *     WALLET_PRIVATE_KEY=0x... DOTA_API_KEY=wc2a_... npx tsx connect-wallet.ts
 *
 * The connection persists across games. After this, the bot equips ring_of_regen
 * on first deploy (the wallet must own it; it's a Bettermint ERC-1155, secondary
 * market only). This is not financial advice — it just describes the mechanic.
 */

import "dotenv/config"; // loads variables from a .env file (git-ignored) into process.env

const API_BASE = "https://game.defenseoftheagents.com";
const ADDR = process.env.WALLET_ADDRESS ?? "";
const API_KEY = process.env.DOTA_API_KEY ?? "";
const PK = process.env.WALLET_PRIVATE_KEY ?? "";
const SIG = process.env.WALLET_SIGNATURE ?? "";
const TS = process.env.WALLET_TIMESTAMP ?? "";

const msg = (address: string, timestamp: number) =>
    `I am connecting my wallet to Defense of the Agents.\n\nAddress: ${address}\nTimestamp: ${timestamp}`;

async function submit(address: string, timestamp: number, signature: string) {
    const r = await fetch(`${API_BASE}/api/wallet/connect`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${API_KEY}` },
        body: JSON.stringify({ address, source: "injected", signature, timestamp }),
    });
    console.log(`\nServer ${r.status}: ${await r.text()}`);
}

// Optional burner-key path (dynamic import so the manual path needs no viem).
if (PK) {
    if (!API_KEY) { console.error("Set DOTA_API_KEY."); process.exit(1); }
    // @ts-ignore optional dependency resolved at runtime via viem's exports map
    const { privateKeyToAccount } = await import("viem/accounts");
    const account = privateKeyToAccount(PK as `0x${string}`);
    const timestamp = Date.now();
    const signature = await account.signMessage({ message: msg(account.address, timestamp) });
    await submit(account.address, timestamp, signature);
}
// Step 2: we have a signature → submit it.
else if (SIG && TS && ADDR && API_KEY) {
    await submit(ADDR, Number(TS), SIG);
}
// Step 1: just print what to sign.
else if (ADDR) {
    const timestamp = Date.now();
    console.log("Sign EXACTLY this message in your wallet (within 5 minutes):\n");
    console.log("--------------------------------------------------");
    console.log(msg(ADDR, timestamp));
    console.log("--------------------------------------------------\n");
    console.log("Then run step 2 with the SAME timestamp:");
    console.log(`  WALLET_ADDRESS=${ADDR} WALLET_TIMESTAMP=${timestamp} \\`);
    console.log(`  WALLET_SIGNATURE=0xYourSig DOTA_API_KEY=wc2a_... npx tsx connect-wallet.ts`);
} else {
    console.error("Set WALLET_ADDRESS (step 1), or WALLET_ADDRESS+WALLET_TIMESTAMP+WALLET_SIGNATURE+DOTA_API_KEY (step 2).");
    process.exit(1);
}