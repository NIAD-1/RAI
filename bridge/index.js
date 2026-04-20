/**
 * Research Bot (Professor AI) — Baileys WhatsApp Bridge
 *
 * Connects to WhatsApp via QR code scan and forwards all incoming
 * messages to the Rust Axum backend via HTTP POST.
 * Listens for reply instructions from the backend to send responses.
 */

import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  BufferJSON,
  initAuthCreds,
  proto,
} from "@whiskeysockets/baileys";
import pg from "pg";
const { Client } = pg;
import { Boom } from "@hapi/boom";
import pino from "pino";
import qrcode from "qrcode-terminal";
import fs from "fs";
import http from "http";

// Prevent process from crashing on unhandled errors
process.on("unhandledRejection", (err) => {
  console.error("⚠️  Unhandled rejection:", err?.message || err);
});
process.on("uncaughtException", (err) => {
  console.error("⚠️  Uncaught exception:", err?.message || err);
});

const RUST_BACKEND = process.env.BACKEND_URL || "http://localhost:3000";
const BRIDGE_PORT = parseInt(process.env.BRIDGE_PORT || "8002", 10);
const BRIDGE_SECRET = process.env.BRIDGE_SECRET || "local_dev_secret_123";
const DATABASE_URL = process.env.DATABASE_URL;
const AUTH_FOLDER = "./auth_state";

const logger = pino({ level: "warn" });

let sock = null;
let latestQR = null; // Store latest QR for web display

// Global JID routing map
const jidMap = new Map();
// Global message deduplication cache to prevent Baileys from double-firing on syncs
const processedMessages = new Set();
// Track users who have already interacted with the bot (for welcome message)
const seenUsers = new Set();

// ─── PostgreSQL Auth State Implementation ─────────────────────────────

async function usePostgresAuthState(dbUrl) {
  const client = new Client({
    connectionString: dbUrl,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();

  // Create table if it doesn't exist
  await client.query(`
    CREATE TABLE IF NOT EXISTS baileys_auth (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `);

  const readData = async (key) => {
    try {
      const res = await client.query(
        "SELECT value FROM baileys_auth WHERE key = $1",
        [key]
      );
      if (res.rows.length > 0) {
        return JSON.parse(res.rows[0].value, BufferJSON.reviver);
      }
      return null;
    } catch (e) {
      return null;
    }
  };

  const writeData = async (key, value) => {
    const data = JSON.stringify(value, BufferJSON.replacer);
    await client.query(
      "INSERT INTO baileys_auth (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
      [key, data]
    );
  };

  const removeData = async (key) => {
    await client.query("DELETE FROM baileys_auth WHERE key = $1", [key]);
  };

  const creds = (await readData("creds")) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === "app-state-sync-key" && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
          return data;
        },
        set: async (data) => {
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              if (value) {
                await writeData(key, value);
              } else {
                await removeData(key);
              }
            }
          }
        },
      },
    },
    saveCreds: () => writeData("creds", creds),
    clearState: async () => {
      try {
        await client.query("DELETE FROM baileys_auth");
      } catch (e) {
        console.error("Error clearing DB state:", e);
      }
    },
  };
}

// ─── Connect to WhatsApp ────────────────────────────────────────────────

const PHONE_NUMBER = process.env.PHONE_NUMBER || "";

async function startBot() {
  let authData;
  if (DATABASE_URL) {
    console.log("💾 Using PostgreSQL for authentication state persistence...");
    authData = await usePostgresAuthState(DATABASE_URL);
  } else {
    console.log("📁 Using local file system for authentication state...");
    authData = await useMultiFileAuthState(AUTH_FOLDER);
  }

  const { state, saveCreds } = authData;
  const { version } = await fetchLatestBaileysVersion();

  const usePairingCode = PHONE_NUMBER.length > 0 && !state.creds.registered;

  sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    logger,
    printQRInTerminal: !usePairingCode,
  });

  sock.ev.on("creds.update", saveCreds);

  // Request pairing code if phone number is set and not yet registered
  if (usePairingCode) {
    setTimeout(async () => {
      try {
        let code = await sock.requestPairingCode(PHONE_NUMBER);
        code = code?.match(/.{1,4}/g)?.join('-') || code;
        console.log("\n╔══════════════════════════════════════════════════╗");
        console.log("║         WHATSAPP PAIRING CODE                   ║");
        console.log("╚══════════════════════════════════════════════════╝");
        console.log(`\n🔑 YOUR CODE: ${code}\n`);
        console.log("📱 Open WhatsApp on your phone:");
        console.log("   → Settings → Linked Devices → Link a Device");
        console.log("   → Tap 'Link with phone number instead'");
        console.log(`   → Enter this code: ${code}\n`);
      } catch (err) {
        console.error("❌ Failed to request pairing code:", err.message);
        console.log("Falling back to QR code mode...");
      }
    }, 3000);
  }

  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr && !usePairingCode) {
      latestQR = qr;
      try { fs.writeFileSync("/tmp/latest_qr.txt", qr); } catch(_) {}
      console.log(`\n📋 QR code generated. Raw string: ${qr}\n`);
      qrcode.generate(qr, { small: true });
    }

    if (connection === "close") {
      const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
      if (reason === DisconnectReason.loggedOut) {
        console.log("❌ Logged out. Clearing local/DB state and restarting to re-link.");
        if (DATABASE_URL && authData.clearState) {
          authData.clearState().then(() => {
             startBot();
          });
        } else {
          if (!DATABASE_URL && fs.existsSync(AUTH_FOLDER)) {
            fs.rmSync(AUTH_FOLDER, { recursive: true });
          }
          startBot();
        }
      } else {
        console.log(`⚠️  Connection closed (reason ${reason}). Reconnecting...`);
        startBot();
      }
    } else if (connection === "open") {
      latestQR = null;
      try { fs.unlinkSync("/tmp/latest_qr.txt"); } catch(_) {}
      console.log("\n✅ WhatsApp connected! Professor AI bridge is live.\n");
    }
  });

  // ── Incoming messages ────────────────────────────────────────────
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;

    for (const msg of messages) {
      if (msg.key.fromMe) continue;
      if (!msg.message) continue;
      
      // Deduplicate messages since WhatsApp syncs can fire multiple events for the same message ID
      const msgId = msg.key.id;
      if (processedMessages.has(msgId)) continue;
      processedMessages.add(msgId);
      
      // Ensure we don't leak memory indefinitely
      if (processedMessages.size > 1000) {
        const firstArr = Array.from(processedMessages).slice(0, 500);
        firstArr.forEach(id => processedMessages.delete(id));
      }

      const chatJid = msg.key.remoteJid;
      if (!chatJid || chatJid.endsWith("@broadcast") || chatJid.endsWith("@newsletter")) continue;

      // For group messages, get the actual sender from participant
      const isGroup = chatJid.endsWith("@g.us");
      const participantJid = msg.key.participant || chatJid;
      const phone = participantJid.replace("@s.whatsapp.net", "").replace("@lid", "");
      const pushName = msg.pushName || "";

      // Globally cache the exact route for this phone so push events never fail to map linked device IDs stringing from +82
      jidMap.set(phone, chatJid);

      // ── Read Receipt (blue ticks) ──────────────────────────────
      try {
        await sock.readMessages([msg.key]);
      } catch (_) { /* best-effort, don't crash on receipt failure */ }

      // ── Typing Indicator ("composing...") ──────────────────────
      try {
        await sock.sendPresenceUpdate("composing", chatJid);
      } catch (_) { /* best-effort */ }

      let msgType = "text";
      let body = "";
      let mediaId = null;

      // Text message
      if (msg.message.conversation) {
        body = msg.message.conversation;
      } else if (msg.message.extendedTextMessage?.text) {
        body = msg.message.extendedTextMessage.text;
      }
      // Voice note
      else if (msg.message.audioMessage) {
        msgType = "audio";
        mediaId = msg.key.id;
      }
      // Ignore other types for now
      else {
        continue;
      }

      // ── GROUP FILTER: Only respond to relevant messages in groups ──
      if (isGroup && msgType === "text") {
        const lower = body.toLowerCase().trim();

        // Always respond if they mention the bot
        const mentionsBot = lower.includes("professor") || lower.includes("research") || lower.includes("bot");

        // Check for command keywords
        const keywords = [
          "report:", "simulate", "thesis", "assignment", "help", "start"
        ];
        const hasKeyword = keywords.some((kw) => lower.includes(kw));

        if (!mentionsBot && !hasKeyword) {
          // Skip noise messages in groups
          console.log(`🔇 [GROUP] Skipped from ${pushName}: "${body.substring(0, 40)}"`);
          continue;
        }
      }

      // ── Welcome message for first-time users ──────────────────
      if (!isGroup && !seenUsers.has(phone) && msgType === "text") {
        seenUsers.add(phone);
        const lower = body.toLowerCase().trim();
        const isGreeting = ["hi", "hello", "hey", "start", "/start", "help", "/help", "menu"].includes(lower) 
          || (lower.length < 15 && (lower.startsWith("hi ") || lower.startsWith("hello ") || lower.startsWith("hey ")));
        if (isGreeting) {
          const welcomeMsg = `🎓 *Welcome to Professor AI!*\n\nHi ${pushName || "there"}! I'm your autonomous academic research assistant. This is your first time here, so let me show you around:\n\n📝 *Quick Research Brief*\nJust type any topic and I'll research it instantly.\n_Example: "Effects of social media on mental health"_\n\n📄 *Full PDF Report*\nType \`report:\` followed by your topic for a detailed academic paper.\n_Example: "report: impact of AI on healthcare"_\n\n🧠 *I Remember Context*\nAsk follow-up questions and I'll understand what you mean!\n\n🔄 *Reset Context*\nType \`/clear\` to start fresh.\n\nSend a topic whenever you're ready! 🚀`;
          try {
            await sock.sendPresenceUpdate("paused", chatJid);
            await sock.sendMessage(chatJid, { text: welcomeMsg });
            console.log(`🆕 Welcome message sent to new user: ${pushName} (${phone})`);
          } catch (e) {
            console.error(`❌ Failed to send welcome:`, e.message);
          }
          continue;
        }
      }

      // Forward to the Rust backend
      const payload = JSON.stringify({
        from: chatJid,
        type: msgType,
        body: body,
        media_key: mediaId,
        push_name: pushName,
        message_id: msg.key.id,
      });

      console.log(`📩 ${isGroup ? "[GROUP] " : ""}${phone} (${pushName}): ${msgType === "audio" ? "[voice note]" : body}`);

      try {
        const resp = await fetch(`${RUST_BACKEND}/bridge/incoming`, {
          method: "POST",
          headers: { 
            "Content-Type": "application/json",
            "X-Bridge-Auth": BRIDGE_SECRET
          },
          body: payload,
        });

        if (resp.ok) {
          const data = await resp.json();
          console.log(`🔍 Backend response:`, JSON.stringify(data).substring(0, 200));

          // Reply to the same chat (group or DM)
          if (data.reply) {
            try {
              await sock.sendPresenceUpdate("paused", chatJid);
              await sock.sendMessage(chatJid, { text: data.reply });
              console.log(`📤 Reply sent to ${chatJid}`);
            } catch (sendErr) {
              console.error(`❌ Failed to send reply to ${chatJid}:`, sendErr.message);
            }
          }

          // If the backend wants to send a PDF document
          if (data.document_base64) {
            try {
              const pdfBuffer = Buffer.from(data.document_base64, "base64");
              await sock.sendMessage(chatJid, {
                document: pdfBuffer,
                mimetype: "application/pdf",
                fileName: data.document_filename || "report.pdf",
                caption: data.document_caption || "📊 Your Research Report",
              });
              console.log(`📄 PDF sent to ${chatJid}`);
            } catch (sendErr) {
              console.error(`❌ Failed to send PDF to ${chatJid}:`, sendErr.message);
            }
          }
        } else {
          console.error(`❌ Backend returned ${resp.status}: ${await resp.text()}`);
        }
      } catch (err) {
        console.error(`❌ Backend error for ${phone}:`, err.message);
      }

      // Download and forward voice notes
      if (msgType === "audio" && msg.message.audioMessage) {
        try {
          const { downloadMediaMessage } = await import("@whiskeysockets/baileys");
          const audioBuffer = await downloadMediaMessage(msg, "buffer", {});
          const audioBase64 = audioBuffer.toString("base64");

          const audioResp = await fetch(`${RUST_BACKEND}/bridge/audio`, {
            method: "POST",
            headers: { 
              "Content-Type": "application/json",
              "X-Bridge-Auth": BRIDGE_SECRET
            },
            body: JSON.stringify({
              from: phone,
              audio_base64: audioBase64,
              push_name: pushName,
            }),
          });

          if (audioResp.ok) {
            const data = await audioResp.json();
            if (data.reply) {
              await sock.sendMessage(chatJid, { text: data.reply });
              console.log(`📤 Voice reply sent to ${chatJid}`);
            }
            if (data.document_base64) {
              const pdfBuffer = Buffer.from(data.document_base64, "base64");
              await sock.sendMessage(chatJid, {
                document: pdfBuffer,
                mimetype: "application/pdf",
                fileName: data.document_filename || "report.pdf",
                caption: data.document_caption || "📊 Your Research Report",
              });
            }
          }
        } catch (err) {
          console.error(`❌ Audio processing error:`, err.message);
          await sock.sendMessage(chatJid, {
            text: "Sorry, I couldn't process that voice note. Please try again or type your message.",
          });
        }
      }
    }
  });
}

// ─── HTTP server for outgoing messages (backend → WhatsApp) ─────────────

const server = http.createServer(async (req, res) => {
  if (req.method === "POST" && req.url === "/send") {
    if (req.headers["x-bridge-auth"] !== BRIDGE_SECRET) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Unauthorized API access" }));
      return;
    }

    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", async () => {
      try {
        const { to, text, document_base64, document_filename, document_caption } = JSON.parse(body);
        
        let jid = to;
        if (jidMap.has(to)) {
          jid = jidMap.get(to);
        } else if (!to.includes("@")) {
          jid = `${to}@s.whatsapp.net`;
        }
        
        console.log(`[PUSH] Received request to send to ${jid}`);

        if (text) {
          console.log(`[PUSH] Sending text: ${text.substring(0, 50)}...`);
          await sock.sendMessage(jid, { text });
        }
        if (document_base64) {
          const pdfBuffer = Buffer.from(document_base64, "base64");
          await sock.sendMessage(jid, {
            document: pdfBuffer,
            mimetype: "application/pdf",
            fileName: document_filename || "report.pdf",
            caption: document_caption || "",
          });
        }

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "sent" }));
        console.log(`[PUSH] Successfully sent.`);
      } catch (err) {
        console.error(`[PUSH] Error:`, err);
        res.writeHead(500);
        res.end(JSON.stringify({ error: err.message }));
      }
    });
  } else if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "alive", connected: !!sock }));
  } else if (req.method === "GET" && req.url === "/qr") {
    res.writeHead(200, { "Content-Type": "text/html" });
    if (latestQR) {
      res.end(`<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Scan QR - Professor AI</title>
<script src="https://cdn.jsdelivr.net/npm/qrcode@1.5.3/build/qrcode.min.js"></script>
<style>body{background:#111;color:#fff;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;font-family:system-ui;margin:0}h1{margin-bottom:8px}p{color:#aaa;margin-top:4px}canvas{border-radius:12px;margin:20px}</style></head>
<body><h1>🎓 Professor AI</h1><p>Scan this QR code with WhatsApp to connect</p><canvas id="qr"></canvas><p>⏳ QR refreshes every 30s — reload if expired</p>
<script>QRCode.toCanvas(document.getElementById('qr'),"${latestQR}",{width:300,margin:2},function(e){if(e)document.body.innerHTML='<h1>Error: '+e.message+'</h1>';})</script></body></html>`);
    } else {
      res.end(`<!DOCTYPE html><html><head><meta charset="utf-8"><title>Professor AI</title>
<style>body{background:#111;color:#0f0;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;font-family:system-ui;margin:0}h1{font-size:3em}</style></head>
<body><h1>✅ Connected!</h1><p>WhatsApp is already linked. No QR needed.</p></body></html>`);
    }
  } else {
    res.writeHead(404);
    res.end("Not found");
  }
});

server.listen(BRIDGE_PORT, () => {
  console.log(`🌉 Bridge API listening on http://localhost:${BRIDGE_PORT}`);
  console.log(`🦀 Forwarding messages to ${RUST_BACKEND}`);
  console.log("");
});

startBot();
