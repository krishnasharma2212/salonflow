import "dotenv/config";

/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║   SalonFlow — WhatsApp AI Booking Assistant                      ║
 * ║   Adapted for SalonFlow PostgreSQL schema                        ║
 * ║   Rewritten for WhiskeySockets Baileys v7.0.0                    ║
 * ╚══════════════════════════════════════════════════════════════════╝
 *
 * Bot-specific columns added to users table on first boot:
 *   wa_status       INTEGER   — 1=fresh/error  2=qr shown  3=connected
 *   wa_qr_code      TEXT      — base64 QR data URL
 *   wa_appointments JSONB     — [{id,service,date,time,...}]
 *   wa_chat_history JSONB     — {sessionId:{phone:[messages]}}
 *   wa_gcal_creds   JSONB     — Google OAuth2 credentials
 */

import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  Browsers,
  fetchLatestBaileysVersion,
} from "@whiskeysockets/baileys";
import { Boom }        from "@hapi/boom";
import P               from "pino";
import { OpenAI }      from "openai";
import { google }      from "googleapis";
import pkg             from "pg";
import fs              from "fs/promises";
import { createReadStream } from "fs";
import path            from "path";
import os              from "os";
import crypto          from "crypto";
import QRCode          from "qrcode";
import { v4 as uuid }  from "uuid";

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────

const CFG = {
  openaiKey:    process.env.OPENAI_API_KEY  || "",
  dbUrl:        process.env.DATABASE_URL    || "",
  debounceMs:   parseInt(process.env.DEBOUNCE_MS     || "3000"),  // 3s default (was 7s)
  bufferMinutes:parseInt(process.env.APPT_BUFFER_MIN || "15"),
  timezone:     "Asia/Kolkata",
  lookaheadDays: 7,
  historyTurns:  6,
  userMsgLimit:  30,
  userCooldownMs: 60 * 60 * 1000,
  sessionDailyLimit: 500,
  sessionBlockMs: 2 * 60 * 60 * 1000,
};

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — CLIENTS
// ─────────────────────────────────────────────────────────────────────────────

const openai = new OpenAI({ apiKey: CFG.openaiKey });
const pool   = new pkg.Pool({
  connectionString:        CFG.dbUrl,
  max:                     10,
  idleTimeoutMillis:       30_000,
  connectionTimeoutMillis: 8_000,  // fail fast if DB unreachable
  allowExitOnIdle:         false,
});

// Prevent stuck pool clients from crashing the process silently
pool.on("error", (err) => {
  console.error("[POOL] Client error:", err.message);
});

// Baileys logger — suppress noisy output in production
const logger = P({ level: process.env.LOG_LEVEL || "warn" });

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 — IN-MEMORY STATE
// ─────────────────────────────────────────────────────────────────────────────

const SESSION_FILE = "sessionID.txt";

// Clean up rate-limiting Maps every hour — prevents unbounded memory growth
setInterval(() => {
  const cutoff = Date.now() - Math.max(CFG.userCooldownMs, CFG.sessionBlockMs);
  for (const [jid, times] of userMessageLog.entries()) {
    if (!times.some(t => t > cutoff)) userMessageLog.delete(jid);
  }
  for (const [sid, times] of sessionMessageLog.entries()) {
    if (!times.some(t => t > cutoff)) sessionMessageLog.delete(sid);
  }
  // Also clean lastReply map
  for (const [jid, ts] of lastReply.entries()) {
    if (Date.now() - ts > 3_600_000) lastReply.delete(jid);
  }
  // Clean _lastSentTs
  for (const [jid, ts] of _lastSentTs.entries()) {
    if (Date.now() - ts > 3_600_000) _lastSentTs.delete(jid);
  }
}, 3_600_000);
const AUTH_DIR     = path.resolve("./wa_credentials");

const userMessageLog    = new Map();
const sessionMessageLog = new Map();
const lastReply         = new Map();
const userProcState     = new Map();
let   isRestarting      = false;

/** @type {ReturnType<typeof makeWASocket> | null} */
let sock = null;

/** Reminder interval handle so we can clear on reconnect */
let reminderInterval = null;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 — DATABASE HELPERS
// All queries use the actual SalonFlow schema column names.
// Bot-specific data lives in wa_* columns (auto-created on boot).
// ─────────────────────────────────────────────────────────────────────────────

async function getUserBySession(sessionId) {
  const { rows } = await pool.query(
    "SELECT * FROM users WHERE whatsapp_session_id = $1 LIMIT 1",
    [sessionId]
  );
  return rows[0] || null;
}

/**
 * Update only allowed columns on the users row.
 * Maps bot internal names → actual SalonFlow column names.
 */
async function updateUserCols(userId, updates = {}) {
  if (isRestarting || !userId) return;

  // Map bot field names → real column names
  const COLMAP = {
    wa_status:       "wa_status",
    qr_code:         "wa_qr_code",
    qr:              "wa_qr_code",
    whatsapp_connected: "whatsapp_connected",
    whatsapp_phone:  "whatsapp_phone",
  };

  const sets = [], vals = [];
  let i = 1;
  for (const [k, v] of Object.entries(updates)) {
    const col = COLMAP[k];
    if (col) { sets.push(`${col} = $${i++}`); vals.push(v); }
  }
  if (!sets.length) return;
  vals.push(userId);
  await pool.query(`UPDATE users SET ${sets.join(", ")} WHERE id = $${i}`, vals);
}

/** Wrapper used by session lifecycle (takes sessionId, resolves to userId). */
async function updateUserBySession(sessionId, updates = {}) {
  const user = await getUserBySession(sessionId);
  if (!user) return;
  await updateUserCols(user.id, updates);
}

// ── Appointments (stored in wa_appointments JSONB) ────────────────────────────
function getAppointments(user) {
  const raw = user.wa_appointments;
  if (!raw) return [];
  return Array.isArray(raw) ? raw : Object.values(raw);
}

async function saveAppointments(userId, arr) {
  await pool.query(
    "UPDATE users SET wa_appointments = $1 WHERE id = $2",
    [JSON.stringify(arr), userId]
  );
}

// ── Chat history (stored in wa_chat_history JSONB) ────────────────────────────
function getChatHistory(user) {
  return user.wa_chat_history || {};
}

async function saveChatHistory(userId, history) {
  await pool.query(
    "UPDATE users SET wa_chat_history = $1 WHERE id = $2",
    [JSON.stringify(history), userId]
  );
}

/**
 * Atomically read-modify-write a JSONB column using a PostgreSQL transaction
 * with FOR UPDATE row-level locking. Parallel messages queue up instead of
 * racing — guarantees zero data loss on wa_appointments and wa_chat_history.
 */
async function atomicUpdateJsonb(userId, column, modifierFn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `SELECT ${column} FROM users WHERE id = $1 FOR UPDATE`,
      [userId]
    );
    let current = rows[0]?.[column];
    if (column === "wa_appointments") {
      current = Array.isArray(current) ? current
              : (current ? Object.values(current) : []);
    }
    if (column === "wa_chat_history") {
      current = current || {};
    }
    const updated = modifierFn(current);
    await client.query(
      `UPDATE users SET ${column} = $1 WHERE id = $2`,
      [JSON.stringify(updated), userId]
    );
    await client.query("COMMIT");
    return updated;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

// ── Subscription check — reads from subscriptions table ──────────────────────
async function ensureAccess(sessionId) {
  const user = await getUserBySession(sessionId);
  if (!user) return false;

  // Check if SalonFlow subscription is active (subscriptions table)
  const { rows } = await pool.query(
    "SELECT status FROM subscriptions WHERE user_id = $1 LIMIT 1",
    [user.id]
  );
  if (rows[0]?.status === "active") return true;

  // Fallback: allow if plan is pro/business (paid)
  if (user.plan === "pro" || user.plan === "business") return true;

  // No active subscription — deny
  console.warn(`[ACCESS] denied for user ${user.id} — no active subscription`);
  return false;
}

/**
 * Build a "profile" object from real SalonFlow columns + services table.
 * This replaces the old chatpilot `profile` JSONB column.
 */
// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3b — DB MIGRATIONS (run once on boot)
// ─────────────────────────────────────────────────────────────────────────────

async function runMigrations() {
  const migrations = [
    // AI usage cost tracking per salon
    `CREATE TABLE IF NOT EXISTS ai_usage (
      id            SERIAL PRIMARY KEY,
      user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      session_date  DATE NOT NULL DEFAULT CURRENT_DATE,
      model         VARCHAR(60) NOT NULL DEFAULT 'gpt-4o-mini',
      prompt_tokens     INTEGER DEFAULT 0,
      completion_tokens INTEGER DEFAULT 0,
      total_tokens      INTEGER DEFAULT 0,
      cost_usd          NUMERIC(10,6) DEFAULT 0,
      calls             INTEGER DEFAULT 1,
      created_at    TIMESTAMPTZ DEFAULT NOW(),
      updated_at    TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(user_id, session_date, model)
    )`,
    // Payment screenshots per appointment
    `CREATE TABLE IF NOT EXISTS payment_screenshots (
      id              SERIAL PRIMARY KEY,
      user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      appointment_id  VARCHAR(64) NOT NULL,
      phone           VARCHAR(30),
      screenshot_path TEXT,
      screenshot_b64  TEXT,
      transaction_id  VARCHAR(120),
      amount_verified NUMERIC(10,2),
      verified        BOOLEAN DEFAULT FALSE,
      fake_score      NUMERIC(3,2) DEFAULT 0,
      ai_notes        TEXT,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )`,
    // Add payment_status and advance_paid columns to users.wa_appointments (done in JS not SQL)
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS schedule_mode VARCHAR(20) DEFAULT 'hourly'`,
  ];
  for (const sql of migrations) {
    try {
      await pool.query(sql);
    } catch (e) {
      console.error("[MIGRATION] failed:", sql.slice(0, 60), e.message);
    }
  }
  console.log("[MIGRATION] complete");
}

// ── AI COST TRACKING ──────────────────────────────────────────────────────────
// Pricing per 1M tokens (as of June 2025, gpt-4o-mini)
const MODEL_PRICING = {
  "gpt-4o-mini":          { input: 0.150,  output: 0.600  },   // per 1M tokens
  "gpt-4o-mini-2024-07-18":{ input: 0.150,  output: 0.600  },
  "gpt-4o":               { input: 2.500,  output: 10.000 },
  "gpt-4o-2024-11-20":    { input: 2.500,  output: 10.000 },
  "gpt-4.1-mini":         { input: 0.400,  output: 1.600  },
  "gpt-4.1-nano":         { input: 0.100,  output: 0.400  },
  "gpt-4.1":              { input: 2.000,  output: 8.000  },
};

function calcCostUsd(model, promptTokens, completionTokens) {
  const pricing = MODEL_PRICING[model] || MODEL_PRICING["gpt-4o-mini"];
  return (promptTokens * pricing.input + completionTokens * pricing.output) / 1_000_000;
}

async function trackUsage(userId, model, usage) {
  if (!userId || !usage) return;
  const { prompt_tokens = 0, completion_tokens = 0 } = usage;
  const total = prompt_tokens + completion_tokens;
  const cost  = calcCostUsd(model, prompt_tokens, completion_tokens);
  try {
    await pool.query(`
      INSERT INTO ai_usage (user_id, session_date, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, calls)
      VALUES ($1, CURRENT_DATE, $2, $3, $4, $5, $6, 1)
      ON CONFLICT (user_id, session_date, model) DO UPDATE SET
        prompt_tokens     = ai_usage.prompt_tokens     + $3,
        completion_tokens = ai_usage.completion_tokens + $4,
        total_tokens      = ai_usage.total_tokens      + $5,
        cost_usd          = ai_usage.cost_usd          + $6,
        calls             = ai_usage.calls             + 1,
        updated_at        = NOW()
    `, [userId, model, prompt_tokens, completion_tokens, total, cost]);
  } catch (e) {
    console.error("[TRACK-USAGE]", e.message);
  }
}

// In-memory profile cache: { userId: { ts, profile } }
const _profileCache = new Map();
const PROFILE_TTL_MS = 60_000;  // 60 seconds

/** Call this whenever user settings change so next message gets fresh profile */
function invalidateProfileCache(userId) {
  _profileCache.delete(userId);
}

async function buildProfile(user) {
  const cached = _profileCache.get(user.id);
  if (cached && (Date.now() - cached.ts) < PROFILE_TTL_MS) return cached.profile;
  // (profile built below, cached at end of function)
  // Services from services table
  const { rows: svcs } = await pool.query(
    "SELECT name, price, duration_min FROM services WHERE user_id = $1 AND is_active = true ORDER BY sort_order",
    [user.id]
  );

  // Parse schedule_data for availability + chairs
  let availability = {};
  let chairs = 1;
  try {
    if (user.schedule_data) {
      const raw = typeof user.schedule_data === "string"
        ? JSON.parse(user.schedule_data)
        : user.schedule_data;
      // _chairs stored in schedule root
      if (raw._chairs) { chairs = parseInt(raw._chairs) || 1; }
      // Build availability structure expected by computeAvailability
      // schedule_data stores { "YYYY-MM-DD": { type, slots:[{from,to}] }, _chairs }
      const dates = {}, slots = {};
      for (const [key, val] of Object.entries(raw)) {
        if (key.startsWith('_') || !key.match(/^\d{4}-\d{2}-\d{2}$/)) continue;
        if (val?.type === 'open' && val?.slots?.length) {
          dates[key] = 'available';
          slots[key] = val.slots.map(s => ({ start: s.from, end: s.to }));
        }
      }
      availability = { dates, slots };
    }
  } catch (e) { console.error("[buildProfile] schedule parse:", e.message); }

  // Read advance payment settings from direct DB columns (set by dashboard)
  let advanceAmount  = parseFloat(user.advance_amount  || 0) || 0;
  let paymentEnabled = !!(user.payment_enabled);
  let upiId          = user.upi_id      || "";
  let upiQrCode      = user.upi_qr_code || "";
  // Fallback: if direct columns empty, check onboarding_data (legacy path)
  if (!advanceAmount || !upiId) {
    try {
      const ob = user.onboarding_data
        ? (typeof user.onboarding_data === "string"
            ? JSON.parse(user.onboarding_data)
            : user.onboarding_data)
        : {};
      if (!advanceAmount)  advanceAmount  = parseFloat(ob.step3?.advance_amount || ob.step4?.advance_amount || "0") || 0;
      if (!paymentEnabled) paymentEnabled = !!(ob.step4?.payment_enabled);
      if (!upiId)          upiId          = ob.step4?.upi_id || "";
    } catch {}
  }

  // Read bot_settings: language, ignore_schedule
  let botLang        = "auto";
  let ignoreSchedule = false;
  try {
    const bs = user.bot_settings
      ? (typeof user.bot_settings === "string" ? JSON.parse(user.bot_settings) : user.bot_settings)
      : {};
    if (bs.language)         botLang        = bs.language;
    if (bs.ignore_schedule)  ignoreSchedule = true;
  } catch {}

  // If ignore_schedule, treat every day as open 9am-9pm (realistic salon hours)
  // Using 00:00-23:59 confused the AI into showing "midnight slots" to customers
  if (ignoreSchedule) {
    const today = todayIST();
    const dates = {}, slots = {};
    for (let i = 0; i < 30; i++) {
      const dt  = new Date(today + "T00:00:00+05:30");
      dt.setDate(dt.getDate() + i);
      const key = dt.toLocaleDateString("sv-SE");
      dates[key] = "available";
      slots[key] = [{ start: "09:00", end: "21:00" }];
    }
    availability = { dates, slots };
  }

  const _profile = {
    business_name:       user.salon_name || "Salon",
    location:            user.address    || "",
    language:            botLang,
    services_list:       svcs,
    availability,
    chairs,
    ignore_schedule:     ignoreSchedule,
    custom_instructions: user.whatsapp_instructions || "",
    blocked_numbers:     (() => {
      try {
        const bs = user.bot_settings
          ? (typeof user.bot_settings === "string" ? JSON.parse(user.bot_settings) : user.bot_settings)
          : {};
        return bs.blocked_numbers || [];
      } catch { return []; }
    })(),
    advance_enabled:     paymentEnabled && advanceAmount > 0,
    advance_amount:      advanceAmount,
    upi_id:              upiId,
    upi_qr_code:         upiQrCode,
    brand_color:         user.brand_color     || "#1a1a2e",
    logo_url:            user.logo_url        || "",
    google_maps_url:     user.google_maps_url || "",
  };
  _profileCache.set(user.id, { ts: Date.now(), profile: _profile });
  return _profile;
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 — GOOGLE CALENDAR SERVICE
// ─────────────────────────────────────────────────────────────────────────────

async function getCalendarService(userId) {
  const { rows } = await pool.query(
    "SELECT wa_gcal_creds FROM users WHERE id = $1",
    [userId]
  );
  if (!rows[0]) return null;
  const creds = rows[0].wa_gcal_creds;
  if (!creds?.token) return null;

  const auth = new google.auth.OAuth2(
    creds.client_id,
    creds.client_secret,
    creds.token_uri
  );
  auth.setCredentials({
    access_token:  creds.token,
    refresh_token: creds.refresh_token,
    scope:         (creds.scopes || []).join(" "),
  });
  auth.on("tokens", async (t) => {
    if (t.refresh_token) creds.refresh_token = t.refresh_token;
    creds.token = t.access_token;
    await pool.query(
      "UPDATE users SET wa_gcal_creds = $1 WHERE id = $2",
      [JSON.stringify(creds), userId]
    );
  });
  return google.calendar({ version: "v3", auth });
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 — DATE/TIME HELPERS (IST)
// ─────────────────────────────────────────────────────────────────────────────

const TZ = CFG.timezone;

/**
 * Parse a date+time string in the salon's configured timezone.
 * Uses native JS Intl — handles IST, Gulf, and any future timezone correctly.
 */
function getTzOffset(tz) {
  const str = new Date().toLocaleString("en-US", { timeZone: tz, timeZoneName: "shortOffset" });
  const m   = str.match(/GMT([+-]\d{1,2}(?::\d{2})?)/);
  if (!m) return "+05:30";
  const raw = m[1];          // e.g. "+5:30" or "+530" or "+5"
  const [hStr, minStr = "0"] = raw.replace("+","").replace("-","").split(":");
  const sign  = raw.startsWith("-") ? -1 : 1;
  const hh    = String(Math.abs(parseInt(hStr))).padStart(2, "0");
  const mm    = String(parseInt(minStr)).padStart(2, "0");
  return `${sign < 0 ? "-" : "+"}${hh}:${mm}`;
}

function parseIST(dateStr, timeStr, tz = CFG.timezone) {
  return new Date(`${dateStr}T${timeStr}:00.000${getTzOffset(tz)}`);
}

function fmtIST(date) {
  return date.toLocaleString("sv-SE", {
    timeZone: TZ, year:"numeric", month:"2-digit", day:"2-digit",
    hour:"2-digit", minute:"2-digit",
  }).replace(" ", " ").slice(0, 16);
}

function todayIST() {
  return new Date().toLocaleDateString("sv-SE", { timeZone: TZ });
}

function nowTimeIST() {
  return new Date().toLocaleTimeString("en-GB", {
    timeZone: TZ, hour: "2-digit", minute: "2-digit",
  });
}

/**
 * Human-like send: mark as read → composing indicator → delay → send message.
 * Makes every bot message feel like a real person typing.
 * @param {string} jid - recipient JID
 * @param {string} text - message text
 * @param {object} [msgKey] - message key for read receipt (optional)
 * @param {string} [lang] - language for delay calibration
 */
async function humanSend(jid, text, msgKey = null, lang = "hinglish") {
  const liveSock = sock;
  if (!liveSock) return;

  // 1. Mark as read (blue ticks)
  try {
    if (msgKey?.id) {
      await liveSock.readMessages([{
        remoteJid: jid, id: msgKey.id,
        fromMe: false, participant: msgKey.participant || undefined,
      }]);
    }
  } catch {}

  // 2. Short pause before composing (person reads message first)
  await sleep(humanDelay(600, 1400));

  // 3. Show composing indicator
  try { await liveSock.sendPresenceUpdate("composing", jid); } catch {}

  // 4. Typing delay proportional to message length
  const typingMs = typingDuration(text);
  await sleep(typingMs);

  // 5. Stop composing and send
  try { await liveSock.sendPresenceUpdate("paused", jid); } catch {}
  await sleep(humanDelay(100, 300));
  await sendSafe(jid, text);
}

/** Convert HH:MM (24h) to natural "2:30 PM" format */
function friendlyTime12(t) {
  if (!t) return t;
  const [hh, mm] = t.split(":").map(Number);
  const suffix = hh >= 12 ? "PM" : "AM";
  const h12    = hh > 12 ? hh - 12 : (hh === 0 ? 12 : hh);
  return mm === 0 ? `${h12} ${suffix}` : `${h12}:${String(mm).padStart(2,"0")} ${suffix}`;
}

function friendlyDate(dateStr) {
  return new Date(`${dateStr}T00:00:00+05:30`).toLocaleDateString("en-IN", {
    timeZone: TZ, weekday:"long", day:"numeric", month:"long", year:"numeric",
  });
}

function prunePastDates(avail = {}) {
  if (!avail.dates) return { dates: {}, slots: {} };
  const today = todayIST();
  const dates = {}, slots = {};
  for (const d in avail.dates) {
    if (d >= today) {
      dates[d] = avail.dates[d];
      if (avail.slots?.[d]) slots[d] = avail.slots[d];
    }
  }
  return { dates, slots };
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7 — AVAILABILITY ENGINE
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Compute truly available time windows.
 * Respects chairs (concurrent bookings): a slot is only blocked when
 * all chairs are occupied at that time.
 * @param {object} availability  { dates, slots } from buildProfile
 * @param {Array}  bookings      existing appointments with .timespan
 * @param {number} bufferMin     gap to add after each booking
 * @param {number} chairs        max concurrent bookings (default 1)
 */
function computeAvailability(availability, bookings = [], bufferMin = 0, chairs = 1) {
  if (!availability?.slots) return { dates: {}, slots: {} };
  const result = { dates: {}, slots: {} };
  const STEP = 15 * 60_000;  // 15-min resolution

  for (const date in availability.slots) {
    const windowStart = parseIST(date, availability.slots[date][0]?.start || "00:00").getTime();
    const windowEnd   = parseIST(date, availability.slots[date].slice(-1)[0]?.end || "23:59").getTime();

    // Collect busy intervals for this date
    const dayBookings = bookings.filter(b => b.timespan?.startsWith(date));
    const busyIntervals = dayBookings.map(b => {
      const [startStr, endStr] = b.timespan.split(" - ");
      const s = parseIST(date, startStr.split(" ")[1]).getTime();
      let   e = parseIST(date, endStr.split(" ")[1]  ).getTime();
      if (bufferMin > 0) e += bufferMin * 60_000;
      return { s, e };
    });

    // For each 15-min step, count how many bookings overlap it
    // Slot is available if count < chairs
    let freeRanges = [];
    let inFree = false, freeStart = 0;

    for (let t = windowStart; t < windowEnd; t += STEP) {
      const count = busyIntervals.filter(b => b.s < t + STEP && b.e > t).length;
      const avail = count < chairs;

      if (avail && !inFree)       { inFree = true;  freeStart = t; }
      if (!avail && inFree)       { inFree = false;  freeRanges.push({ start: freeStart, end: t }); }
    }
    if (inFree) freeRanges.push({ start: freeStart, end: windowEnd });

    // Merge the working-hours windows with free ranges
    const workRanges = availability.slots[date].map(s => ({
      start: parseIST(date, s.start).getTime(),
      end:   parseIST(date, s.end  ).getTime(),
    }));

    // Intersect freeRanges with workRanges
    const finalSlots = [];
    for (const fr of freeRanges) {
      for (const wr of workRanges) {
        const s = Math.max(fr.start, wr.start);
        const e = Math.min(fr.end,   wr.end);
        if (e - s >= 15 * 60_000) {
          finalSlots.push({
            start: new Date(s).toLocaleTimeString("en-GB", { timeZone: TZ, hour:"2-digit", minute:"2-digit" }),
            end:   new Date(e).toLocaleTimeString("en-GB", { timeZone: TZ, hour:"2-digit", minute:"2-digit" }),
          });
        }
      }
    }

    if (finalSlots.length) {
      result.slots[date] = finalSlots;
      result.dates[date] = "available";
    }
  }
  return result;
}

function buildAvailabilityChart(avail, lookahead = CFG.lookaheadDays) {
  if (!avail?.dates || !Object.keys(avail.dates).length)
    return ["No slots configured yet."];

  const today  = todayIST();
  const cutoff = new Date(`${today}T00:00:00+05:30`);
  cutoff.setDate(cutoff.getDate() + lookahead);
  const cutStr = cutoff.toLocaleDateString("sv-SE", { timeZone: TZ });

  return Object.keys(avail.dates)
    .filter(d => d >= today && d <= cutStr).sort()
    .map(d => {
      const slots = avail.slots[d];
      if (!slots?.length) return null;
      return `${friendlyDate(d)}: ${slots.map(s => `${s.start}-${s.end}`).join(", ")}`;
    }).filter(Boolean);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 — LANGUAGE DETECTION
// ─────────────────────────────────────────────────────────────────────────────

function detectLanguage(text) {
  if (/[\u0900-\u097F]/.test(text)) return "hindi";
  // Common Hinglish words + Indian SMS shorthand (kl=kal, h=hai, bje=baje, etc.)
  if (/\b(kya|mujhe|mera|aapka|bhai|yaar|haan|nahi|karo|chahiye|booking|balo|facial|wax|salon|kal|kl|aaj|kal|bje|baj|baje|aunga|aana|krna|krwana|karana|theek|thik|accha|achha|acha|bilkul|zaroor|bas|sirf|abhi|jaldi|thoda|bahut|kitna|kab|kaise|kahan|milte|milna|dena|lena|karein|batao|batana|chahte|chahiye|price|kitne|paise|rupaye|paisa|time|slot|appointment|salon|haircut|baal|hair|cut|wax|massage|facial)\b/i.test(text))
    return "hinglish";
  // Short messages with numbers like "3 pm", "kl 2 bje" are almost always hinglish
  if (/\b(pm|am|bje|baj)\b/i.test(text) && text.length < 50) return "hinglish";
  return "english";
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 — OPENAI TOOLS
// ─────────────────────────────────────────────────────────────────────────────

const SALON_TOOLS = [
  {
    type: "function",
    function: {
      name: "check_availability",
      description: "Check available appointment slots for a date.",
      parameters: {
        type: "object",
        properties: {
          date: { type: "string", description: "Date YYYY-MM-DD" },
        },
        required: ["date"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_services",
      description: "Return salon service menu with prices.",
      parameters: { type: "object", properties: {}, required: [] },
    },
  },
  {
    type: "function",
    function: {
      name: "book_appointment",
      description: "Book appointment ONLY after customer confirms name, service, date, time, duration.",
      parameters: {
        type: "object",
        properties: {
          customer_name: { type: "string" },
          service:       { type: "string" },
          date:          { type: "string", description: "YYYY-MM-DD" },
          time:          { type: "string", description: "HH:MM 24h" },
          duration_min:  { type: "integer" },
          notes:         { type: "string" },
        },
        required: ["customer_name", "service", "date", "time", "duration_min"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "reschedule_appointment",
      description: "Reschedule an existing appointment.",
      parameters: {
        type: "object",
        properties: {
          appointment_id:   { type: "string" },
          new_date:         { type: "string" },
          new_time:         { type: "string" },
          new_duration_min: { type: "integer" },
        },
        required: ["appointment_id", "new_date", "new_time"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "cancel_appointment",
      description: "Cancel an appointment after confirmation.",
      parameters: {
        type: "object",
        properties: {
          appointment_id: { type: "string" },
        },
        required: ["appointment_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_my_appointments",
      description: "List customer's upcoming appointments.",
      parameters: { type: "object", properties: {}, required: [] },
    },
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 — TOOL EXECUTORS
// ─────────────────────────────────────────────────────────────────────────────

async function execCheckAvailability({ date }, ctx) {
  const { user, p, lang } = ctx;

  // In accept-anytime mode: confirm availability but still enforce chair capacity
  if (p.ignore_schedule) {
    const chairs    = p.chairs || 1;
    const requested = ctx.requestedTime || null;
    if (requested) {
      // Check if all chairs are occupied at the exact requested time
      const activeAtTime = getAppointments(user).filter(b =>
        b.date === date && b.time === requested &&
        b.payment_status !== "cancelled" && b.payment_status !== "cancelled_paid" &&
        b.payment_status !== "payment_timeout"
      );
      if (activeAtTime.length >= chairs) {
        if (lang === "hindi")    return `${requested} pe saari ${chairs > 1 ? chairs + " seats" : "seat"} full hai. Koi aur time batayein?`;
        if (lang === "hinglish") return `Yaar ${requested} pe sab chairs full hain. Koi aur time?`;
        return `All ${chairs} chair${chairs > 1 ? "s are" : " is"} booked at ${requested}. Please choose another time.`;
      }
    }
    if (lang === "hindi")
      return `${friendlyDate(date)} ko appointment available hai. Kaunsa time chahiye aapko?`;
    if (lang === "hinglish")
      return `${friendlyDate(date)} pe booking available hai bhai! Kaunsa time suit karega?`;
    return `${friendlyDate(date)} is available for booking. What time works for you?`;
  }

  const baseAvail    = prunePastDates(p.availability || {});
  const avail        = computeAvailability(baseAvail, [...getAppointments(user), ...ctx.gcalBookings], CFG.bufferMinutes);
  const slotsForDate = avail.slots[date];

  if (!slotsForDate?.length) {
    if (lang === "hindi")    return `${friendlyDate(date)} ko koi slot available nahi hai. Koi aur din dekhein?`;
    if (lang === "hinglish") return `${friendlyDate(date)} pe koi time nahi bhai. Koi doosra din?`;
    return `No slots available on ${friendlyDate(date)}. Want to check another day?`;
  }

  // Format slots as natural times, not raw ranges
  const fmtSlot = s => {
    const [h, m] = s.start.split(":").map(Number);
    const suffix = h >= 12 ? "PM" : "AM";
    const h12    = h > 12 ? h - 12 : (h === 0 ? 12 : h);
    return m === 0 ? `${h12} ${suffix}` : `${h12}:${m.toString().padStart(2,"0")} ${suffix}`;
  };
  const times = slotsForDate.map(fmtSlot).join(", ");
  if (lang === "hindi")    return `${friendlyDate(date)} pe yeh time available hai: ${times}`;
  if (lang === "hinglish") return `${friendlyDate(date)} pe ye slots free hain: ${times}`;
  return `Available on ${friendlyDate(date)}: ${times}`;
}

async function execGetServices({}, ctx) {
  const { p, lang } = ctx;
  if (!p.services_list?.length) {
    if (lang === "hindi")    return "Services list configure nahi hui. Seedha contact karein.";
    if (lang === "hinglish") return "Bhai services abhi nahi hain. Directly poocho.";
    return "Services not configured yet. Please ask directly.";
  }
  const lines = p.services_list.map(s =>
    `• ${s.name}${s.duration_min ? ` (${s.duration_min}min)` : ""}${s.price ? ` — ₹${s.price}` : ""}`
  ).join("\n");
  if (lang === "hindi")    return `Hamare services:\n${lines}\n\nKaunsi chahiye?`;
  if (lang === "hinglish") return `Ye hain hamare services:\n${lines}\n\nKya book karna hai?`;
  return `Our services:\n${lines}\n\nWhat would you like to book?`;
}

async function execBookAppointment(args, ctx) {
  const { user, p, phone, remoteJid, lang } = ctx;
  const { service, date, time, duration_min, notes = "" } = args;
  // Use AI-collected name first; fall back to WhatsApp profile name if available
  const customer_name = (args.customer_name || "").trim() || ctx.customerName || "";
  if (!customer_name) {
    if (lang === "hindi")    return "Aapka naam batayein please.";
    if (lang === "hinglish") return "Bhai apna naam batao pehle.";
    return "Please share your name to complete the booking.";
  }

  // Per-customer upcoming booking limit (prevent calendar flooding)
  const today0 = todayIST();
  const customerUpcoming = getAppointments(user).filter(b =>
    b.phone === phone && b.date >= today0 &&
    b.payment_status !== "cancelled" && b.payment_status !== "cancelled_paid"
  );
  const MAX_UPCOMING = 3;
  if (customerUpcoming.length >= MAX_UPCOMING) {
    const existing = customerUpcoming.sort((a,b) => a.date.localeCompare(b.date))
      .map(b => `• ${b.service} — ${friendlyDate(b.date)} at ${friendlyTime12(b.time)}`).join("\n");
    if (lang === "hindi")    return `Aapki already ${MAX_UPCOMING} upcoming bookings hain:\n${existing}\n\nPehle ek cancel karein, phir naya book karein.`;
    if (lang === "hinglish") return `Bhai already ${MAX_UPCOMING} bookings hain:\n${existing}\n\nEk cancel karo pehle.`;
    return `You already have ${MAX_UPCOMING} upcoming bookings:\n${existing}\n\nPlease cancel one before making a new booking.`;
  }

  // Double-booking check
  const baseAvail = prunePastDates(p.availability || {});
  const avail     = computeAvailability(baseAvail, [...getAppointments(user), ...ctx.gcalBookings], CFG.bufferMinutes, ctx.p?.chairs || 1);
  const reqStart  = parseIST(date, time).getTime();
  const reqEnd    = reqStart + duration_min * 60_000;

  // Prevent booking in the past (including earlier today)
  const nowMs = Date.now();
  if (reqStart < nowMs - 5 * 60_000) {
    const fdl2 = friendlyDate(date);
    const ftm2 = friendlyTime12(time);
    if (lang === "hindi")    return `${fdl2} ${ftm2} ka time already nikal gaya hai. Koi aage ka time batayein.`;
    if (lang === "hinglish") return `Bhai ${ftm2} toh pehle ho chuka hai! Koi future time batao.`;
    return `${ftm2} on ${fdl2} has already passed. Please choose a future time.`;
  }

  const slotOk    = (avail.slots[date] || []).some(s => {
    const ss = parseIST(date, s.start).getTime();
    const se = parseIST(date, s.end  ).getTime();
    return reqStart >= ss && reqEnd <= se;
  });

  if (!slotOk) {
    const fdl = friendlyDate(date);
    const ftm = friendlyTime12(time);
    if (lang === "hindi")    return `${ftm} ka slot ${fdl} pe available nahi hai. Koi aur time batayein.`;
    if (lang === "hinglish") return `Yaar ${ftm} wala slot ${fdl} pe nahi milega. Koi aur time?`;
    return `Sorry, ${ftm} on ${fdl} is not available. Please choose another time.`;
  }

  const startDt = new Date(reqStart), endDt = new Date(reqEnd);
  const id      = uuid();
  const appt    = {
    id, customer_name, service, date, time, duration_min, notes, phone, remoteJid,
    timespan:  `${fmtIST(startDt)} - ${fmtIST(endDt)}`,
    timestamp: new Date().toISOString(),
    remindersSent: {},
  };

  // Google Calendar
  const calSvc = await getCalendarService(user.id);
  if (calSvc) {
    try {
      const created = await calSvc.events.insert({
        calendarId: "primary",
        resource: {
          summary:     `${service} — ${customer_name}`,
          description: `Customer: ${phone}\nNotes: ${notes}`,
          location:    p.location || "",
          start: { dateTime: startDt.toISOString(), timeZone: TZ },
          end:   { dateTime: endDt.toISOString(),   timeZone: TZ },
        },
      });
      appt.calendarEventId = created.data.id;
    } catch (e) { console.error("[GCal] create:", e.message); }
  }

  // Set payment_status before saving so image handler can find it as "pending"
  if (p.advance_enabled && p.advance_amount > 0) {
    appt.payment_status = "pending";
    appt.advance_amount  = p.advance_amount;
  }

  // Atomic save: lock the row and append
  await atomicUpdateJsonb(user.id, "wa_appointments", (appts) => {
    appts.push(appt);
    return appts;
  });

  const dl = friendlyDate(date);

  // ── Advance payment: send premium branded QR code only ─────────────────────
  if (p.advance_enabled && p.advance_amount > 0) {
    const amt = `₹${p.advance_amount}`;
    const upi = p.upi_id || "";

    // Generate and send QR code immediately (fire-and-forget)
    if (upi) {
      (async () => {
        try {
          const upiLink = `upi://pay?pa=${encodeURIComponent(upi)}&pn=${encodeURIComponent(p.business_name)}&am=${p.advance_amount}&cu=INR`;
          const imgBuf  = await generatePremiumQR(upiLink, p.brand_color);
          if (!imgBuf) { console.error("[QR-SEND] null buffer — skipping"); return; }

          const caption = lang === "hindi"
            ? `💳 *${p.business_name}*\n💰 *${amt}* bhejein\nUPI: *${upi}*\n\n📸 Payment ka screenshot bhejein — booking confirm ho jaayegi!\n⏱ Slot *10 min* ke liye hold hai.`
            : lang === "hinglish"
            ? `💳 *${p.business_name}*\n💰 *${amt}* bhejo\nUPI: *${upi}*\n\n📸 Screenshot bhejo — booking confirm ho jaayegi!\n⏱ Slot *10 min* ke liye hold hai.`
            : `💳 *${p.business_name}*\n💰 Pay *${amt}*\nUPI: *${upi}*\n\n📸 Send payment screenshot to confirm.\n⏱ Slot held for *10 minutes*.`;

          // Wait for slot-reserved text to arrive first
          await sleep(humanDelay(2000, 3000));

          // Use live global sock at send time (most reliable — handles reconnects)
          const liveSock = sock;
          if (!liveSock) { console.error("[QR-SEND] sock null at send time"); return; }
          await liveSock.sendMessage(remoteJid, { image: imgBuf, caption });
          console.log(`[QR-SEND] ✅ QR sent to ${remoteJid}`);
        } catch (e) {
          console.error("[QR-SEND] failed:", e.message, e.stack?.split("\n")[1] || "");
        }
      })();
    }
  }

  // Send slot confirmation text DIRECTLY via sendSafe — bypasses AI entirely
  // This guarantees correct order: text first, QR 2-3s later
  const amt = p.advance_amount ? `₹${p.advance_amount}` : "";
  const confirmationText = p.advance_enabled
    ? (lang === "hindi"
        ? `📋 *Slot Reserve Hua!*\n*Naam:* ${customer_name} | *${service}*\n*Date:* ${dl}, ${friendlyTime12(time)}\n⏳ QR code aa raha hai — payment karo confirm karne ke liye. 🙏`
        : lang === "hinglish"
        ? `📋 *Slot Reserve!*\n${customer_name} | *${service}*\n${dl}, ${friendlyTime12(time)}\n⏳ QR code aa raha hai — pay karo confirm karne ke liye. ✅`
        : `📋 *Slot Reserved* — ${customer_name} | ${service} | ${dl} at ${friendlyTime12(time)}\n⏳ QR code on its way — scan to pay ${amt} & confirm.`)
    : (lang === "hindi"
        ? `✅ *Booking Confirm!*\n*Naam:* ${customer_name} | *${service}*\n*Date:* ${dl}, ${friendlyTime12(time)}\nAapka intezaar rahega! 🙏`
        : lang === "hinglish"
        ? `✅ *Pakki ho gayi!*\n${customer_name} | *${service}*\n${dl}, ${friendlyTime12(time)}\nMilte hain! 😊`
        : `✅ *Confirmed!* — ${customer_name} | ${service} | ${dl} at ${friendlyTime12(time)}\nSee you! 🙏`);

  // Send slot confirmation with human-like typing indicator
  ;(async () => {
    try {
      const liveSock = sock;
      if (!liveSock) return;
      // Show composing (person is writing the confirmation)
      try { await liveSock.sendPresenceUpdate("composing", remoteJid); } catch {}
      await sleep(humanDelay(800, 1600));  // typing the confirmation
      try { await liveSock.sendPresenceUpdate("paused", remoteJid); } catch {}
      await sleep(humanDelay(80, 200));
      await sendSafe(remoteJid, confirmationText);
    } catch (e) { console.error("[BOOKING-TEXT]", e.message); }
  })();

  // Return a minimal string so the AI tool result is non-empty but
  // the second OpenAI call gets suppressed (single tool call + finalReply set)
  return "__BOOKING_DONE__";
}

async function execReschedule(args, ctx) {
  const { user, p, lang } = ctx;
  const { appointment_id, new_date, new_time, new_duration_min } = args;
  let books = getAppointments(user);
  const idx = books.findIndex(b => b.id === appointment_id);
  if (idx === -1) {
    if (lang === "hindi")    return "Appointment ID nahi mili. Sahi ID daalein.";
    if (lang === "hinglish") return "Bhai yeh ID nahi mila. Sahi ID daalein.";
    return "Appointment not found. Please check the ID and try again.";
  }
  // Ownership check — customer can only reschedule their own booking
  if (books[idx].phone && ctx.phone && books[idx].phone !== ctx.phone) {
    if (lang === "hindi")    return "Aap sirf apni booking reschedule kar sakte hain.";
    if (lang === "hinglish") return "Bhai sirf apni booking badal sakte ho.";
    return "You can only reschedule your own appointments.";
  }

  const orig   = books[idx];
  const durMin = new_duration_min || orig.duration_min;
  const others = books.filter(b => b.id !== appointment_id);
  const avail  = computeAvailability(prunePastDates(p.availability || {}), [...others, ...ctx.gcalBookings], CFG.bufferMinutes, p.chairs || 1);
  const rs     = parseIST(new_date, new_time).getTime();
  const re     = rs + durMin * 60_000;
  const ok     = (avail.slots[new_date] || []).some(s => {
    const ss = parseIST(new_date, s.start).getTime();
    const se = parseIST(new_date, s.end  ).getTime();
    return rs >= ss && re <= se;
  });

  if (!ok) {
    const friendlyNewDate = friendlyDate(new_date);
    const friendlyNewTime = friendlyTime12(new_time);
    if (lang === "hindi")    return `${friendlyNewDate} ko ${friendlyNewTime} ka slot nahi hai. Koi aur time batayein?`;
    if (lang === "hinglish") return `${friendlyNewDate} pe ${friendlyNewTime} available nahi yaar. Koi doosra time?`;
    return `${friendlyNewTime} on ${friendlyNewDate} is not available. Want to try another time?`;
  }

  const sd = new Date(rs), ed = new Date(re);
  books[idx] = { ...orig, date: new_date, time: new_time, duration_min: durMin,
    timespan: `${fmtIST(sd)} - ${fmtIST(ed)}`, timestamp: new Date().toISOString(), remindersSent: {} };

  if (orig.calendarEventId) {
    const calSvc = await getCalendarService(user.id);
    if (calSvc) try {
      await calSvc.events.patch({ calendarId:"primary", eventId: orig.calendarEventId,
        resource: { start:{dateTime:sd.toISOString(),timeZone:TZ}, end:{dateTime:ed.toISOString(),timeZone:TZ} } });
    } catch (e) { console.error("[GCal] update:", e.message); }
  }

  await saveAppointments(user.id, books);
  const dl = friendlyDate(new_date);
  if (lang === "hindi")    return `✅ Reschedule ho gaya!\n*Nayi date:* ${dl}\n*Time:* ${friendlyTime12(new_time)}`;
  if (lang === "hinglish") return `✅ Done! Nayi booking: ${dl} at ${friendlyTime12(new_time)}`;
  return `✅ Rescheduled to ${dl} at ${friendlyTime12(new_time)}`;
}

async function execCancel(args, ctx) {
  const { user, lang } = ctx;
  let books = getAppointments(user);
  const idx = books.findIndex(b => b.id === args.appointment_id);
  if (idx === -1) {
    if (lang === "hindi")    return "Appointment nahi mili. Sahi ID check karein.";
    if (lang === "hinglish") return "Bhai yeh booking nahi mili. ID sahi hai?";
    return "Appointment not found. Please check the ID and try again.";
  }
  // Ownership check — customer can only cancel their own booking
  if (books[idx].phone && ctx.phone && books[idx].phone !== ctx.phone) {
    if (lang === "hindi")    return "Aap sirf apni booking cancel kar sakte hain.";
    if (lang === "hinglish") return "Bhai sirf apni booking cancel kar sakte ho.";
    return "You can only cancel your own appointments.";
  }

  const appt = books.splice(idx, 1)[0];
  appt.payment_status = appt.payment_status === "paid" ? "cancelled_paid" : "cancelled";
  // Keep in array for records but mark cancelled (don't push back — already spliced)
  books.push({ ...appt });  // keep for history
  await saveAppointments(user.id, books);

  if (appt.calendarEventId) {
    const calSvc = await getCalendarService(user.id);
    if (calSvc) try {
      await calSvc.events.delete({ calendarId:"primary", eventId: appt.calendarEventId });
    } catch (e) { console.error("[GCal] delete:", e.message); }
  }

  const dl = friendlyDate(appt.date);
  const paidNote = appt.payment_status === "cancelled_paid"
    ? (lang === "hindi"    ? "\n\n⚠️ Aapne advance payment ki thi. Refund ke liye salon se seedha contact karein."
       : lang === "hinglish" ? "\n\n⚠️ Advance pay kiya tha. Refund ke liye salon se contact karo."
       : "\n\n⚠️ You had paid an advance. Please contact the salon directly for a refund.")
    : "";
  if (lang === "hindi")    return `✅ ${dl} wala appointment cancel ho gaya.${paidNote}`;
  if (lang === "hinglish") return `✅ ${dl} wali booking cancel ho gayi bhai.${paidNote}`;
  return `✅ Your appointment on ${dl} has been cancelled.${paidNote}`;
}

async function execGetMyAppointments({}, ctx) {
  const { user, phone, lang } = ctx;
  const today    = todayIST();
  // Show ALL upcoming — including pending (unpaid) so customer knows slot is reserved
  // Exclude only permanently cancelled/timed-out ones
  const upcoming = getAppointments(user).filter(b =>
    b.phone === phone &&
    b.date >= today &&
    b.payment_status !== "cancelled" &&
    b.payment_status !== "cancelled_paid" &&
    b.payment_status !== "payment_timeout"
  );
  if (!upcoming.length) {
    if (lang === "hindi")    return "Koi upcoming appointment nahi hai.";
    if (lang === "hinglish") return "Bhai koi booking nahi hai.";
    return "No upcoming appointments.";
  }
  const lines = upcoming.sort((a,b)=>a.date.localeCompare(b.date))
    .map(b => {
      const statusTag =
        b.payment_status === "pending"      ? (lang === "hinglish" ? " ⏳ Payment pending" : lang === "hindi" ? " ⏳ Payment baaki" : " ⏳ Awaiting payment") :
        b.payment_status === "paid"         ? " ✅" :
        b.payment_status === "wrong_amount" ? " ⚠️ Wrong amount" :
        "";
      return `• *${b.service}* — ${friendlyDate(b.date)} at ${friendlyTime12(b.time)}${statusTag} (ID: \`${b.id}\`)`;
    });
  if (lang === "hindi")    return `Aapki upcoming bookings:\n${lines.join("\n")}`;
  if (lang === "hinglish") return `Teri bookings:\n${lines.join("\n")}`;
  return `Your appointments:\n${lines.join("\n")}`;
}

async function executeTool(name, args, ctx) {
  switch (name) {
    case "check_availability":     return execCheckAvailability(args, ctx);
    case "get_services":           return execGetServices(args, ctx);
    case "book_appointment":       return execBookAppointment(args, ctx);
    case "reschedule_appointment": return execReschedule(args, ctx);
    case "cancel_appointment":     return execCancel(args, ctx);
    case "get_my_appointments":    return execGetMyAppointments(args, ctx);
    default: {
      if (lang === "hindi")    return "Yeh action abhi available nahi hai. Koi aur kaam batayein.";
      if (lang === "hinglish") return "Yaar yeh nahi ho sakta. Kuch aur batao kya chahiye.";
      return "This action isn't available right now. How else can I help?";
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11 — SYSTEM PROMPT
// ─────────────────────────────────────────────────────────────────────────────

function buildSystemPrompt(user, p, availChart, userAppts, lang, customerName = null) {
  const todayFull = new Date().toLocaleDateString("en-IN", {
    timeZone: TZ, weekday:"long", day:"numeric", month:"long", year:"numeric"
  });
  const todayKey = todayIST();
  const now = nowTimeIST();

  // Effective language: profile setting overrides per-message detection
  //   auto = use detected lang, otherwise use forced lang
  const effectiveLang = (p.language && p.language !== "auto") ? p.language : lang;

  const langInstruction =
    effectiveLang === "hindi"    ? "Sirf Hindi mein jawab dena (Devanagari script). Har message Hindi mein ho." :
    effectiveLang === "hinglish" ? "Hinglish mein jawab dena (Roman Hindi + English mix). Friendly aur casual raho." :
                                   "Reply in clear, friendly English only.";

  const servicesList = (p.services_list || [])
    .map(s => {
      const price = s.price ? `₹${s.price}` : "";
      const dur   = s.duration_min ? `${s.duration_min}min` : "";
      const adv   = (p.advance_enabled && s.price && p.advance_amount)
        ? ` (₹${p.advance_amount} advance, baaki ${s.price > p.advance_amount ? "₹"+(s.price-p.advance_amount)+" baad mein" : "adjust hoga"})`
        : "";
      return `• ${s.name} — ${price}${dur ? " · "+dur : ""}${adv}`;
    })
    .join("\n") || "No services listed — ask salon owner to add services";

  // Extended availability context: past 10 days (for reference) + next 10 days
  const windowDays  = [];
  for (let i = -10; i <= 10; i++) {
    const dt  = new Date(todayKey + "T00:00:00+05:30");
    dt.setDate(dt.getDate() + i);
    windowDays.push(dt.toLocaleDateString("sv-SE"));
  }

  const slotsText = p.ignore_schedule
    ? "ACCEPT ANYTIME MODE: Salon accepts bookings any day, 9 AM to 9 PM. Do NOT list time ranges to the customer. When customer requests a specific time, directly confirm that time and proceed to book."
    : availChart.length
      ? availChart.join("\n")
      : "No availability configured — ask owner to set schedule";

  const apptText = userAppts.length
    ? userAppts.map(b => {
        const ps = b.payment_status === "paid" ? " ✅ Paid"
                 : b.payment_status === "pending" ? " ⏳ Awaiting payment"
                 : "";
        return `ID:${b.id} | ${b.service} | ${friendlyDate(b.date)} at ${friendlyTime12(b.time)}${ps}`;
      }).join("\n")
    : "None";

  // Date range context for the AI
  const windowStart = windowDays[0];
  const windowEnd   = windowDays[windowDays.length - 1];

  const nameGreeting = customerName
    ? `
CUSTOMER NAME: ${customerName}. Address them as "${customerName} ji" in Hindi/Hinglish, or "${customerName}" in English. Use their name naturally once or twice — don't overdo it.`
    : "";

  return [
    `You are the AI booking assistant for *${p.business_name}*, an Indian salon/beauty parlour.`,
    `Today: ${todayFull} (${todayKey}), current time ${now} IST (India Standard Time, UTC+5:30).`,
    `Tomorrow is ${(() => { const d=new Date(todayKey+"T00:00:00+05:30"); d.setDate(d.getDate()+1); return d.toLocaleDateString("sv-SE"); })()}.`,
    `IMPORTANT: Always use IST timezone. "Kal"/"tomorrow" = the date shown above as Tomorrow. Never use server local time.`,
    `Location: ${p.location || "India"}.${p.google_maps_url ? " Google Maps: " + p.google_maps_url : ""}`,
    nameGreeting,
    ``,
    `SERVICES & PRICING:`,
    servicesList,
    ``,
    `AVAILABILITY WINDOW: ${windowStart} to ${windowEnd}`,
    `Available slots:`,
    slotsText,
    ``,
    `THIS CUSTOMER'S UPCOMING BOOKINGS:`,
    apptText,
    ``,
    `BOOKING RULES:`,
    `- NEVER say "please wait", "thoda intezaar", "booking kar raha hoon" or any variation. Call the tool immediately and respond with the result directly.`,
    `- NEVER mention QR code, UPI ID, or payment in your text — these are sent automatically as a separate image by the system.`,
    `- NEVER say "booking ho rahi hai" or "main book kar raha hoon" — the booking either succeeded or failed, report the result only.`,
    `- BOOKING FLOW (follow exactly):
  STEP 1: Collect name, service, date, time (ask only what is missing).
  STEP 2: Once you have all 4, show a short summary and ask for confirmation. Example:
    Hinglish: "Theek hai! Haircut kal 4 baje — Rs.100. Kya main book kar dun? 😊"
    Hindi: "Bilkul! Haircut kal 4 baje ke liye. Confirm kar dun?"
    English: "Great! Haircut on [date] at [time]. Shall I book this for you?"
  STEP 3: When customer replies yes/haan/ha/ok/bilkul/done/book/confirm/zaroor/please → call book_appointment IMMEDIATELY.
  STEP 4: After tool returns, send the result (slot reserved + QR coming).`,
    `- At STEP 2 ask ONE short question only. Do not re-ask unless customer changes something.`,
    `- If customer replies with a change (different time/date/service) at STEP 3 → update and re-ask confirmation.`,
    p.ignore_schedule
      ? `- ACCEPT ANYTIME MODE is ON. When customer gives service+date+time (and name is known from CUSTOMER NAME above), show a short confirmation summary and ask "Kya main book kar dun?" or similar. When they say yes → call book_appointment. If all 4 are in a single message like "kal 2 bje haircut", jump straight to asking confirmation — no need to re-collect anything.`
      : `- Use check_availability tool to verify the slot before booking.`,
    `- If a requested date is beyond ${windowEnd}, tell customer that far-future bookings are not open yet.`,
    p.ignore_schedule
      ? `- Only reject a time if another appointment already exists at that exact slot.`
      : `- If requested slot is unavailable, suggest the next 2-3 available times in natural language (e.g. "2:30 PM or 4 PM are free").`,
    `- Duration: use service default or ask. Never assume.`,
    `- Keep replies SHORT (2-4 lines max). Warm, friendly, conversational.`,
    `- NEVER say "00:00 - 23:59" or show raw time ranges to customers.`,
    `- ALWAYS convert 24-hour time to 12-hour format in your messages. "17:00" → "5 PM", "14:30" → "2:30 PM", "09:00" → "9 AM". NEVER show times like "17:00" or "14:00" to customers.`,
    `- ALWAYS show full service price before confirming. Example in Hinglish: "Haircut ₹150 hai — ₹50 advance abhi dena hoga, baaki ₹100 salon mein."`,
    `- NEVER just say "advance ₹50" without mentioning the total service price first.`,
    `- Use natural Indian expressions: "bilkul", "zaroor", "acha", "theek hai". Don't sound robotic.`,
    `- Don't ask for name if CUSTOMER NAME is already provided above — use it directly.`,
    `- Greet warmly on first message. Keep subsequent replies short and direct.`,
    `- ${langInstruction}`,
    p.advance_enabled
      ? `\nADVANCE PAYMENT RULES:
- Advance required: ₹${p.advance_amount} (full service price shown in services list above)
- After booking tool call: tell customer their slot is reserved and a QR code is on its way. DO NOT mention the UPI ID — it is shown on the QR code.
- Say something like: "Aapka slot reserve ho gaya! QR code aa raha hai, ussse pay karo." Keep it short.
- NEVER include the UPI ID (${p.upi_id}) in your text messages — it is on the QR image.
- If payment not received in 10 minutes, slot is automatically released.
- NEVER say "booking confirmed" until payment_status = "paid".`
      : "",
    p.chairs > 1
      ? `CHAIRS: Up to ${p.chairs} clients can be served simultaneously.`
      : "",
    p.custom_instructions
      ? `\nCUSTOM INSTRUCTIONS (follow exactly):\n${p.custom_instructions}`
      : "",
  ].filter(Boolean).join("\n").trim();
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 12 — MAIN AI PIPELINE
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Summarize old chat history when it grows too long.
 * Keeps the last 6 messages verbatim + a running summary.
 * Saves API costs by replacing 20+ messages with ~50 tokens of summary.
 */
async function summarizeHistoryIfNeeded(userId, sessionId, phone, allHistory) {
  const msgs = allHistory[sessionId]?.[phone] || [];
  if (msgs.length < 20) return msgs;  // only summarize when history is long

  const keepLast       = 6;
  const oldMsgs        = msgs.slice(0, msgs.length - keepLast);
  const recentMsgs     = msgs.slice(msgs.length - keepLast);
  const previousSummary = allHistory[sessionId][phone + "_summary"] || "";

  const summaryPrompt = `You are summarizing a salon booking chat for an AI assistant's memory.
Extract and keep: customer's preferred services, preferred stylist name, allergies/sensitivities, communication style, past issues, and any personal details mentioned.
Be concise (max 100 words). Write in third person.
${previousSummary ? "Previous summary:\n" + previousSummary + "\n" : ""}New messages to incorporate:
${oldMsgs.map(m => m.role + ": " + m.content).join("\n")}`;

  try {
    const resp = await openai.chat.completions.create({
      model:       "gpt-4o-mini",
      messages:    [{ role: "system", content: summaryPrompt }],
      temperature: 0.2,
      max_tokens:  150,
    });
    await trackUsage(userId, "gpt-4o-mini", resp.usage);
    const newSummary = resp.choices[0].message.content?.trim() || "";
    allHistory[sessionId][phone + "_summary"] = newSummary;
    allHistory[sessionId][phone]              = recentMsgs;
    console.log(`[MEMORY] Summarized ${oldMsgs.length} old messages for ${phone}`);
    return recentMsgs;
  } catch (e) {
    console.error("[MEMORY] Summarization failed:", e.message);
    return msgs;  // fall back to full history on error
  }
}

async function buildPrompt(text, sessionId, remoteJid, senderName = null) {
  try {
    const user = await getUserBySession(sessionId);
    if (!user) return null;

    const p    = await buildProfile(user);
    const phone = (remoteJid.match(/\d+/) || [])[0] || "";
    const lang  = detectLanguage(text);

    // Chat history — read snapshot for building prompt (atomic save handles merging)
    const allHistorySnapshot = (() => {
      try { return getChatHistory(user); } catch { return {}; }
    })();
    allHistorySnapshot[sessionId]        = allHistorySnapshot[sessionId]        || {};
    allHistorySnapshot[sessionId][phone] = allHistorySnapshot[sessionId][phone] || [];

    // Summarize old messages if history is getting long (>20 msgs)
    const trimmedHistory = await summarizeHistoryIfNeeded(user.id, sessionId, phone, allHistorySnapshot);
    const msgs = [...trimmedHistory, { role: "user", content: text }];

    // Pull any existing summary to inject into system prompt
    const customerMemory = allHistorySnapshot[sessionId][phone + "_summary"]
      ? `
CUSTOMER MEMORY (from previous conversations):
${allHistorySnapshot[sessionId][phone + "_summary"]}
`
      : "";

    // Google Calendar bookings
    let gcalBookings = [];
    const calSvc = await getCalendarService(user.id);
    if (calSvc) {
      try {
        const horizon = new Date();
        horizon.setDate(horizon.getDate() + CFG.lookaheadDays);
        const { data } = await calSvc.events.list({
          calendarId:"primary", timeMin:new Date().toISOString(),
          timeMax:horizon.toISOString(), singleEvents:true, orderBy:"startTime",
        });
        // Filter out events created by the bot itself (already in local appointments)
        // Prevents double-counting: same booking would consume 2 chair slots instead of 1
        const localEventIds = new Set(
          getAppointments(user).map(a => a.calendarEventId).filter(Boolean)
        );
        gcalBookings = (data.items||[])
          .filter(e => e.transparency !== "transparent" && !localEventIds.has(e.id))
          .map(e => ({
            id: `gcal-${e.id}`,
            timespan: `${fmtIST(new Date(e.start.dateTime||e.start.date))} - ${fmtIST(new Date(e.end.dateTime||e.end.date))}`,
          }));
      } catch (e) { console.error("[GCal]", e.message); }
    }

    // Availability
    const baseAvail  = prunePastDates(p.availability || {});
    const avail      = computeAvailability(baseAvail, [...getAppointments(user),...gcalBookings], CFG.bufferMinutes, p.chairs || 1);
    const availChart = buildAvailabilityChart(avail, 10);
    const today      = todayIST();
    const userAppts  = getAppointments(user).filter(b=>b.phone===phone&&b.date>=today);

    // Validate WhatsApp display name — only use if it looks like a real name
    // Reject: pure emojis, numbers only, too short, or no letters
    let customerName = null;
    if (senderName && senderName.trim().length >= 2) {
      const cleaned = senderName.trim();
      const hasLetter = /[a-zA-Z\u0900-\u097F]/.test(cleaned);
      const emojiOnly = /^[\u{1F000}-\u{1FFFF}\u{2600}-\u{27FF}\s]+$/u.test(cleaned);
      if (hasLetter && !emojiOnly) customerName = cleaned;
    }

    const ctx = { user, p, phone, remoteJid, sessionId, lang, gcalBookings, sock, customerName };

    const effectiveLang = (p.language && p.language !== "auto") ? p.language : lang;
    // Check for pending payment — warn AI not to create new bookings
    const allPhoneAppts = getAppointments(user).filter(b => b.phone === phone && b.date >= today);
    const pendingPayAppt = allPhoneAppts.find(b => b.payment_status === "pending");
    const paidAppt       = allPhoneAppts.find(b => b.payment_status === "paid" && b.date >= today);
    const pendingNote = pendingPayAppt
      ? `\n\n⚠️ PENDING PAYMENT: Customer already has *${pendingPayAppt.service}* slot RESERVED for ${friendlyDate(pendingPayAppt.date)} at ${friendlyTime12(pendingPayAppt.time)} — awaiting payment screenshot. Do NOT book another appointment. Remind them to send screenshot.`
      : paidAppt
      ? ("\n\nCRITICAL — BOOKING FULLY CONFIRMED AND PAID:\n"
        + "Customer's " + paidAppt.service + " on " + friendlyDate(paidAppt.date) + " at " + friendlyTime12(paidAppt.time) + " is CONFIRMED.\n"
        + "RULES: NEVER book again. NEVER send QR. NEVER ask for payment. NEVER say slot unavailable.\n"
        + "If customer says ok/thanks/done — confirm booking warmly, say milte hain.\n"
        + "If they ask something else — help, but no re-booking or re-charging.")
      : "";
    const systemContent = buildSystemPrompt(user, p, availChart, userAppts, lang, customerName) + customerMemory + pendingNote;
    const openaiMessages = [
      { role:"system", content:systemContent },
      ...msgs.slice(-(CFG.historyTurns * 2)),
    ];

    // Use the latest cost-effective model
    const MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";

    let response = await openai.chat.completions.create({
      model:       MODEL,
      messages:    openaiMessages,
      tools:       SALON_TOOLS,
      tool_choice: "auto",
      max_tokens:  400,
      temperature: 0.3,
    });

    // Track cost for this call
    await trackUsage(user.id, MODEL, response.usage);

    let assistantMsg = response.choices[0].message;
    let finalReply   = null;

    if (assistantMsg.tool_calls?.length) {
      // Suppress any "wait I'm booking" text the AI sends alongside tool calls
      // Customer only needs the result, not "thoda intezaar kijiye, booking kar raha hoon"
      const toolResults = [];
      for (const tc of assistantMsg.tool_calls) {
        const args   = JSON.parse(tc.function.arguments);
        const result = await executeTool(tc.function.name, args, ctx);
        toolResults.push({ tool_call_id:tc.id, role:"tool", content:result });
        finalReply = result;
      }

      // Booking was handled directly by execBookAppointment — suppress AI follow-up
      if (finalReply === "__BOOKING_DONE__") {
        finalReply = null;  // don't send anything — text was already sent via sendSafe
        // Add a neutral assistant message to history so context is maintained
        msgs.push({ role:"assistant", content:"[Booking confirmed and sent to customer]" });
      } else if (assistantMsg.tool_calls.length > 1 || !finalReply) {
        // Second call: pass language in system so AI replies in right language
        const langHint = effectiveLang === "hindi"
          ? "Sirf Hindi mein jawab dena."
          : effectiveLang === "hinglish"
          ? "Hinglish mein jawab dena (Roman Hindi + English mix). Friendly aur short raho."
          : "Reply in English only. Keep it short.";
        const r2 = await openai.chat.completions.create({
          model:MODEL,
          messages:[
            { role:"system", content: systemContent + "\n\nIMPORTANT: " + langHint },
            ...openaiMessages.slice(1),  // skip old system, keep history
            assistantMsg,
            ...toolResults,
          ],
          max_tokens:250, temperature:0.3,
        });
        await trackUsage(user.id, MODEL, r2.usage);
        finalReply = r2.choices[0].message.content?.trim() || finalReply;
      }  // end else-if (multiple tools or no reply)
    } else {
      finalReply = assistantMsg.content?.trim() || null;
    }

    if (finalReply) msgs.push({ role:"assistant", content:finalReply });
    const trimmedMsgs = msgs.slice(-(CFG.historyTurns * 2 + 2));

    // Atomic update: lock row, merge with FRESH DB data — no stale overwrites
    await atomicUpdateJsonb(user.id, "wa_chat_history", (allHist) => {
      allHist[sessionId] = allHist[sessionId] || {};
      // Merge our new messages on top of what's currently in the DB (fresh, not stale)
      const freshMsgs  = allHist[sessionId][phone] || [];
      const ourNewMsgs = trimmedMsgs.slice(freshMsgs.length);
      const merged = [...freshMsgs, ...ourNewMsgs].slice(-(CFG.historyTurns * 2 + 2));
      allHist[sessionId][phone] = merged;
      // Persist the summary key if summarization ran this call
      const summaryKey = phone + "_summary";
      if (allHistorySnapshot[sessionId][summaryKey]) {
        allHist[sessionId][summaryKey] = allHistorySnapshot[sessionId][summaryKey];
      }
      // Keep only 50 most-recent phones
      const sHist = allHist[sessionId];
      const allKeys = Object.keys(sHist).filter(k => !k.endsWith("_summary"));
      if (allKeys.length > 50) {
        allKeys.slice(0, allKeys.length - 50).forEach(k => {
          delete sHist[k];
          delete sHist[k + "_summary"];
        });
      }
      allHist[sessionId] = sHist;
      return allHist;
    });

        // Post-process 0: strip "booking in progress" filler sentences from replies
    // Strategy: REMOVE filler sentences rather than suppressing the whole reply
    if (finalReply) {
      // Strip ONLY "currently booking" filler — keep "kya main book kar dun?" (that's good)
      const fillerSentencePatterns = [
        /[^।।.!?]*(?:main|hum)\s+abhi\s+booking\s+kar\s+raha\s+hoon[^।।.!?]*[।।.!?]?\s*/gi,
        /[^।।.!?]*(?:main|hum)\s+(?:abhi\s+)?book\s+kar\s+raha\s+hoon[^।।.!?]*[।।.!?]?\s*/gi,
        /[^।।.!?]*booking\s+kar\s+raha\s+hoon[^।।.!?]*[।।.!?]?\s*/gi,
        /[^।।.!?]*thodi?\s+der\s+(?:mein|me)\s+confirm\s+kar\s+(?:deta|raha)\s+hoon[^।।.!?]*[।।.!?]?\s*/gi,
        /[^।।.!?]*book\s+karne\s+ka\s+soch\s+rahe?\s+(?:hain|hai)[^।।.!?]*[।।.!?]?\s*/gi,
      ];
      let cleaned = finalReply;
      for (const pattern of fillerSentencePatterns) {
        cleaned = cleaned.replace(pattern, '');
      }
      cleaned = cleaned.trim().replace(/\s{2,}/g, ' ').replace(/^\s*[।।.!?,]\s*/g, '');
      if (cleaned !== finalReply && cleaned.length > 10) {
        console.warn("[FILLER-STRIPPED]", finalReply.length, "→", cleaned.length);
        finalReply = cleaned;
      } else if (cleaned.length <= 10 && finalReply.length > 20) {
        // Whole reply was filler — suppress entirely
        console.warn("[FILLER-SUPPRESSED]", finalReply.substring(0, 80));
        finalReply = null;
      }
    }


    // Post-process 1: convert HH:MM to 12h
    if (finalReply) {
      finalReply = finalReply.replace(/\b([01]?\d|2[0-3]):([0-5]\d)\b/g, (_, h, m) => {
        const hh = parseInt(h), mm = parseInt(m);
        const suffix = hh >= 12 ? "PM" : "AM";
        const h12 = hh > 12 ? hh - 12 : (hh === 0 ? 12 : hh);
        return mm === 0 ? `${h12} ${suffix}` : `${h12}:${String(mm).padStart(2,"0")} ${suffix}`;
      });
    }
    // Post-process 2: translate English error phrases when conversation is Hinglish/Hindi
    if (finalReply && effectiveLang !== "english") {
      finalReply = finalReply
        .replace(/Sorry,?\s*(.+?)\s*is not available\. Please choose another time\.?/gi,
          effectiveLang === "hindi"
            ? "Yeh time available nahi hai. Koi aur time batayein?"
            : "Yaar yeh time available nahi hai. Koi aur time batao?")
        .replace(/Appointment not found\. Please check the ID and try again\.?/gi,
          effectiveLang === "hindi" ? "Appointment nahi mili." : "Booking nahi mili bhai.")
        .replace(/You can only (cancel|reschedule) your own appointments\.?/gi,
          effectiveLang === "hindi" ? "Sirf apni booking $1 kar sakte hain." : "Sirf apni booking $1 kar sakte ho.")
        .replace(/No upcoming appointments\.?/gi,
          effectiveLang === "hindi" ? "Koi upcoming appointment nahi hai." : "Koi booking nahi hai bhai.");
    }
    return finalReply;
  } catch (err) {
    console.error("[buildPrompt]", err);
    // Return a language-aware error message
    const errLang = detectLanguage(text);
    if (errLang === "hindi")    return "Kuch technical problem aa gayi hai. Thodi der baad try karo. 🙏";
    if (errLang === "hinglish") return "Bhai kuch gadbad ho gayi! Thodi der mein dobara try karo. 🙏";
    return "A technical issue occurred. Please try again in a moment. 🙏";
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 13 — REMINDERS
// ─────────────────────────────────────────────────────────────────────────────

function reminderMessage(appt, salonName, minutesUntil, lang = "hinglish") {
  const t  = friendlyTime12(appt.time);   // "2:30 PM" not "14:30"
  const dl = friendlyDate(appt.date);
  if (lang === "hindi") {
    if (minutesUntil <= 10)  return `🕐 *${salonName}* mein aapka *${appt.service}* appointment ${t} pe shuru hone wala hai. Aa jaiye! 🙏`;
    if (minutesUntil <= 30)  return `⏰ 30 minute mein milte hain! *${salonName}* — *${appt.service}* ${dl} ${t}.`;
    return `📅 Kal *${t}* baje *${salonName}* mein *${appt.service}* appointment hai. Yaad rakhen! 🙏`;
  }
  if (lang === "english") {
    if (minutesUntil <= 10)  return `🕐 Your *${appt.service}* at *${salonName}* starts in 10 min at ${t}. See you soon! 🙏`;
    if (minutesUntil <= 30)  return `⏰ See you in 30 min! *${appt.service}* at *${salonName}* — ${dl} at ${t}. 😊`;
    return `📅 Reminder: *${appt.service}* at *${salonName}* tomorrow at ${t}. Looking forward to seeing you! 🙏`;
  }
  // hinglish (default)
  if (minutesUntil <= 10)  return `🕐 Bhai/Didi, *${salonName}* mein *${appt.service}* ${t} pe hai — bas 10 min baaki! Aa jaiye! 🙏`;
  if (minutesUntil <= 30)  return `⏰ 30 min mein milte hain! *${salonName}* — *${appt.service}*, ${dl} ${t}. 😊`;
  return `📅 Kal *${t}* baje *${salonName}* mein *${appt.service}* appointment hai. Mat bhoolna! 😄`;
}

async function runReminderChecks(sessionId) {
  try {
    const user = await getUserBySession(sessionId);
    if (!user) return;
    let books = getAppointments(user);
    if (!books.length) return;

    // ── Cleanup: archive appointments older than 90 days ──────────────────────
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - 90);
    const cutoffStr = cutoffDate.toLocaleDateString("sv-SE", { timeZone: CFG.timezone });
    const sizeBefore = books.length;
    books = books.filter(b => !b.date || b.date >= cutoffStr);
    if (books.length < sizeBefore) {
      console.log(`[CLEANUP] Removed ${sizeBefore - books.length} old appointments for user ${user.id}`);
      await saveAppointments(user.id, books);
      invalidateProfileCache(user.id);
    }

    if (!books.length) return;

    const now       = Date.now();
    const p         = await buildProfile(user);
    const salonName = p.business_name;
    const lang      = p.language || "hinglish";
    let   changed   = false;

    for (let i = 0; i < books.length; i++) {
      const appt = books[i];
      if (!appt?.date || !appt?.time || !appt?.remoteJid) continue;
      const apptMs  = parseIST(appt.date, appt.time).getTime();
      const diffMin = (apptMs - now) / 60_000;
      if (diffMin < -1) continue;

      const sent = appt.remindersSent || {};
      let   msg  = null;
      if      (diffMin > 59 && diffMin <= 61 && !sent.oneHour)  { msg = reminderMessage(appt, salonName, 60, lang); sent.oneHour  = true; }
      else if (diffMin > 29 && diffMin <= 31 && !sent.thirtyMin){ msg = reminderMessage(appt, salonName, 30, lang); sent.thirtyMin= true; }
      else if (diffMin > 9  && diffMin <= 11 && !sent.tenMin)   { msg = reminderMessage(appt, salonName, 10, lang); sent.tenMin   = true; }

      if (msg) {
        // Only send reminder if customer messaged within last 48 hours
        // (avoids sending unsolicited first messages — WA ban risk)
        const chatHistory = getChatHistory(user);
        const sId = sessionId;
        const custPhone = appt.phone || (appt.remoteJid?.match(/\d+/) || [])[0] || "";
        const custMsgs = chatHistory[sId]?.[custPhone] || [];
        const lastMsgTs = custMsgs.length
          ? new Date(custMsgs[custMsgs.length - 1]?.ts || 0).getTime()
          : 0;
        const hoursSinceMsg = (Date.now() - lastMsgTs) / 3_600_000;
        // Skip if no conversation at all OR if last message was >48h ago
        if (custMsgs.length === 0 || hoursSinceMsg > 48) {
          console.log(`[REMINDER] Skipping ${custPhone} — no recent conversation (${Math.round(hoursSinceMsg)}h ago)`);
          continue;
        }
        // Check opt-out list
        const bs = p.bot_settings ? (typeof p.bot_settings === "string" ? JSON.parse(p.bot_settings) : p.bot_settings) : {};
        if ((bs.blocked_numbers || []).includes(custPhone)) {
          console.log(`[REMINDER] Skipping ${custPhone} — opted out`);
          continue;
        }
        const ok = await sendSafe(appt.remoteJid, msg);
        if (ok) { books[i].remindersSent = sent; changed = true; }
      }
    }
    if (changed) await saveAppointments(user.id, books);
  } catch (e) { console.error("[REMINDER]", e); }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14 — SEND HELPERS & HUMAN DELAYS
// ─────────────────────────────────────────────────────────────────────────────

const sleep = ms => new Promise(r => setTimeout(r, ms));

function humanDelay(minMs, maxMs) {
  const raw  = minMs + Math.random() * (maxMs - minMs);
  const skew = raw * (0.75 + Math.random() * 0.5);
  return Math.round(Math.max(minMs, Math.min(maxMs, skew)));
}

function typingDuration(text = "") {
  const cpm   = 180 + Math.random() * 80;
  const baseMs = (text.length / cpm) * 60_000;
  return Math.round(Math.max(800, Math.min(6000, baseMs * (0.8 + Math.random() * 0.4))));
}

/**
 * Send text via Baileys with retry logic.
 * Uses the module-level `sock` variable.
 */
// Per-JID last-send timestamps — prevent sending >1 message/2s to same number
const _lastSentTs = new Map();

async function sendSafe(to, text, retries = 2) {
  // Enforce minimum 2s gap between outgoing messages to the same JID
  const lastSent = _lastSentTs.get(to) || 0;
  const gap = Date.now() - lastSent;
  if (gap < 2000) await sleep(2000 - gap);
  _lastSentTs.set(to, Date.now());

  // Snapshot the socket NOW — in-flight calls survive reconnect setting sock=null
  const activeSock = sock;
  for (let i = 0; i <= retries; i++) {
    try {
      if (!activeSock) throw new Error("Socket not connected");
      await activeSock.sendMessage(to, { text });
      return true;
    } catch (e) {
      console.warn(`[SEND] attempt ${i+1} failed: ${e.message}`);
      if (i < retries && /timed out|not connected|connection|lost/i.test(e.message)) {
        await sleep(humanDelay((i+1)*3000, (i+1)*8000));
      } else {
        console.error(`[SEND] final failure for ${to}`);
        return false;
      }
    }
  }
  return false;
}

const MEDIA_REGEX = /https?:\/\/[^\s'"]+\.(?:png|jpe?g|gif|webp|mp4|mov|webm)(?:\?[^\s'"]*)?/gi;

async function sendReply(sessionId, remoteJid, text) {
  const activeSock = sock;  // snapshot — immune to mid-flight reconnect
  if (!activeSock) return;
  const urls = text.match(MEDIA_REGEX) || [];

  // Always send the text first — never swallow the message
  await sendSafe(remoteJid, text);

  // Then send any extracted media separately
  for (const url of urls) {
    try {
      await sleep(humanDelay(400, 900));
      if (/\.(?:mp4|mov|webm)(?:\?|$)/i.test(url)) {
        await activeSock.sendMessage(remoteJid, { video: { url } });
      } else {
        await activeSock.sendMessage(remoteJid, { image: { url } });
      }
    } catch (e) {
      console.error(`[SEND-MEDIA] ${url}:`, e.message);
      // Non-fatal: text was already sent above
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 15 — MESSAGE HANDLING
// ─────────────────────────────────────────────────────────────────────────────

async function processUserQueue(sessionId, remoteJid) {
  const state = userProcState.get(remoteJid);
  if (!state || state.isProcessing || !state.queue.length) return;

  state.isProcessing = true;
  try {
    const burst    = state.queue.shift();
    const combined = burst.map(m => m.text).join("\n");

    // Realistic read-receipt delay before processing (simulates human reading)
    const wordCount   = combined.split(/\s+/).length;
    const readMs      = Math.min(wordCount * 250, 4000);  // ~250ms per word, max 4s
    const jitter      = humanDelay(300, 1200);
    await sleep(readMs + jitter);

    const lastTs = burst[burst.length - 1].ts;
    // Per-JID dedup: if we replied to THIS SPECIFIC user < 10s ago, skip
    // (protects against duplicate processing if queue races)
    if (lastTs - (lastReply.get(remoteJid) || 0) < 10_000) {
      state.isProcessing = false;
      processUserQueue(sessionId, remoteJid);
      return;
    }
    lastReply.set(remoteJid, lastTs);

    const senderName = burst.map(m => m.pushName).find(Boolean) || null;
    const reply = await buildPrompt(combined, sessionId, remoteJid, senderName);
    if (reply) {
      const typingMs = typingDuration(reply);
      const activeSockQ = sock;
      // 1. Mark last message as read (blue ticks)
      try {
        const lastMsg = burst[burst.length - 1];
        if (activeSockQ && lastMsg?.msgId) {
          await activeSockQ.readMessages([{
            remoteJid, id: lastMsg.msgId, fromMe: false,
            participant: lastMsg.participant || undefined,
          }]);
        }
      } catch {}
      // 2. Short pause — simulates person reading before typing
      await sleep(humanDelay(500, 1100));
      // 3. Show composing indicator
      try { if (activeSockQ) await activeSockQ.sendPresenceUpdate("composing", remoteJid); } catch {}
      // 4. Natural typing delay scaled to message length
      await sleep(typingMs);
      // 5. Brief pause after typing before message appears
      try { if (activeSockQ) await activeSockQ.sendPresenceUpdate("paused", remoteJid); } catch {}
      await sleep(humanDelay(80, 200));
      await sendReply(sessionId, remoteJid, reply);;
    }
  } catch (e) {
    console.error(`[QUEUE] ${remoteJid}:`, e);
    if (/closed|authentication|not connected/i.test(e.message)) {
      await updateUserBySession(sessionId, { wa_status: 1 });
    }
  } finally {
    state.isProcessing = false;
    // Use setTimeout to avoid synchronous recursion stack buildup under heavy load
    setTimeout(() => processUserQueue(sessionId, remoteJid), 0);
  }
}

/**
 * Resolve the best phone-number string from a remoteJid.
 * Handles both PN JIDs (@s.whatsapp.net) and LID JIDs (@lid).
 * For LIDs, attempts to resolve via the sock's LID mapping store.
 */
function extractPhoneFromJid(remoteJid, msgKey) {
  // If we have a PN-style JID, just extract digits
  if (remoteJid?.endsWith("@s.whatsapp.net") || remoteJid?.endsWith("@c.us")) {
    return (remoteJid.match(/\d+/) || [])[0] || "";
  }

  // v7 LID handling: check for remoteJidAlt (the alternate PN JID)
  if (msgKey?.remoteJidAlt) {
    return (msgKey.remoteJidAlt.match(/\d+/) || [])[0] || "";
  }

  // Try to resolve LID → PN via Baileys' internal mapping
  if (sock?.signalRepository?.lidMapping) {
    try {
      const pn = sock.signalRepository.lidMapping.getPNForLID(remoteJid);
      if (pn) return (pn.match(/\d+/) || [])[0] || "";
    } catch {}
  }

  // Fallback: extract whatever digits exist in the JID
  return (remoteJid?.match(/\d+/) || [])[0] || "";
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14d — PREMIUM BRANDED QR CODE GENERATOR
// ─────────────────────────────────────────────────────────────────────────────

const QR_SIZE   = 480;
const LOGO_SIZE = 112;  // ~23% of QR_SIZE

function hexToRgb(hex) {
  const c = hex.replace("#", "");
  return {
    r: parseInt(c.substring(0, 2), 16),
    g: parseInt(c.substring(2, 4), 16),
    b: parseInt(c.substring(4, 6), 16),
  };
}

// Inline scissors SVG — no network fetch, always available
function scissorsSVG(size, color = "#1a1a2e") {
  const s = size;
  // Clean professional scissors using standard path data
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${s}" height="${s}" viewBox="0 0 24 24">
  <rect width="24" height="24" rx="4" fill="white"/>
  <path fill="${color}" d="M19.071 2.343 12 9.414 4.929 2.343 3.515 3.757 9.172 9.414l-1.415 1.415A3.5 3.5 0 1 0 9.9 13.07L12 10.97l2.1 2.1a3.5 3.5 0 1 0 1.415-1.414l-1.415-1.415 5.657-5.657-1.686-1.241ZM7 14.914a1.5 1.5 0 1 1-3 0 1.5 1.5 0 0 1 3 0Zm10 0a1.5 1.5 0 1 1-3 0 1.5 1.5 0 0 1 3 0Z"/>
</svg>`;
}

/**
 * Generate a branded UPI QR code with scissors icon centred.
 * Uses sharp for compositing — falls back to plain QR if sharp unavailable.
 * Never makes network requests — scissors are inline SVG.
 */
async function generatePremiumQR(upiLink, brandColor) {
  const col = hexToRgb(brandColor || "#1a1a2e");
  const darkHex = `#${col.r.toString(16).padStart(2,"0")}${col.g.toString(16).padStart(2,"0")}${col.b.toString(16).padStart(2,"0")}`;

  // ── 1. Base QR buffer ──────────────────────────────────────────────────────
  let qrBuf;
  try {
    qrBuf = await QRCode.toBuffer(upiLink, {
      errorCorrectionLevel: "H",
      width:  QR_SIZE,
      margin: 2,
      color: { dark: darkHex, light: "#ffffff" },
    });
  } catch (e) {
    console.error("[QR] toBuffer failed:", e.message);
    return null;
  }

  // ── 2. Try compositing scissors with sharp ─────────────────────────────────
  try {
    const sharp = (await import("sharp")).default;
    const svgBuf = Buffer.from(scissorsSVG(LOGO_SIZE, darkHex));

    // White padded background behind scissors
    const padded = await sharp({
      create: { width: LOGO_SIZE, height: LOGO_SIZE, channels: 4,
                background: { r:255, g:255, b:255, alpha:255 } }
    })
      .composite([{ input: svgBuf }])
      .png()
      .toBuffer();

    const centre = Math.round((QR_SIZE - LOGO_SIZE) / 2);
    const final  = await sharp(qrBuf)
      .composite([{ input: padded, top: centre, left: centre }])
      .png()
      .toBuffer();

    return final;
  } catch (e) {
    console.warn("[QR-COMPOSITE] sharp failed, using plain QR:", e.message);
    return qrBuf;  // plain branded QR — still works perfectly
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14c — VOICE NOTE TRANSCRIPTION (Whisper)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Transcribe a voice note buffer using OpenAI Whisper.
 * Saves to a temp file (Whisper requires a file stream), then cleans up.
 * Cost: ~$0.006/min. Average voice note ≈15s ≈ $0.0015 per call.
 */
async function transcribeAudio(audioBuffer, userId) {
  const tmpPath = path.join(os.tmpdir(), `voice_${Date.now()}_${userId}.ogg`);
  try {
    await fs.writeFile(tmpPath, audioBuffer);
    const transcription = await openai.audio.transcriptions.create({
      model: "whisper-1",
      file:  createReadStream(tmpPath),
      language: "hi",    // hint: Indian languages (handles Hindi, hinglish, English)
      response_format: "text",
    });
    // Track Whisper cost — billed per second, approximate as 15s avg clip
    try { await pool.query(`
      INSERT INTO ai_usage (user_id, session_date, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, calls)
      VALUES ($1, CURRENT_DATE, 'whisper-1', 0, 0, 0, 0.0015, 1)
      ON CONFLICT (user_id, session_date, model) DO UPDATE SET
        cost_usd = ai_usage.cost_usd + 0.0015, calls = ai_usage.calls + 1, updated_at = NOW()
    `, [userId]); } catch {}
    return typeof transcription === "string" ? transcription.trim() : transcription?.text?.trim() || null;
  } catch (e) {
    console.error("[WHISPER] Transcription failed:", e.message);
    return null;
  } finally {
    await fs.unlink(tmpPath).catch(() => {});  // always clean up temp file
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14b — PAYMENT SCREENSHOT VERIFICATION
// ─────────────────────────────────────────────────────────────────────────────

const FLASK_APP_URL = process.env.FLASK_APP_URL || "http://127.0.0.1:5000";
const BOT_API_KEY   = process.env.BOT_API_KEY   || "";

/**
 * Download a Baileys image message to a buffer.
 */
async function downloadImageBuffer(imageMessage) {
  const { downloadMediaMessage } = await import("@whiskeysockets/baileys");
  const buffer = await downloadMediaMessage(
    { message: { imageMessage } },
    "buffer",
    {},
    { logger, reuploadRequest: sock.updateMediaMessage }
  );
  return buffer;
}

/**
 * Upload screenshot to Flask static folder and store payment record in DB.
 * Calls POST /api/payment/screenshot (API-key protected).
 */
async function uploadScreenshotToFlask(userId, appointmentId, phone, imgB64, txnId, verified, fakeScore, aiNotes, amount, bankingName = null) {
  try {
    const payload = JSON.stringify({
      user_id:        userId,
      appointment_id: appointmentId,
      phone,
      screenshot_b64: imgB64,
      transaction_id: txnId,
      amount_verified: amount,
      verified,
      fake_score:     fakeScore,
      ai_notes:       aiNotes,
      banking_name:   bankingName,
    });
    const res = await fetch(`${FLASK_APP_URL}/api/payment/screenshot`, {
      method:  "POST",
      headers: { "Content-Type": "application/json", "X-API-Key": BOT_API_KEY },
      body:    payload,
    });
    const data = await res.json();
    if (!data.ok) console.error("[UPLOAD-SS] Flask error:", data.error);
    return data;
  } catch (e) {
    console.error("[UPLOAD-SS]", e.message);
    return { ok: false };
  }
}

/**
 * Use GPT-4o-mini vision to verify a payment screenshot.
 * Returns { verified, transaction_id, amount, fake_score, notes }
 */
async function verifyPaymentScreenshot(imgB64, expectedAmount, upiId, userId, receivedAtISO = null) {
  const MODEL = process.env.PAYMENT_VERIFY_MODEL || "gpt-4o";
  const receivedStr = receivedAtISO ? new Date(receivedAtISO).toLocaleString("en-IN", { timeZone: "Asia/Kolkata" }) : "unknown time";
  const receivedLine = `Screenshot received at: ${receivedStr} IST`;
  const expectedLine = `Expected: \u20b9${expectedAmount} to UPI ${upiId}`;
  const prompt = [
    "You are an expert payment fraud analyst for an Indian salon.",
    receivedLine,
    "",
    "Respond ONLY with valid JSON (no markdown):",
    '{ "is_payment": true/false, "verified": true/false, "transaction_id": "UTR string or null", "amount": number_or_null, "banking_name": "name or null", "screenshot_time": "time string or null", "fake_score": 0.0-1.0, "notes": "one line" }',
    "",
    "STEP 1: Is this a payment app? Accept PhonePe/GPay/Paytm/BHIM/bank UPI success. Reject selfies/food/etc (is_payment=false).",
    "",
    "STEP 2 (if is_payment=true):",
    expectedLine,
    "- Amount: extract exact rupee amount shown. Must equal expected.",
    "- Status: must be Transaction Successful/Payment Successful/Sent. FAIL if Pending/Failed/Processing.",
    "- Transaction ID: extract UTR/TXN/Reference ID. Required for verified=true.",
    "- Time: extract timestamp shown IN screenshot. Flag suspicious if >2 hours before received time or in future.",
    "- Banking name: extract payee account holder name if visible.",
    "",
    "STEP 3: UPI ID — apps mask accounts (XXXXXXXX6606). This is NORMAL. Do NOT fail for masked numbers.",
    "",
    "STEP 4: Fake signs (raise fake_score): no TXN ID=0.8, edited UI=0.7+, Pending/Failed=verified false, wrong amount=verified false.",
    "",
    "verified=true ONLY if: is_payment=true AND status=successful AND amount matches AND transaction_id present.",
  ].join("\n")

  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{
        role: "user",
        content: [
          { type: "text",       text: prompt },
          { type: "image_url",  image_url: { url: `data:image/jpeg;base64,${imgB64}`, detail: "high" } },
        ],
      }],
      max_tokens: 300,
      temperature: 0,
    });
    await trackUsage(userId, MODEL, resp.usage);
    const raw  = resp.choices[0].message.content?.trim() || "{}";
    const clean = raw.replace(/```json|```/g, "").trim();
    return JSON.parse(clean);
  } catch (e) {
    console.error("[VERIFY-SS]", e.message);
    return { verified: false, transaction_id: null, amount: null, fake_score: 0.5, notes: "Verification error: " + e.message };
  }
}

async function handlePaymentScreenshot(imageMessage, msgKey, pendingAppt, user, p, remoteJid, sessionId, senderNum) {
  const lang     = p.language || "hinglish";
  const liveSock = sock;

  // 1. Human delay — read receipt first, then "seen", then start typing
  await sleep(humanDelay(1200, 2200));  // simulate reading the image

  // Send read receipt (marks screenshot as "seen" — blue ticks)
  try {
    if (liveSock && msgKey?.id) {
      await liveSock.readMessages([{
        remoteJid,
        id:          msgKey.id,
        fromMe:      false,
        participant: msgKey.participant || undefined,
      }]);
    }
  } catch (e) { console.warn("[IMG-READ]", e.message); }

  // Short pause before acknowledging (human-like)
  await sleep(humanDelay(800, 1500));

  // 2. Show composing → type briefly → send ack
  try { if (liveSock) await liveSock.sendPresenceUpdate("composing", remoteJid); } catch {}
  await sleep(humanDelay(700, 1200));  // typing the ack message
  try { if (liveSock) await liveSock.sendPresenceUpdate("paused", remoteJid); } catch {}
  await sleep(humanDelay(80, 180));

  const ackMsg = lang === "hindi"
    ? "📸 Screenshot dekh raha hoon... 🔍"
    : lang === "hinglish"
    ? "📸 Screenshot dekh raha hoon bhai... 🔍"
    : "📸 Checking your screenshot... 🔍";
  await sendSafe(remoteJid, ackMsg);

  // 3. Download image
  let imgBuffer, imgB64;
  try {
    imgBuffer = await downloadImageBuffer(imageMessage);
    imgB64    = imgBuffer.toString("base64");
  } catch (e) {
    console.error("[DL-IMG]", e.message);
    try { if (liveSock) await liveSock.sendPresenceUpdate("paused", remoteJid); } catch {}
    const failMsg = lang === "hinglish"
      ? "Screenshot download nahi hua. Dobara bhejo. 🙏"
      : lang === "hindi"
      ? "Screenshot nahi aayi. Dobara bhejein. 🙏"
      : "Could not download screenshot. Please send again. 🙏";
    try { if (liveSock) await liveSock.sendPresenceUpdate("composing", remoteJid); } catch {}
    await sleep(humanDelay(600, 1000));
    try { if (liveSock) await liveSock.sendPresenceUpdate("paused", remoteJid); } catch {}
    await sleep(100);
    await sendSafe(remoteJid, failMsg);
    return;
  }

  // Composing while AI verifies
  try { if (liveSock) await liveSock.sendPresenceUpdate("composing", remoteJid); } catch {}

  // 4. AI verification — with timestamp of when screenshot was received
  const receivedAt = new Date().toISOString();
  const result = await verifyPaymentScreenshot(imgB64, p.advance_amount, p.upi_id, user.id, receivedAt);
  const { is_payment = true, verified, transaction_id, amount, fake_score = 0, notes,
          banking_name = null, screenshot_time = null } = result;

  try { if (liveSock) await liveSock.sendPresenceUpdate("paused", remoteJid); } catch {}
  await sleep(humanDelay(600, 1200));  // pause before reply

  // Not a payment image (haircut photo, selfie, etc.) — respond naturally
  if (is_payment === false) {
    const notPayMsg = lang === "hindi"
      ? "Ye payment screenshot nahi hai. Advance payment ki screenshot bhejein. 🙏"
      : lang === "hinglish"
      ? "Bhai ye payment screenshot nahi lag rahi. GPay/PhonePe ka screenshot bhejo. 🙏"
      : "This doesn't appear to be a payment screenshot. Please send your GPay/PhonePe/Paytm payment confirmation. 🙏";
    await sleep(humanDelay(600, 1000));
    try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
    await sleep(humanDelay(700, 1200));
    try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
    await sleep(100);
    await sendSafe(remoteJid, notPayMsg);
    return;
  }

  // 4. Upload to Flask regardless of verification (for records)
  const uploadRes = await uploadScreenshotToFlask(
    user.id, pendingAppt.id, senderNum, imgB64,
    transaction_id, verified, fake_score, notes, amount, banking_name
  );

  // 5. Update appointment in DB based on result
  const books = getAppointments(user);
  const bi    = books.findIndex(b => b.id === pendingAppt.id);

  // Validate paid amount is at least the required advance (prevents ₹1 scam)
  const expectedAmount = Number(p.advance_amount) || 0;
  const paidAmount     = Number(amount) || 0;
  const amountOk       = expectedAmount === 0 || paidAmount >= expectedAmount * 0.98; // 2% tolerance

  if (verified && fake_score < 0.4 && amountOk) {
    // ── PAYMENT VERIFIED ────────────────────────────────────────────────────
    // Use fresh atomicUpdateJsonb to avoid stale user object bug
    await atomicUpdateJsonb(user.id, "wa_appointments", (appts) => {
      const appt = appts.find(b => b.id === pendingAppt.id);
      if (appt) {
        appt.payment_status = "paid";
        appt.transaction_id = transaction_id;
        appt.amount_paid    = amount || p.advance_amount;
        appt.screenshot_id   = uploadRes.screenshot_id;
        appt.screenshot_path = uploadRes.path || null;   // <-- was missing, needed by dashboard
        if (banking_name)    appt.banking_name    = banking_name;
        if (screenshot_time) appt.screenshot_time = screenshot_time;
        appt.payment_verified_at = new Date().toISOString();
      }
      return appts;
    });
    invalidateProfileCache(user.id);
    const dl = pendingAppt.date ? ` on ${friendlyDate(pendingAppt.date)} at ${friendlyTime12(pendingAppt.time)}` : "";
    const txnLine = transaction_id ? `\n🔑 TXN: ${transaction_id}` : "";
    const nameLine = banking_name ? `\n👤 ${banking_name}` : "";
    const confirmMsg = lang === "hindi"
      ? `✅ *Payment Verified!*\n💰 ₹${amount || p.advance_amount}${txnLine}${nameLine}\n\nAapki *${pendingAppt.service}* booking *confirm* ho gayi${dl}! Milte hain. 🙏`
      : lang === "hinglish"
      ? `✅ *Payment Confirmed!*\n💰 ₹${amount || p.advance_amount}${txnLine}${nameLine}\n\n*${pendingAppt.service}* booking *confirm* ho gayi${dl}! Milte hain bhai. 😊`
      : `✅ *Payment Verified!*\n💰 ₹${amount || p.advance_amount}${txnLine}${nameLine}\n\n*${pendingAppt.service}* booking *confirmed*${dl}! See you soon. 🙏`;
    await sleep(humanDelay(800, 1400));
    try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
    await sleep(humanDelay(1000, 1800));
    try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
    await sleep(150);
    await sendSafe(remoteJid, confirmMsg);

    // ── Clear old booking-phase chat history so AI doesn't hallucinate ────────
    // Inject only the payment confirmation exchange — wipes "QR code aa raha hai" etc.
    await atomicUpdateJsonb(user.id, "wa_chat_history", (allHist) => {
      allHist[sessionId] = allHist[sessionId] || {};
      allHist[sessionId][senderNum] = [
        { role: "user",      content: "[Payment screenshot sent — payment verified successfully]" },
        { role: "assistant", content: confirmMsg },
      ];
      return allHist;
    });

    // ── Send salon location after payment confirmed ──────────────────────────
    setTimeout(async () => {
      try {
        const liveSock2 = sock;
        if (!liveSock2) return;
        const mapsUrl = p.google_maps_url || "";
        const addr    = p.location || "";
        if (!mapsUrl && !addr) return;

        // Extract lat/lng — supports ?q=lat,lng  @lat,lng  !3dlat  formats
        // Match Google Maps coordinate patterns:
        // maps.google.com/?q=28.6139,77.2090
        // google.com/maps/@28.6139,77.2090,15z
        // maps.app.goo.gl redirects (no coords — fall back to link)
        // goo.gl/maps/... (no coords)
        const coordMatch = mapsUrl.match(/(?:[?&@]|!3d)(-?[0-9]{1,3}\.[0-9]{3,}),(-?[0-9]{1,3}\.[0-9]{3,})/);

        const salonName = p.business_name || "Salon";
        const addrText  = addr || "";
        const mapLine   = mapsUrl ? `\n🗺️ *Google Maps:* ${mapsUrl}` : "";

        // ── Always send address text first ───────────────────────────────────
        try { await liveSock2.sendPresenceUpdate("composing", remoteJid); } catch {}
        await sleep(humanDelay(600, 1000));
        try { await liveSock2.sendPresenceUpdate("paused", remoteJid); } catch {}
        await sleep(100);

        const addrMsg = lang === "hindi"
          ? `📍 *${salonName}*${addrText ? "\n" + addrText : ""}${mapLine}\n\nAapka intezaar hai! 🙏`
          : lang === "hinglish"
          ? `📍 *${salonName}*${addrText ? "\n" + addrText : ""}${mapLine}\n\nMilte hain! 😊`
          : `📍 *${salonName}*${addrText ? "\n" + addrText : ""}${mapLine}\n\nSee you! 🙏`;

        await sendSafe(remoteJid, addrMsg);
        console.log("[LOCATION] Address text sent");

        // ── Then send native WhatsApp location pin if coords available ────────
        if (coordMatch) {
          const lat = parseFloat(coordMatch[1]);
          const lng = parseFloat(coordMatch[2]);
          await sleep(humanDelay(800, 1400));
          await liveSock2.sendMessage(remoteJid, {
            location: {
              degreesLatitude:  lat,
              degreesLongitude: lng,
              name:    salonName,
              address: addrText || mapsUrl,
            }
          });
          console.log(`[LOCATION] ✅ WA pin sent: ${lat},${lng}`);
        }
      } catch (e) { console.error("[LOCATION-SEND]", e.message); }
    }, humanDelay(2500, 3500));

  } else if (verified && fake_score < 0.4 && !amountOk) {
    // ── WRONG AMOUNT PAID ───────────────────────────────────────────────────
    await atomicUpdateJsonb(user.id, "wa_appointments", (appts) => {
      const appt = appts.find(b => b.id === pendingAppt.id);
      if (appt) { appt.payment_status = "wrong_amount"; appt.ai_notes = `Paid: ${paidAmount}, Expected: ${expectedAmount}`; }
      return appts;
    });
    invalidateProfileCache(user.id);
    const wrongAmtMsg = lang === "hindi"
      ? `⚠️ Payment mili (₹${paidAmount}), lekin ₹${expectedAmount} chahiye tha.\n\nBaaki amount bhejein ya refund ke liye salon se contact karein.`
      : lang === "hinglish"
      ? `⚠️ Payment mili bhai (₹${paidAmount}) par ₹${expectedAmount} chahiye tha.\n\nBaaki bhejo ya salon se baat karo.`
      : `⚠️ Payment received (₹${paidAmount}) but ₹${expectedAmount} was required.\n\nPlease pay the remaining amount or contact the salon.`;
    await sleep(humanDelay(700, 1200));
    try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
    await sleep(humanDelay(900, 1600));
    try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
    await sleep(120);
    await sendSafe(remoteJid, wrongAmtMsg);

  } else if (fake_score >= 0.6) {
    // ── SUSPICIOUS / FAKE PAYMENT ───────────────────────────────────────────
    await atomicUpdateJsonb(user.id, "wa_appointments", (appts) => {
      const appt = appts.find(b => b.id === pendingAppt.id);
      if (appt) { appt.payment_status = "suspicious"; appt.ai_notes = notes; }
      return appts;
    });
    invalidateProfileCache(user.id);
    const suspectMsg = lang === "hindi"
      ? `⚠️ Payment verify nahi ho saka.\n\nPlease *original screenshot* bhejein PhonePe/GPay/Paytm app se. Edited ya WhatsApp status screenshot accept nahi hoga.\n\nDobara bhejein ya UPI ID se seedha bhejein: *${p.upi_id}*`
      : lang === "hinglish"
      ? `⚠️ Yaar payment verify nahi hua.\n\nPhonePe/GPay/Paytm ka *original screenshot* bhejo. Edited wala nahi chalega.\n\nDobara bhejo ya UPI karo: *${p.upi_id}*`
      : `⚠️ Could not verify this payment screenshot.\n\nPlease send the *original screenshot* directly from PhonePe/GPay/Paytm. Edited images are not accepted.\n\nResend screenshot or pay to: *${p.upi_id}*`;
    await sleep(humanDelay(700, 1200));
    try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
    await sleep(humanDelay(900, 1600));
    try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
    await sleep(120);
    await sendSafe(remoteJid, suspectMsg);

  } else {
    // ── UNCLEAR / NEEDS MANUAL CHECK ────────────────────────────────────────
    await atomicUpdateJsonb(user.id, "wa_appointments", (appts) => {
      const appt = appts.find(b => b.id === pendingAppt.id);
      if (appt) {
        appt.payment_status = "manual_review";
        appt.transaction_id = transaction_id;
        if (banking_name)    appt.banking_name    = banking_name;
        if (screenshot_time) appt.screenshot_time = screenshot_time;
        appt.ai_notes = notes;
      }
      return appts;
    });
    invalidateProfileCache(user.id);
    const reviewMsg = lang === "hindi"
      ? `📋 Screenshot mila. Salon owner manual review karenge aur jald confirm karenge. 🙏\n\nApki booking: *${pendingAppt.service}* — ${friendlyDate(pendingAppt.date)} ${friendlyTime12(pendingAppt.time)}`
      : lang === "hinglish"
      ? `📋 Screenshot mil gaya. Salon owner check karke confirm kar denge. 🙏\n\nBooking: *${pendingAppt.service}* — ${friendlyDate(pendingAppt.date)} ${friendlyTime12(pendingAppt.time)}`
      : `📋 Screenshot received. The salon owner will review and confirm your booking shortly. 🙏\n\nBooking: *${pendingAppt.service}* — ${friendlyDate(pendingAppt.date)} at ${friendlyTime12(pendingAppt.time)}`;
    await sleep(humanDelay(700, 1200));
    try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
    await sleep(humanDelay(900, 1600));
    try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
    await sleep(120);
    await sendSafe(remoteJid, reviewMsg);
  }
}

async function handleMessage(msg, sessionId) {
  const { key, message } = msg;
  const { fromMe, remoteJid } = key;

  // Skip group messages
  if (remoteJid?.endsWith("@g.us")) return;
  // Skip own messages
  if (fromMe) return;
  // Only process DMs — accept both PN (@s.whatsapp.net, @c.us) and LID formats
  if (!remoteJid) return;
  const isPN  = remoteJid.endsWith("@s.whatsapp.net") || remoteJid.endsWith("@c.us");
  const isLID = remoteJid.endsWith("@lid");
  if (!isPN && !isLID) return;

  const senderNum = extractPhoneFromJid(remoteJid, key);

  // Subscribe to typing presence — so we can pause the bot if they're still typing
  // Fire-and-forget: non-fatal if it fails
  try { sock?.presenceSubscribe(remoteJid); } catch {}

  // Fetch user + profile once — reused for all checks below
  let _user = null, _profile = null;
  try {
    _user = await getUserBySession(sessionId);
    if (_user) {
      _profile = await buildProfile(_user);
      const blocked = (_profile.blocked_numbers || []).map(n => String(n).replace(/\D/g,"").replace(/^00/,""));
      if (blocked.includes(senderNum)) { console.log(`[BLOCKED] ${remoteJid}`); return; }
    }
  } catch (e) { console.error("[BLOCKLIST]", e); }

  // Access check
  if (!(await ensureAccess(sessionId))) {
    console.warn(`[ACCESS] denied for ${sessionId}`);
    try {
      // Determine lang from user profile if available
      let aLang = "english";
      try {
        if (_profile) aLang = _profile.language || "english";
      } catch {}
      const accessMsg = aLang === "hindi"
        ? "Is salon ka booking assistant abhi active nahi hai. Seedha salon se contact karein."
        : aLang === "hinglish"
        ? "Bhai yeh bot abhi active nahi hai. Salon se directly poocho."
        : "This salon's booking assistant is currently inactive. Please contact the salon directly.";
      await sock.sendMessage(remoteJid, { text: accessMsg });
    } catch {}
    return;
  }

  // Rate limiting
  const now = Date.now();
  const ul  = (userMessageLog.get(remoteJid) || []).filter(t => t > now - CFG.userCooldownMs);
  const sl  = (sessionMessageLog.get(sessionId) || []).filter(t => t > now - CFG.userCooldownMs);
  ul.push(now); sl.push(now);
  userMessageLog.set(remoteJid, ul);
  sessionMessageLog.set(sessionId, sl);
  if (ul.length > CFG.userMsgLimit) {
    try {
      const rlLang = _profile?.language || "hinglish";
      const rlMsg  = rlLang === "hindi"
        ? "Bhai bahut jaldi messages aa rahe hain. Thodi der ruko. 🙏"
        : rlLang === "english"
        ? "You're sending messages very fast. Please wait a few minutes. 🙏"
        : "Yaar bahut fast messages aa rahe hain. Thoda ruko phir try karo. 🙏";
      await sock.sendMessage(remoteJid, { text: rlMsg });
    } catch {}
    await sleep(CFG.userCooldownMs);
    userMessageLog.set(remoteJid, []);
  }
  if (sl.length > CFG.sessionDailyLimit) {
    try { await sock.sendMessage(remoteJid, { text:
      "The booking assistant is temporarily unavailable. Please try again in 2 hours. 🙏" }); } catch {}
    await sleep(CFG.sessionBlockMs);
    sessionMessageLog.set(sessionId, []);
    return;
  }

  // ── Filter non-conversational message types (ban risk + noise) ─────────────
  if (message) {
    // Skip reactions, protocol events, ephemeral notices, and stub messages
    // These are not real customer messages — responding to them looks bot-like
    if (message.reactionMessage)         return;
    if (message.protocolMessage)         return;
    if (message.ephemeralMessage)        return;
    if (message.senderKeyDistributionMessage) return;
    if (message.messageStubType != null) return;  // System notifications
    if (message.pollUpdateMessage)       return;
    // Skip status broadcasts entirely
    if (remoteJid === "status@broadcast") return;
    // Skip old messages (>5 min) to avoid reply-storm after reconnect
    const msgTs = (msg.messageTimestamp || 0) * 1000;
    if (msgTs > 0 && Date.now() - msgTs > 5 * 60_000) return;
  }

  // ── Opt-out / STOP handling (before any other processing) ──────────────────
  {
    const rawText = (message?.conversation || message?.extendedTextMessage?.text || "").trim().toLowerCase();
    const stopWords = ["stop", "unsubscribe", "opt out", "optout", "dont message", "don't message",
                       "band karo", "mat bhejo", "mujhe message mat", "nahi chahiye", "block"];
    if (stopWords.some(w => rawText === w || rawText.startsWith(w + " "))) {
      const user = await getUserBySession(sessionId);
      if (user) {
        // Add to blocked_numbers list in bot_settings
        try {
          const bs = user.bot_settings
            ? (typeof user.bot_settings === "string" ? JSON.parse(user.bot_settings) : user.bot_settings)
            : {};
          const blocked = bs.blocked_numbers || [];
          if (!blocked.includes(senderNum)) {
            blocked.push(senderNum);
            bs.blocked_numbers = blocked;
            await pool.query("UPDATE users SET bot_settings=$1 WHERE id=$2",
              [JSON.stringify(bs), user.id]);
          invalidateProfileCache(user.id);
          }
        } catch {}
      }
      await sleep(humanDelay(600, 1200));
      try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
      await sleep(humanDelay(800, 1400));
      try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
      await sleep(100);
      await sendSafe(remoteJid,
        "You've been unsubscribed from this salon's WhatsApp bot. " +
        "You won't receive any further messages. Reply START to opt back in. 🙏");
      return;
    }

    // ── Opt-in / START re-subscribe ──────────────────────────────────────────
    if (rawText === "start") {
      const user = await getUserBySession(sessionId);
      if (user) {
        try {
          const bs = user.bot_settings
            ? (typeof user.bot_settings === "string" ? JSON.parse(user.bot_settings) : user.bot_settings)
            : {};
          bs.blocked_numbers = (bs.blocked_numbers || []).filter(n => n !== senderNum);
          await pool.query("UPDATE users SET bot_settings=$1 WHERE id=$2",
            [JSON.stringify(bs), user.id]);
        } catch {}
      }
      await sleep(humanDelay(500, 1000));
      try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
      await sleep(humanDelay(700, 1200));
      try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
      await sleep(100);
      await sendSafe(remoteJid,
        "Welcome back! 😊 You're now subscribed again. How can I help you today?");
      return;
    }
  }

  // ── Audio / voice note handler — transcribe with Whisper ───────────────────
  if (message?.audioMessage) {
    const lang = _profile?.language || "hinglish";
    // Acknowledge immediately so user knows we received it
    const ackMsg = lang === "hindi"
      ? "🎙️ Voice note mila! Samajh raha hoon..."
      : lang === "hinglish"
      ? "🎙️ Voice note sun raha hoon bhai..."
      : "🎙️ Got your voice note! Listening...";
    // Human: read receipt already sent above; now compose before ack
    try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
    await sleep(humanDelay(500, 900));
    try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
    await sleep(80);
    await sendSafe(remoteJid, ackMsg);
    try {
      const { downloadMediaMessage } = await import("@whiskeysockets/baileys");
      const activeSock = sock;
      const buffer = await downloadMediaMessage(
        msg, "buffer", {},
        { logger, reuploadRequest: activeSock?.updateMediaMessage }
      );
      const transcription = await transcribeAudio(buffer, _user?.id || "unknown");
      if (transcription) {
        // Inject transcription as text and let normal flow handle it
        text = `[Voice Note]: "${transcription}"`;
        console.log(`[WHISPER] Transcribed for ${senderNum}: ${transcription.slice(0, 60)}`);
      } else {
        const failMsg = lang === "hindi"
          ? "Voice note clear nahi tha. Kya aap type kar sakte hain? 🙏"
          : lang === "hinglish"
          ? "Yaar voice clear nahi tha. Type karo please. 🙏"
          : "Sorry, couldn't understand the voice note. Could you type instead? 🙏";
        try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
        await sleep(humanDelay(600, 1000));
        try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
        await sleep(80);
        await sendSafe(remoteJid, failMsg);
        return;
      }
    } catch (e) {
      console.error("[AUDIO] Download/transcribe failed:", e.message);
      const errMsg = lang === "hindi"
        ? "Voice note process nahi ho saka. Type karke bhejein. 🙏"
        : "Couldn't process voice note. Please type your message. 🙏";
      try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
      await sleep(humanDelay(500, 900));
      try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
      await sleep(80);
      await sendSafe(remoteJid, errMsg);
      return;
    }
  }

  // ── Image / screenshot message handler ────────────────────────────────────
  if (message?.imageMessage) {
    // Always fetch FRESH user from DB — _user may be stale from before booking was saved
    const freshUser = await getUserBySession(sessionId);
    if (freshUser) {
      const freshProfile = await buildProfile(freshUser);
      if (freshProfile.advance_enabled) {
        const books = getAppointments(freshUser);
        // Match by phone OR remoteJid (handles country-code prefix differences)
        const pendingAppts = books.filter(b =>
          b.payment_status === "pending" &&
          (b.phone === senderNum ||
           b.remoteJid === remoteJid ||
           b.phone === remoteJid.split("@")[0] ||
           senderNum.endsWith(b.phone) ||
           b.phone?.endsWith(senderNum))
        );
        if (pendingAppts.length > 0) {
          await handlePaymentScreenshot(message.imageMessage, key, pendingAppts[0], freshUser, freshProfile, remoteJid, sessionId, senderNum);
          return;
        }
      }
    }
    // No pending payment — check if image has a caption to process
    const imgCaption = message.imageMessage?.caption?.trim();
    if (imgCaption) {
      text = imgCaption;  // use caption as text for AI
    } else {
      return;  // silent image, nothing to respond to
    }
  }

  // Extract text
  if (!message) return;
  let text = "";
  const quoted = message.extendedTextMessage?.contextInfo?.quotedMessage;
  if (quoted) {
    const qText = quoted.conversation || quoted.extendedTextMessage?.text || "";
    text = `QUOTED: "${qText}"\nREPLY: "${message.extendedTextMessage?.text || ""}"`;
  } else {
    text = message.conversation ?? message.extendedTextMessage?.text ?? message.imageMessage?.caption ?? "";
  }
  if (!text.trim()) {
    // Handle other message types customer might send (location, document, sticker, video)
    const hasContent = message && (
      message.documentMessage || message.videoMessage ||
      message.locationMessage  || message.stickerMessage ||
      message.contactMessage
    );
    if (hasContent && _profile) {
      const lang = _profile.language || "hinglish";
      const unsupportedMsg = lang === "hindi"
        ? "Maaf kijiye, main sirf text messages samajh sakta hoon. Apna sawaal type karke bhejein. 🙏"
        : lang === "hinglish"
        ? "Bhai main sirf text samajh sakta hoon. Type karke bhejo please. 🙏"
        : "Sorry, I can only process text messages. Please type your question. 🙏";
      await sleep(humanDelay(800, 1500));
      try { sock?.sendPresenceUpdate("composing", remoteJid); } catch {}
      await sleep(humanDelay(600, 1000));
      try { sock?.sendPresenceUpdate("paused", remoteJid); } catch {}
      await sleep(100);
      await sendSafe(remoteJid, unsupportedMsg);
    }
    return;
  }

  // Natural async read receipt — proportional to message word count, non-blocking
  // (setTimeout ensures it never blocks the event loop for incoming messages)
  {
    const wordCount    = text.split(/\s+/).filter(Boolean).length;
    const readDelayMs  = humanDelay(1000, 2500) + Math.min(wordCount * 240, 6000);
    const capturedSock = sock;
    setTimeout(async () => {
      try { if (capturedSock) await capturedSock.readMessages([key]); }
      catch (e) { console.warn("[READ]", e.message); }
    }, readDelayMs);
  }

  // ── Short ambiguous messages during pending payment — guide customer ─────────
  {
    const trimmedText = text.trim();
    // "paid", "done", "hogaya" type messages also handled here if pending payment
    const isPendingReminder = trimmedText.length <= 4 ||
      /^[?!.,]+$/.test(trimmedText) ||
      /^(ok|done|paid|sent|ho gaya|bhej diya|kar diya|check karo|dekho|yes|haan|ha|ji)$/i.test(trimmedText);
    if (isPendingReminder) {
      // Check if customer has a pending payment — if so, remind them
      const checkUser = await getUserBySession(sessionId);
      if (checkUser) {
        const pendingBook = getAppointments(checkUser).find(b =>
          b.payment_status === "pending" &&
          (b.phone === senderNum || b.remoteJid === remoteJid)
        );
        if (pendingBook) {
          const remLang = checkUser.bot_settings
            ? (typeof checkUser.bot_settings === "string"
                ? JSON.parse(checkUser.bot_settings) : checkUser.bot_settings).language || "hinglish"
            : "hinglish";
          const remindMsg = remLang === "hindi"
            ? `⏳ Aapki *${pendingBook.service}* booking ka slot reserved hai.

Payment screenshot bhejein confirm karne ke liye. 🙏`
            : remLang === "hinglish"
            ? `⏳ Bhai *${pendingBook.service}* ka slot abhi bhi hold hai!

Payment ho gayi? Screenshot bhejo — booking confirm ho jaayegi. ✅`
            : `⏳ Your *${pendingBook.service}* slot is still on hold.

Please send your payment screenshot to confirm booking.`;
          // Human delay: read message, think, then reply
          await humanSend(remoteJid, remindMsg, key, checkUser.bot_settings
            ? (typeof checkUser.bot_settings === "string"
                ? JSON.parse(checkUser.bot_settings) : checkUser.bot_settings).language || "hinglish"
            : "hinglish");
          return;
        }
      }
    }
  }

  // Burst queue
  if (!userProcState.has(remoteJid)) {
    userProcState.set(remoteJid, { burst:[], queue:[], timer:null, isProcessing:false });
  }
  const state = userProcState.get(remoteJid);
  const pushName = msg.pushName || null;
  state.burst.push({ text, ts: now, msgId: key.id, pushName });
  clearTimeout(state.timer);
  state.timer = setTimeout(() => {
    if (state.burst.length) { state.queue.push([...state.burst]); state.burst = []; }
    processUserQueue(sessionId, remoteJid);
  }, CFG.debounceMs);
}

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 16 — SESSION LIFECYCLE (Baileys v7 direct socket)
// ─────────────────────────────────────────────────────────────────────────────

async function ensureSessionFile() {
  let id;
  try { id = (await fs.readFile(SESSION_FILE, "utf8")).trim(); } catch {}
  if (!id) {
    id = crypto.randomBytes(16).toString("hex");
    await fs.writeFile(SESSION_FILE, id, "utf8");
    console.log(`[BOOT] New sessionId: ${id}`);
  }
  return id;
}

/**
 * Build the auth state directory path.
 * Uses useMultiFileAuthState for file-based credential persistence.
 */
function getAuthDir(sessionId) {
  return path.join(AUTH_DIR, `${sessionId}_credentials`);
}

/**
 * Core Baileys connection function.
 * Creates a makeWASocket instance, wires up all event handlers,
 * and manages reconnection logic per the v7 migration guide.
 */
// Track reconnect attempts per session for exponential backoff
const _reconnectAttempts = {};

async function connectBaileys(sessionId) {
  const authDir = getAuthDir(sessionId);
  await fs.mkdir(authDir, { recursive: true });

  const { state, saveCreds } = await useMultiFileAuthState(authDir);

  // Cache Baileys version for 24h — avoids hammering GitHub on every reconnect
  let version;
  const now_v = Date.now();
  if (!connectBaileys._cachedVersion || now_v - connectBaileys._cachedVersionTs > 86_400_000) {
    try {
      ({ version } = await fetchLatestBaileysVersion());
      connectBaileys._cachedVersion   = version;
      connectBaileys._cachedVersionTs = now_v;
    } catch (e) {
      console.warn("[WA] Could not fetch latest version, using cached/default:", e.message);
      version = connectBaileys._cachedVersion;
    }
  } else {
    version = connectBaileys._cachedVersion;
  }

  sock = makeWASocket({
    auth: state,
    logger,
    browser: Browsers.macOS("Chrome"),  // Less detectable than ubuntu/custom strings
    ...(version ? { version } : {}),
    // Mark as unavailable so the phone still receives push notifications
    markOnlineOnConnect: false,
    // Sync only minimal history to speed up connection
    syncFullHistory: false,
    // Connection timeout
    connectTimeoutMs: 60_000,
    // Retry delays
    defaultQueryTimeoutMs: 60_000,
    // Keep-alive pings to WhatsApp servers — prevents idle disconnects
    keepAliveIntervalMs: 25_000,
    // Prevent issues with v7 — let the library manage acks
    emitOwnEvents: true,
    // Retry on read errors
    retryRequestDelayMs: 250,
  });

  // ── Credential persistence ─────────────────────────────────────────────
  sock.ev.on("creds.update", saveCreds);

  // ── Connection state changes ───────────────────────────────────────────
  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    // QR code received — store to DB as base64 data URL
    if (qr) {
      try {
        const dataUrl = await QRCode.toDataURL(qr);
        await pool.query(
          "UPDATE users SET wa_qr_code=$1, wa_status=2 WHERE whatsapp_session_id=$2",
          [dataUrl, sessionId]
        );
        console.log(`[QR] updated for ${sessionId}`);
      } catch (e) { console.error("[QR]", e); }
    }

    // Connection opened
    if (connection === "open") {
      // Get phone number from sock for dashboard display
      let waPhone = null;
      try { waPhone = sock?.user?.id?.split(":")[0] || sock?.user?.phone || null; } catch {}

      await pool.query(
        `UPDATE users SET wa_qr_code=NULL, wa_status=3, whatsapp_connected=TRUE
         ${waPhone ? ", whatsapp_phone=$2" : ""}
         WHERE whatsapp_session_id=$1`,
        waPhone ? [sessionId, waPhone] : [sessionId]
      );
      console.log(`[WA] connected: ${sessionId} phone=${waPhone || "unknown"}`);

      // Start reminder checks (clear previous interval if reconnecting)
      if (reminderInterval) clearInterval(reminderInterval);
      console.log("[REMINDER] Service starting…");
      runReminderChecks(sessionId);
      reminderInterval = setInterval(() => runReminderChecks(sessionId), 60_000);
    }

    // Connection closed
    if (connection === "close") {
      if (isRestarting) return;

      const statusCode = (lastDisconnect?.error)?.output?.statusCode;
      const reason     = lastDisconnect?.error?.message || "unknown";

      console.error(`[WA] disconnected: ${reason} (code=${statusCode})`);

      // ── Logged out: phone removed linked device — need fresh QR scan ────────
      if (statusCode === DisconnectReason.loggedOut) {
        console.error("[WA] Logged out — clearing credentials, awaiting re-scan");
        await pool.query(
          "UPDATE users SET wa_status=1, wa_qr_code=NULL, whatsapp_connected=FALSE WHERE whatsapp_session_id=$1",
          [sessionId]
        ).catch(() => {});
        await fs.rm(authDir, { recursive: true, force: true }).catch(() => {});
        // Don't exit — restart connection so user can re-scan
        _reconnectAttempts[sessionId] = 0;
        await sleep(2000);
        try { await connectBaileys(sessionId); } catch(e) { console.error("[WA] Re-scan start failed:", e.message); process.exit(1); }
        return;
      }

      // ── Connection replaced: another device/session took over ────────────────
      if (statusCode === 440) {
        console.warn("[WA] Connection replaced by another session — reconnecting in 10s");
        await sleep(10_000);
        sock = null;
        if (reminderInterval) { clearInterval(reminderInterval); reminderInterval = null; }
        try { await connectBaileys(sessionId); } catch(e) { console.error("[WA] Reconnect failed:", e.message); process.exit(1); }
        return;
      }

      // ── Restart required (WhatsApp server requests clean reconnect) ──────────
      if (statusCode === 515) {
        console.log("[WA] Restart required by server — reconnecting cleanly");
        sock = null;
        if (reminderInterval) { clearInterval(reminderInterval); reminderInterval = null; }
        await sleep(1500);
        try { await connectBaileys(sessionId); } catch(e) { console.error("[WA] Restart failed:", e.message); process.exit(1); }
        return;
      }

      // ── code=408: QR expired without scan — regenerate QR, don't wipe creds ──
      if (statusCode === 408) {
        console.log("[WA] QR expired (not scanned). Generating new QR...");
        // Update DB: back to QR-ready state (wa_status=2 means showing QR)
        await pool.query(
          "UPDATE users SET wa_status=2, wa_qr_code=NULL WHERE whatsapp_session_id=$1",
          [sessionId]
        ).catch(() => {});
        sock = null;
        await sleep(2000);
        // Reconnect WITHOUT wiping credentials — just get a fresh QR
        try { await connectBaileys(sessionId); } catch(e) {
          console.error("[WA] QR reconnect failed:", e.message);
          process.exit(1);
        }
        return;
      }

      // ── All other reasons: exponential backoff reconnect ────────────────────
      _reconnectAttempts[sessionId] = (_reconnectAttempts[sessionId] || 0) + 1;
      const attempt   = _reconnectAttempts[sessionId];
      const backoffMs = Math.min(3000 * Math.pow(2, attempt - 1), 60_000);

      console.log(`[WA] Reconnecting (attempt ${attempt}, wait ${backoffMs}ms)…`);
      // Only set wa_status=1 if we were previously connected (wa_status=3)
      // Do NOT set wa_status=1 if we're in QR-scan mode — that triggers the restart poll
      await pool.query(
        "UPDATE users SET whatsapp_connected=FALSE WHERE whatsapp_session_id=$1",
        [sessionId]
      ).catch(() => {});

      sock = null;
      if (reminderInterval) { clearInterval(reminderInterval); reminderInterval = null; }

      await sleep(backoffMs);
      try {
        await connectBaileys(sessionId);
        _reconnectAttempts[sessionId] = 0;
      } catch (e) {
        console.error("[WA] Reconnection failed:", e.message);
        if (attempt >= 10) {
          console.error("[WA] 10 consecutive failures — exiting");
          process.exit(1);
        }
      }
    }
  });

  // ── Message events ─────────────────────────────────────────────────────
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    // Only process newly received messages (not history sync)
    if (type !== "notify") return;

    // Process messages independently — don't block on sequential awaits
    for (const msg of messages) {
      handleMessage(msg, sessionId).catch(e => console.error("[MSG-HANDLER]", e));
    }
  });

  // ── Typing presence — pause bot if customer is mid-message ───────────────
  sock.ev.on("presence.update", ({ id, presences }) => {
    for (const [participant, presence] of Object.entries(presences || {})) {
      const state = userProcState.get(participant) || userProcState.get(id);
      if (!state || (!state.queue.length && !state.burst.length)) continue;
      const p = presence.lastKnownPresence;
      if (p === "composing" || p === "recording") {
        // Customer is typing — pause the debounce timer so bot doesn't interrupt
        if (state.timer) { clearTimeout(state.timer); state.timer = null; }
        console.log(`[PAUSE] ${participant} is typing — holding response`);
      } else if ((p === "paused" || p === "available") && !state.timer) {
        // Customer stopped typing — restart the debounce countdown
        state.timer = setTimeout(() => {
          if (state.burst.length) { state.queue.push([...state.burst]); state.burst = []; }
          processUserQueue(sessionId, participant);
        }, CFG.debounceMs);
      }
    }
  });

  return sock;
}

/**
 * Start a fresh session: clear old credentials and connect.
 */
async function startFreshSession(sessionId) {
  try {
    await pool.query(
      "UPDATE users SET wa_status=1, wa_qr_code=NULL WHERE whatsapp_session_id=$1",
      [sessionId]
    );
    const authDir = getAuthDir(sessionId);
    // Only delete THIS session's credentials, not all sessions under AUTH_DIR
    await fs.rm(authDir, { recursive: true, force: true }).catch(() => {});
    await fs.mkdir(authDir, { recursive: true });
    await connectBaileys(sessionId);
    console.log(`[WA] session ${sessionId} started`);
  } catch (err) {
    console.error("[WA] could not start session:", err);
    await pool.query(
      "UPDATE users SET wa_status=1 WHERE whatsapp_session_id=$1",
      [sessionId]
    ).catch(() => {});
    process.exit(1);
  }
}

// ── Dashboard-triggered restart poll ─────────────────────────────────────
// wa_status=9 is the dedicated "please restart" signal written by the dashboard.
// wa_status=1 is normal idle/disconnected — must NOT trigger an exit here.
// ── Batch expiry: check all sessions every 60s for unpaid slots ──────────────
setInterval(async () => {
  if (isRestarting) return;
  try {
    const PAY_TIMEOUT_MS = 10 * 60_000;
    const now = Date.now();
    // Get all users with a connected bot session
    const { rows: users } = await pool.query(
      "SELECT id, wa_appointments, bot_settings, whatsapp_session_id FROM users WHERE wa_status = 3"
    );
    for (const user of users) {
      let books;
      try {
        const raw = user.wa_appointments;
        books = Array.isArray(raw) ? raw : (raw ? Object.values(raw) : []);
      } catch { continue; }

      const expiredIds = books
        .filter(b => b.payment_status === "pending" && b.timestamp &&
                     now - new Date(b.timestamp).getTime() > PAY_TIMEOUT_MS)
        .map(b => b.id);

      if (!expiredIds.length) continue;

      // Read language from bot_settings
      let tLang = "hinglish";
      try {
        const bs = user.bot_settings
          ? (typeof user.bot_settings === "string" ? JSON.parse(user.bot_settings) : user.bot_settings)
          : {};
        tLang = bs.language || "hinglish";
      } catch {}

      // Expire and notify each expired booking
      for (const expId of expiredIds) {
        const b = books.find(x => x.id === expId);
        if (!b) continue;
        b.payment_status = "payment_timeout";
        console.log(`[EXPIRY] Booking ${expId} for user ${user.id} expired (no payment in 10 min)`);

        if (b.remoteJid && sock) {
          const msg = tLang === "hindi"
            ? `⏱️ Aapki *${b.service}* booking ka slot release ho gaya — 10 min mein payment nahi aayi. Dobara book karein.`
            : tLang === "hinglish"
            ? `⏱️ Bhai *${b.service}* ka slot release ho gaya — 10 min mein payment nahi aayi. Phir se book karo! 😊`
            : `⏱️ Your *${b.service}* slot has been released — payment not received within 10 minutes. Please book again.`;
          sendSafe(b.remoteJid, msg).catch(() => {});
        }
      }

      // Save updated appointments atomically
      await atomicUpdateJsonb(user.id, "wa_appointments", (appts) =>
        appts.map(a => expiredIds.includes(a.id)
          ? { ...a, payment_status: "payment_timeout" }
          : a
        )
      );
      invalidateProfileCache(user.id);
    }
  } catch (e) { console.error("[EXPIRY-BATCH]", e.message); }
}, 60_000);

setInterval(async () => {
  if (isRestarting) return;
  // Skip poll when already connected — only needed to detect dashboard restart signal
  try {
    const sessionId = await ensureSessionFile();
    const { rows }  = await pool.query(
      "SELECT wa_status FROM users WHERE whatsapp_session_id=$1 LIMIT 1",
      [sessionId]
    );
    // Skip poll when bot is fully connected (wa_status=3) — saves 8,640 DB queries/day
    if (rows[0]?.wa_status === 3) return;
    if (rows[0]?.wa_status === 9) {
      console.log("[RESTART] Dashboard requested restart (wa_status=9)");
      isRestarting = true;
      // Acknowledge: clear the restart flag before exiting
      await pool.query(
        "UPDATE users SET wa_status=1 WHERE whatsapp_session_id=$1",
        [sessionId]
      ).catch(() => {});
      try { if (sock) { sock.end(undefined); sock = null; } } catch {}
      process.exit(0);  // exit code 0 so systemd does a clean restart
    }
  } catch {}
}, 30_000);

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 17 — BOOT
// ─────────────────────────────────────────────────────────────────────────────

(async () => {
  // Run DB migrations on every boot
  await runMigrations();

  if (!CFG.openaiKey) { console.error("[BOOT] OPENAI_API_KEY not set."); process.exit(1); }
  if (!CFG.dbUrl)     { console.error("[BOOT] DATABASE_URL not set."); process.exit(1); }

  // Auto-create all bot-specific columns
  const migrations = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS whatsapp_session_id VARCHAR(64) UNIQUE",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS wa_status       INTEGER DEFAULT 1",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS wa_qr_code      TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS wa_appointments JSONB",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS wa_chat_history JSONB",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS wa_gcal_creds   JSONB",
    "CREATE INDEX IF NOT EXISTS ix_users_wa_session ON users(whatsapp_session_id)",
  ];

  for (const sql of migrations) {
    try { await pool.query(sql); }
    catch (e) { console.error("[BOOT] migration failed:", sql, e.message); process.exit(1); }
  }
  console.log("[BOOT] DB schema OK");

  const sessionId  = await ensureSessionFile();
  const authDir    = getAuthDir(sessionId);
  const credsExist = await fs.access(authDir).then(() => true).catch(() => false);
  const user       = await getUserBySession(sessionId);

  console.log(`[BOOT] sessionId=${sessionId} wa_status=${user?.wa_status ?? "?"} creds=${credsExist}`);

  if (!user) {
    console.error(`[BOOT] ❌  No user linked to sessionId "${sessionId}"`);
    console.error(`[BOOT]    Run: UPDATE users SET whatsapp_session_id = '${sessionId}' WHERE email = 'your@email.com';`);
    process.exit(1);
  }

  if (credsExist) {
    // Credentials exist — always try to resume/reconnect.
    // Baileys will show a new QR if the session expired (e.g. after 408).
    // This prevents unnecessary credential wipes on every disconnect.
    if (user.wa_status === 3) {
      console.log("[BOOT] Resuming connected session…");
    } else if (user.wa_status === 2) {
      console.log("[BOOT] Resuming QR scan session (credentials exist)…");
    } else {
      console.log("[BOOT] Reconnecting after disconnect (credentials exist)…");
    }
    await connectBaileys(sessionId);
  } else {
    // No credentials at all — this is a true fresh start (first time or post-logout wipe)
    console.log("[BOOT] No credentials found — fresh start, new QR needed…");
    await startFreshSession(sessionId);
  }
})();