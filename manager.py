#!/usr/bin/env python3
"""
SalonFlow Bot Manager v1.0
Manages WhatsApp bot instances across VPS machines.

Usage:
  sudo python3 manager.py 5            # ensure 5 instances + start server
  sudo python3 manager.py 10           # scale to 10 instances
  sudo python3 manager.py --status     # show status table, then exit
  sudo python3 manager.py --update-code  # sync reply.js from backend, restart all
  sudo python3 manager.py --serve      # web server only (no scaling)
"""

# stdlib needed before anything else (venv bootstrap uses these)
import os
import subprocess
import sys
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env_file(Path(__file__).with_name(".env"))

# Configuration -- set these values in the environment before deploying.

DATABASE_URL  = os.environ.get("DATABASE_URL", "").strip()
API_KEY       = os.environ.get("BOT_API_KEY", "").strip()
FLASK_APP_URL = os.environ.get("FLASK_APP_URL", os.environ.get("APP_URL", "http://localhost:5000")).strip()
MANAGER_PORT  = int(os.environ.get("MANAGER_PORT", "8218"))
MANAGER_SERVICE = "salonflow-manager"     # systemd service name for manager itself
MANAGER_SCRIPT  = os.path.abspath(__file__)  # absolute path to this file

# Bot source files live in the same directory as manager.py (the backend folder).
# No bot.zip needed — edit reply.js directly and run --update-code to deploy.
# Bot files are downloaded from the Flask server using the API key.
# Flask serves /bot/reply.js and /bot/package.json (GET, X-API-Key protected).
BOT_FILES = ["reply.js", "package.json"]

# VENV BOOTSTRAP
# Ubuntu 24.04 uses PEP 668 -- system Python blocks pip install.
# We create an isolated venv at /opt/salonflow/venv, install deps there,
# then re-exec this script using the venv's Python (os.execv).
# The re-exec is transparent -- all CLI args are forwarded unchanged.

VENV_DIR = Path("/opt/salonflow/venv")
VENV_PY  = VENV_DIR / "bin" / "python3"
VENV_PIP = VENV_DIR / "bin" / "pip"

# Only bootstrap when NOT already running inside the venv
if str(VENV_DIR) not in sys.prefix:
    # 1. Ensure python3-venv package is present
    subprocess.call(
        ["apt-get", "install", "-y", "-qq", "python3-venv", "python3-full"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    # 2. Create venv if missing
    if not VENV_PY.exists():
        print("[SETUP] Creating virtualenv at /opt/salonflow/venv ...")
        Path("/opt/salonflow").mkdir(parents=True, exist_ok=True)
        subprocess.check_call([sys.executable, "-m", "venv", str(VENV_DIR)])
        print("[SETUP] Virtualenv created OK")

    # 3. Install required packages into venv
    NEED = []
    for pkg, mod in [
        ("psycopg2-binary", "psycopg2"),
        ("Flask",           "flask"),
        ("requests",        "requests"),
        ("tabulate",        "tabulate"),
        ("gunicorn",        "gunicorn"),
    ]:
        rc = subprocess.call(
            [str(VENV_PY), "-c", f"import {mod}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if rc != 0:
            NEED.append(pkg)

    if NEED:
        print(f"[SETUP] Installing into venv: {', '.join(NEED)} ...")
        subprocess.check_call(
            [str(VENV_PIP), "install", "--quiet"] + NEED,
        )
        print("[SETUP] Packages installed OK")

    # 4. Re-exec this script with the venv Python (replaces current process)
    print("[SETUP] Switching to venv Python ...")
    os.execv(str(VENV_PY), [str(VENV_PY)] + sys.argv)
    # Nothing below executes on first launch -- execv replaces the process

# From here we are always running inside the venv

import argparse
import json
import logging
import re
import secrets
import shutil
import socket
import threading
import time
import traceback
from datetime import datetime, timezone
from functools import wraps

import psycopg2
import psycopg2.extras
import requests
from flask import Flask, jsonify, request
from tabulate import tabulate

# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR   = Path("/opt/salonflow")
BOTS_DIR   = BASE_DIR / "bots"
SHARED_DIR = BASE_DIR / "shared"
LOG_DIR    = BASE_DIR / "logs"
BOT_USER   = "salonflow"
NODE_MIN   = 18

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("manager")

_START_TIME = time.time()
_VPS_ID_CACHE = None

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def run_sql(sql, params=None, fetch=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
        conn.commit()

def run_tx(stmts):
    """Execute list of (sql, params) in one transaction."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql, params in stmts:
                cur.execute(sql, params or ())
        conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA BOOTSTRAP
# ─────────────────────────────────────────────────────────────────────────────

def ensure_schema():
    log.info("[DB] Ensuring schema…")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
CREATE TABLE IF NOT EXISTS vps_servers (
    id             SERIAL PRIMARY KEY,
    hostname       VARCHAR(200),
    public_ip      VARCHAR(50) NOT NULL UNIQUE,
    port           INTEGER DEFAULT 8218,
    api_key        VARCHAR(200),
    total_capacity INTEGER DEFAULT 0,
    is_active      BOOLEAN DEFAULT TRUE,
    last_seen      TIMESTAMPTZ,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_instances (
    id             SERIAL PRIMARY KEY,
    vps_id         INTEGER NOT NULL REFERENCES vps_servers(id) ON DELETE CASCADE,
    instance_num   INTEGER NOT NULL,
    session_id     VARCHAR(100) UNIQUE,
    status         VARCHAR(30) DEFAULT 'free',
    service_name   VARCHAR(100),
    install_path   TEXT,
    user_id        INTEGER REFERENCES users(id) ON DELETE SET NULL,
    last_heartbeat TIMESTAMPTZ,
    error_msg      TEXT,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(vps_id, instance_num)
);

CREATE INDEX IF NOT EXISTS ix_bot_inst_status  ON bot_instances(status);
CREATE INDEX IF NOT EXISTS ix_bot_inst_vps     ON bot_instances(vps_id);
CREATE INDEX IF NOT EXISTS ix_bot_inst_session ON bot_instances(session_id);

CREATE TABLE IF NOT EXISTS bot_config (
    key         VARCHAR(100) PRIMARY KEY,
    value       TEXT,
    description TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO bot_config(key, value, description) VALUES
    ('OPENAI_API_KEY',  '', 'OpenAI API key for all bot instances')
    ON CONFLICT(key) DO NOTHING;
INSERT INTO bot_config(key, value, description) VALUES
    ('DEBOUNCE_MS',     '7000', 'Message burst debounce ms')
    ON CONFLICT(key) DO NOTHING;
INSERT INTO bot_config(key, value, description) VALUES
    ('APPT_BUFFER_MIN', '15', 'Gap between appointments (minutes)')
    ON CONFLICT(key) DO NOTHING;
INSERT INTO bot_config(key, value, description) VALUES
    ('BOT_VERSION', '1', 'Incremented on each code update')
    ON CONFLICT(key) DO NOTHING;
""")
        conn.commit()
    log.info("[DB] Schema OK")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG TABLE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_config(key, default=""):
    row = run_sql("SELECT value FROM bot_config WHERE key=%s", (key,), fetch="one")
    return row["value"] if row else default

def set_config(key, value):
    run_sql("""
        INSERT INTO bot_config(key, value, updated_at)
        VALUES (%s,%s,NOW())
        ON CONFLICT(key) DO UPDATE SET value=%s, updated_at=NOW()
    """, (key, value, value))

def get_all_config():
    rows = run_sql("SELECT key, value FROM bot_config", fetch="all") or []
    return {r["key"]: r["value"] for r in rows}

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def run_cmd(cmd, cwd=None, capture=False, check=True):
    result = subprocess.run(
        cmd, shell=True, cwd=cwd,
        capture_output=capture, text=True,
    )
    if check and result.returncode != 0:
        msg = result.stderr.strip() if capture else f"exit {result.returncode}"
        raise RuntimeError(f"Command failed [{cmd}]: {msg}")
    return result.returncode, result.stdout if capture else "", result.stderr if capture else ""

def get_node_version():
    try:
        _, out, _ = run_cmd("node --version", capture=True, check=False)
        m = re.search(r"v(\d+)", out)
        return int(m.group(1)) if m else 0
    except Exception:
        return 0

def install_nodejs():
    ver = get_node_version()
    if ver >= NODE_MIN:
        log.info(f"[SETUP] Node.js v{ver} already installed OK")
        return
    log.info(f"[SETUP] Installing Node.js LTS (need >= {NODE_MIN})…")
    run_cmd("apt-get update -qq")
    run_cmd("apt-get install -y -qq curl ca-certificates")
    run_cmd("curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -")
    run_cmd("apt-get install -y -qq nodejs")
    log.info(f"[SETUP] Node.js v{get_node_version()} installed")

def install_system_deps():
    log.info("[SETUP] Installing OS packages…")
    run_cmd("apt-get install -y -qq python3-pip unzip curl ca-certificates build-essential")
    log.info("[SETUP] OS packages OK")

def ensure_bot_user():
    rc, _, _ = run_cmd(f"id {BOT_USER}", capture=True, check=False)
    if rc != 0:
        log.info(f"[SETUP] Creating system user '{BOT_USER}'…")
        run_cmd(f"useradd --system --no-create-home --shell /usr/sbin/nologin {BOT_USER}")
    else:
        log.info(f"[SETUP] User '{BOT_USER}' exists OK")

def get_public_ip():
    for url in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
        try:
            r = requests.get(url, timeout=5)
            ip = r.text.strip()
            if re.match(r"^\d+\.\d+\.\d+\.\d+$", ip):
                return ip
        except Exception:
            pass
    return socket.gethostbyname(socket.gethostname())

def ensure_directories():
    for d in [BASE_DIR, BOTS_DIR, SHARED_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    run_cmd(f"chown -R {BOT_USER}:{BOT_USER} {BASE_DIR}", check=False)
    log.info(f"[SETUP] Directories ready under {BASE_DIR}")

# ─────────────────────────────────────────────────────────────────────────────
# BOT SOURCE — copy from backend folder (same dir as manager.py)
# ─────────────────────────────────────────────────────────────────────────────

BOT_FILES = ["reply.js", "package.json"]

def sync_bot_source():
    """Download reply.js and package.json from the Flask server."""
    SHARED_DIR.mkdir(parents=True, exist_ok=True)
    for fname in BOT_FILES:
        url = f"{FLASK_APP_URL.rstrip('/')}/bot/{fname}"
        log.info(f"[BOT] Downloading {fname} from {url}…")
        resp = requests.get(url, headers={"X-API-Key": API_KEY}, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to download {fname}: HTTP {resp.status_code} — {resp.text[:200]}"
            )
        dest = SHARED_DIR / fname
        dest.write_bytes(resp.content)
        log.info(f"[BOT] Saved {fname} ({len(resp.content)} bytes) → {dest}")

def npm_install_shared():
    if not (SHARED_DIR / "package.json").exists():
        raise FileNotFoundError("package.json missing in shared/ — run sync_bot_source() first")
    log.info("[BOT] npm install (shared)…")
    run_cmd("npm install --omit=dev", cwd=str(SHARED_DIR))
    run_cmd(f"chown -R {BOT_USER}:{BOT_USER} {SHARED_DIR}", check=False)
    log.info("[BOT] npm install done")

def setup_shared_bot():
    """Initial setup: copy bot files from backend and install npm deps."""
    sync_bot_source()
    npm_install_shared()

# ─────────────────────────────────────────────────────────────────────────────
# INSTANCE CREATION
# ─────────────────────────────────────────────────────────────────────────────

def instance_dir(n: int) -> Path:
    return BOTS_DIR / f"instance_{n}"

def svc_name(n: int) -> str:
    return f"salonflow-bot-{n}"

def write_env(idir: Path):
    """Write .env from bot_config table + local DATABASE_URL."""
    cfg = get_all_config()
    lines = [
        f"DATABASE_URL={DATABASE_URL}",
        f"OPENAI_API_KEY={cfg.get('OPENAI_API_KEY', '')}",
        f"DEBOUNCE_MS={cfg.get('DEBOUNCE_MS', '7000')}",
        f"APPT_BUFFER_MIN={cfg.get('APPT_BUFFER_MIN', '15')}",
    ]
    (idir / ".env").write_text("\n".join(lines) + "\n")

def create_instance(n: int, vps_id: int) -> dict:
    idir = instance_dir(n)
    idir.mkdir(parents=True, exist_ok=True)

    # Symlinks to shared code + node_modules
    for name in ["reply.js", "package.json", "node_modules"]:
        src  = SHARED_DIR / name
        dest = idir / name
        if dest.is_symlink():
            dest.unlink()
        elif dest.exists() and name == "node_modules":
            shutil.rmtree(dest)
        if src.exists():
            dest.symlink_to(src)

    # Per-instance state dirs — create and immediately chown
    (idir / "wa_credentials").mkdir(exist_ok=True)

    write_env(idir)
    run_cmd(f"chown -R {BOT_USER}:{BOT_USER} {idir}", check=False)
    run_cmd(f"chmod -R 755 {idir}", check=False)

    svc = svc_name(n)
    create_systemd_unit(n, idir)

    row = run_sql("""
        INSERT INTO bot_instances
            (vps_id, instance_num, status, service_name, install_path, created_at, updated_at)
        VALUES (%s,%s,'free',%s,%s,NOW(),NOW())
        ON CONFLICT(vps_id, instance_num) DO UPDATE
            SET service_name=%s, install_path=%s, updated_at=NOW()
        RETURNING *
    """, (vps_id, n, svc, str(idir), svc, str(idir)), fetch="one")

    log.info(f"[BOT] Instance {n} created at {idir}")
    return dict(row)

def create_systemd_unit(n: int, idir: Path):
    svc       = svc_name(n)
    node_bin  = shutil.which("node") or "/usr/bin/node"
    unit_text = f"""[Unit]
Description=SalonFlow WhatsApp Bot instance {n}
After=network.target
StartLimitIntervalSec=120
StartLimitBurst=5

[Service]
Type=simple
User={BOT_USER}
WorkingDirectory={idir}
ExecStartPre=/bin/bash -c 'mkdir -p {idir}/wa_credentials && chown -R {BOT_USER}:{BOT_USER} {idir}'
ExecStart={node_bin} reply.js
Restart=on-failure
RestartSec=15
StartLimitIntervalSec=300
StartLimitBurst=10
StandardOutput=append:{LOG_DIR}/bot_{n}.log
StandardError=append:{LOG_DIR}/bot_{n}.err
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
"""
    unit_path = Path(f"/etc/systemd/system/{svc}.service")
    unit_path.write_text(unit_text)
    run_cmd("systemctl daemon-reload")
    log.info(f"[SYSTEMD] {unit_path} written")

# ─────────────────────────────────────────────────────────────────────────────
# SERVICE CONTROLS
# ─────────────────────────────────────────────────────────────────────────────

def svc_start(svc: str):
    run_cmd(f"systemctl enable --now {svc}", check=False)

def svc_stop(svc: str):
    run_cmd(f"systemctl stop {svc}", check=False)

def svc_restart(svc: str):
    run_cmd(f"systemctl restart {svc}", check=False)

def svc_active(svc: str) -> bool:
    rc, _, _ = run_cmd(f"systemctl is-active {svc}", capture=True, check=False)
    return rc == 0

def svc_remove(svc: str):
    svc_stop(svc)
    run_cmd(f"systemctl disable {svc}", check=False)
    p = Path(f"/etc/systemd/system/{svc}.service")
    p.unlink(missing_ok=True)
    run_cmd("systemctl daemon-reload")
    log.info(f"[SYSTEMD] {svc} removed")

# ─────────────────────────────────────────────────────────────────────────────
# VPS REGISTRATION
# ─────────────────────────────────────────────────────────────────────────────

def register_vps(capacity: int) -> int:
    global _VPS_ID_CACHE
    ip  = get_public_ip()
    hn  = socket.gethostname()
    row = run_sql("""
        INSERT INTO vps_servers(hostname, public_ip, port, api_key, total_capacity, is_active, last_seen)
        VALUES (%s,%s,%s,%s,%s,TRUE,NOW())
        ON CONFLICT(public_ip) DO UPDATE
            SET hostname=%s, port=%s, api_key=%s, total_capacity=%s, is_active=TRUE, last_seen=NOW()
        RETURNING id
    """, (hn, ip, MANAGER_PORT, API_KEY, capacity,
          hn, MANAGER_PORT, API_KEY, capacity), fetch="one")
    _VPS_ID_CACHE = row["id"]
    log.info(f"[VPS] Registered id={_VPS_ID_CACHE} ip={ip}")
    return _VPS_ID_CACHE

def get_vps_id() -> int:
    global _VPS_ID_CACHE
    if _VPS_ID_CACHE:
        return _VPS_ID_CACHE
    ip  = get_public_ip()
    row = run_sql("SELECT id FROM vps_servers WHERE public_ip=%s", (ip,), fetch="one")
    if not row:
        raise RuntimeError("VPS not registered. Run: sudo python3 manager.py <N>")
    _VPS_ID_CACHE = row["id"]
    return _VPS_ID_CACHE

# ─────────────────────────────────────────────────────────────────────────────
# SCALING
# ─────────────────────────────────────────────────────────────────────────────

def scale_instances(target: int):
    vps_id = register_vps(target)
    existing = run_sql(
        "SELECT instance_num FROM bot_instances WHERE vps_id=%s ORDER BY instance_num",
        (vps_id,), fetch="all"
    ) or []
    have = {r["instance_num"] for r in existing}
    new_count = 0
    for n in range(1, target + 1):
        if n not in have:
            log.info(f"[SCALE] Creating instance {n}…")
            create_instance(n, vps_id)
            new_count += 1
        else:
            log.info(f"[SCALE] Instance {n} already exists")
    total = max(target, len(have) + new_count)
    run_sql("UPDATE vps_servers SET total_capacity=%s WHERE id=%s", (total, vps_id))
    log.info(f"[SCALE] Done. total={total} new={new_count}")

# ─────────────────────────────────────────────────────────────────────────────
# BOT ASSIGNMENT / RELEASE
# ─────────────────────────────────────────────────────────────────────────────

def assign_bot(session_id: str, user_id: int) -> dict:
    vps_id = get_vps_id()

    # Atomically claim a free slot
    row = run_sql("""
        UPDATE bot_instances
        SET status='starting', session_id=%s, user_id=%s, updated_at=NOW()
        WHERE id = (
            SELECT id FROM bot_instances
            WHERE vps_id=%s AND status='free'
            ORDER BY instance_num
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
    """, (session_id, user_id, vps_id), fetch="one")

    if not row:
        raise RuntimeError("No free bot instances available on this VPS.")

    idir = Path(row["install_path"])

    # Write sessionID.txt (reply.js reads this to know its WA session)
    (idir / "sessionID.txt").write_text(session_id)

    # Link session to user in main DB
    run_sql(
        "UPDATE users SET whatsapp_session_id=%s WHERE id=%s",
        (session_id, user_id)
    )

    # Refresh .env (pick up latest OPENAI_API_KEY etc.)
    write_env(idir)

    # Start systemd service
    svc_start(row["service_name"])
    log.info(f"[ASSIGN] Instance {row['instance_num']} → session={session_id} user={user_id}")
    return dict(row)

def release_bot(session_id: str) -> dict:
    row = run_sql(
        "SELECT * FROM bot_instances WHERE session_id=%s",
        (session_id,), fetch="one"
    )
    if not row:
        raise RuntimeError(f"No instance found for session {session_id}")

    svc_stop(row["service_name"])

    idir = Path(row["install_path"])
    (idir / "sessionID.txt").unlink(missing_ok=True)

    # Remove WA credentials so the slot is truly clean
    cred = idir / "wa_credentials"
    if cred.exists():
        shutil.rmtree(cred)
    cred.mkdir(exist_ok=True)
    # chown after recreating — manager runs as root so mkdir creates root:root
    run_cmd(f"chown -R {BOT_USER}:{BOT_USER} {cred}", check=False)

    run_sql("""
        UPDATE bot_instances
        SET status='free', session_id=NULL, user_id=NULL,
            last_heartbeat=NULL, error_msg=NULL, updated_at=NOW()
        WHERE id=%s
    """, (row["id"],))

    run_sql(
        "UPDATE users SET whatsapp_session_id=NULL, whatsapp_connected=FALSE WHERE whatsapp_session_id=%s",
        (session_id,)
    )

    log.info(f"[RELEASE] Instance {row['instance_num']} freed")
    return dict(row)

# ─────────────────────────────────────────────────────────────────────────────
# BOT CODE UPDATE
# ─────────────────────────────────────────────────────────────────────────────

def update_bot_code() -> dict:
    log.info("[UPDATE] Syncing bot code from backend folder…")
    vps_id = get_vps_id()

    # Copy reply.js and package.json from backend folder → shared/
    sync_bot_source()
    npm_install_shared()

    # Refresh .env and restart running instances
    rows = run_sql(
        "SELECT * FROM bot_instances WHERE vps_id=%s ORDER BY instance_num",
        (vps_id,), fetch="all"
    ) or []

    restarted = 0
    for r in rows:
        write_env(Path(r["install_path"]))
        if r["status"] in ("starting", "connected"):
            svc_restart(r["service_name"])
            restarted += 1
            log.info(f"[UPDATE] Restarted instance {r['instance_num']}")

    ver = int(get_config("BOT_VERSION", "1"))
    set_config("BOT_VERSION", str(ver + 1))
    run_sql("UPDATE vps_servers SET last_seen=NOW() WHERE id=%s", (vps_id,))
    log.info(f"[UPDATE] Done. version={ver+1} restarted={restarted}")
    return {"updated": True, "version": ver + 1, "restarted": restarted}

# ─────────────────────────────────────────────────────────────────────────────
# HEARTBEAT — reconcile systemd state with DB every 30s
# ─────────────────────────────────────────────────────────────────────────────

def sync_status():
    try:
        vps_id = get_vps_id()
        rows   = run_sql(
            "SELECT * FROM bot_instances WHERE vps_id=%s",
            (vps_id,), fetch="all"
        ) or []
        for r in rows:
            alive  = svc_active(r["service_name"])
            new_st = r["status"]

            if r["status"] == "free" and alive:
                svc_stop(r["service_name"])

            elif r["status"] in ("starting", "connected"):
                if not alive:
                    new_st = "error"
                elif r["session_id"]:
                    u = run_sql(
                        "SELECT whatsapp_connected FROM users WHERE whatsapp_session_id=%s",
                        (r["session_id"],), fetch="one"
                    )
                    if u and u["whatsapp_connected"]:
                        new_st = "connected"

            if new_st != r["status"]:
                run_sql(
                    "UPDATE bot_instances SET status=%s, updated_at=NOW() WHERE id=%s",
                    (new_st, r["id"])
                )

        run_sql("UPDATE vps_servers SET last_seen=NOW() WHERE id=%s", (vps_id,))
    except Exception as e:
        log.warning(f"[HEARTBEAT] {e}")

def start_heartbeat():
    def loop():
        while True:
            time.sleep(30)
            sync_status()
    threading.Thread(target=loop, daemon=True, name="heartbeat").start()
    log.info("[HEARTBEAT] Started (30s interval)")

# ─────────────────────────────────────────────────────────────────────────────
# WEEKLY CRON
# ─────────────────────────────────────────────────────────────────────────────

def install_manager_service():
    """
    Fully automatic production setup:
      - Installs gunicorn into venv (no manual steps)
      - Copies manager.py to stable path /opt/salonflow/manager.py
      - Writes systemd unit: 2 workers x 4 threads = 8 concurrent requests
      - Opens firewall port 8218 via ufw
      - Enables + starts service, waits to confirm it is live
      - Prints public URL — nothing else needed
    """
    import shutil as _sh
    import time   as _t

    # 1. Ensure gunicorn is in the venv
    venv_gunicorn = VENV_DIR / "bin" / "gunicorn"
    if not venv_gunicorn.exists():
        log.info("[MANAGER SVC] Installing gunicorn into venv...")
        subprocess.check_call([str(VENV_PIP), "install", "--quiet", "gunicorn"])
        log.info("[MANAGER SVC] gunicorn installed OK")
    else:
        log.info("[MANAGER SVC] gunicorn already installed OK")

    # 2. Copy manager.py to stable location so systemd always finds it
    stable_script = BASE_DIR / "manager.py"
    current_script = Path(MANAGER_SCRIPT).resolve()
    if current_script != stable_script.resolve():
        _sh.copy2(str(current_script), str(stable_script))
        log.info(f"[MANAGER SVC] Copied manager.py to {stable_script}")

    venv_python = str(VENV_DIR / "bin" / "python3")
    venv_gunicorn_bin = str(VENV_DIR / "bin" / "gunicorn")

    # 3. Write the systemd unit file
    #    2 workers x 4 threads (gthread) = 8 concurrent requests
    #    ExecStartPre re-runs setup/scale on every reboot safely
    unit_lines = [
        "[Unit]",
        "Description=SalonFlow Bot Manager API (gunicorn)",
        "After=network.target",
        "StartLimitIntervalSec=60",
        "StartLimitBurst=5",
        "",
        "[Service]",
        "Type=simple",
        "User=root",
        f"WorkingDirectory={BASE_DIR}",
        f"ExecStartPre={venv_python} {stable_script} --no-server",
        (
            f"ExecStart={venv_gunicorn_bin}"
            " --workers 2"
            " --worker-class gthread"
            " --threads 4"
            f" --bind 0.0.0.0:{MANAGER_PORT}"
            " --timeout 120"
            " --keep-alive 5"
            f" --access-logfile {LOG_DIR}/manager_access.log"
            f" --error-logfile {LOG_DIR}/manager_error.log"
            " --log-level info"
            " --capture-output"
            f" --chdir {BASE_DIR}"
            " manager:flask_app"
        ),
        "Restart=always",
        "RestartSec=5",
        "KillMode=mixed",
        "TimeoutStopSec=30",
        f"StandardOutput=append:{LOG_DIR}/manager.log",
        f"StandardError=append:{LOG_DIR}/manager.log",
        "",
        "[Install]",
        "WantedBy=multi-user.target",
        "",
    ]
    unit_text = "\n".join(unit_lines)
    unit_path = Path(f"/etc/systemd/system/{MANAGER_SERVICE}.service")
    unit_path.write_text(unit_text)
    log.info(f"[MANAGER SVC] Unit file written: {unit_path}")

    # 4. Open firewall port 8218 (silent if ufw not active)
    run_cmd(f"ufw allow {MANAGER_PORT}/tcp", check=False)
    log.info(f"[MANAGER SVC] Firewall: port {MANAGER_PORT} allowed")

    # 5. Reload systemd, enable service to start on reboot, restart now
    run_cmd("systemctl daemon-reload")
    run_cmd(f"systemctl enable {MANAGER_SERVICE}")
    run_cmd(f"systemctl restart {MANAGER_SERVICE}")

    # 6. Poll for up to 15s to confirm the service is active
    log.info("[MANAGER SVC] Waiting for service to come up...")
    alive = False
    for _ in range(15):
        _t.sleep(1)
        rc, _, _ = run_cmd(
            f"systemctl is-active {MANAGER_SERVICE}",
            capture=True, check=False
        )
        if rc == 0:
            alive = True
            break

    if alive:
        ip = get_public_ip()
        log.info("")
        log.info("=" * 60)
        log.info("  Manager API is LIVE")
        log.info(f"  URL  : http://{ip}:{MANAGER_PORT}/health")
        log.info(f"  Key  : X-API-Key: {API_KEY}")
        log.info(f"  Logs : {LOG_DIR}/manager_access.log")
        log.info(f"  Ctrl : systemctl status {MANAGER_SERVICE}")
        log.info("=" * 60)
        log.info("")
    else:
        _, journal, _ = run_cmd(
            f"journalctl -u {MANAGER_SERVICE} -n 40 --no-pager",
            capture=True, check=False
        )
        log.error(f"[MANAGER SVC] Service did not start! Journal:\n{journal}")

def install_cron():
    script = (
        "#!/bin/bash\n"
        "# SalonFlow weekly bot update — managed by manager.py\n"
        f"curl -sf -X POST http://127.0.0.1:{MANAGER_PORT}/update \\\n"
        f'  -H "X-API-Key: {API_KEY}" \\\n'
        f"  -H 'Content-Type: application/json' >> {LOG_DIR}/weekly_update.log 2>&1\n"
    )
    p = Path("/etc/cron.weekly/salonflow-bot-update")
    p.write_text(script)
    p.chmod(0o755)
    log.info(f"[CRON] Weekly update installed at {p}")

# ─────────────────────────────────────────────────────────────────────────────
# CLI STATUS DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

def print_status():
    try:
        vps_id = get_vps_id()
    except Exception:
        print("VPS not registered. Run: sudo python3 manager.py <N>")
        return

    rows = run_sql("""
        SELECT bi.instance_num, bi.status, bi.session_id, bi.service_name,
               bi.last_heartbeat, bi.error_msg, u.salon_name, u.whatsapp_connected
        FROM bot_instances bi
        LEFT JOIN users u ON u.id = bi.user_id
        WHERE bi.vps_id=%s
        ORDER BY bi.instance_num
    """, (vps_id,), fetch="all") or []

    table = []
    for r in rows:
        alive  = "🟢" if svc_active(r["service_name"]) else "🔴"
        hb     = str(r["last_heartbeat"])[:19] if r["last_heartbeat"] else "—"
        table.append([
            r["instance_num"], r["status"], alive,
            (r["session_id"] or "—")[:22],
            r["salon_name"] or "—",
            "✓" if r["whatsapp_connected"] else "—",
            hb,
        ])

    print(f"\n{'━'*72}")
    print(f"  SalonFlow Manager  |  VPS id={vps_id}  |  {get_public_ip()}:{MANAGER_PORT}")
    print(f"{'━'*72}")
    print(tabulate(table,
        headers=["#","Status","Svc","Session ID","Salon","WA","Heartbeat"],
        tablefmt="rounded_grid"))

    free  = sum(1 for r in rows if r["status"] == "free")
    conn  = sum(1 for r in rows if r["status"] == "connected")
    total = len(rows)
    print(f"\n  Total={total}  Free={free}  Connected={conn}  Other={total-free-conn}")
    print(f"{'━'*72}\n")

# ─────────────────────────────────────────────────────────────────────────────
# FLASK WEB SERVER
# ─────────────────────────────────────────────────────────────────────────────

flask_app = Flask("salonflow-manager")

def require_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        key = (
            request.headers.get("X-API-Key")
            or (request.get_json(silent=True) or {}).get("api_key", "")
        )
        if not API_KEY or not secrets.compare_digest(str(key), API_KEY):
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

def ok(**kw):
    return jsonify({"ok": True, **kw})

def fail(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code

# ── Health & stats ────────────────────────────────────────────────────────────

@flask_app.route("/health", methods=["GET"])
@require_key
def ep_health():
    try:
        vps_id = get_vps_id()
        rows   = run_sql(
            "SELECT status, COUNT(*) AS n FROM bot_instances WHERE vps_id=%s GROUP BY status",
            (vps_id,), fetch="all"
        ) or []
        stats = {r["status"]: r["n"] for r in rows}
        total = sum(stats.values())
        return ok(
            vps_id      = vps_id,
            public_ip   = get_public_ip(),
            port        = MANAGER_PORT,
            total       = total,
            free        = stats.get("free", 0),
            connected   = stats.get("connected", 0),
            starting    = stats.get("starting", 0),
            error       = stats.get("error", 0),
            stopped     = stats.get("stopped", 0),
            node_ver    = get_node_version(),
            uptime_s    = int(time.time() - _START_TIME),
            bot_version = get_config("BOT_VERSION", "1"),
        )
    except Exception as e:
        return fail(str(e), 500)

@flask_app.route("/status", methods=["GET"])
@require_key
def ep_status():
    try:
        vps_id = get_vps_id()
        rows   = run_sql("""
            SELECT bi.*, u.salon_name, u.whatsapp_connected, u.email
            FROM bot_instances bi
            LEFT JOIN users u ON u.id = bi.user_id
            WHERE bi.vps_id=%s
            ORDER BY bi.instance_num
        """, (vps_id,), fetch="all") or []
        out = []
        for r in rows:
            d = dict(r)
            d["service_active"] = svc_active(r["service_name"])
            for k in ("created_at", "updated_at", "last_heartbeat"):
                d[k] = str(d.get(k) or "")
            out.append(d)
        return ok(instances=out)
    except Exception as e:
        return fail(str(e), 500)

# ── Scaling ───────────────────────────────────────────────────────────────────

@flask_app.route("/instances/scale", methods=["POST"])
@require_key
def ep_scale():
    data  = request.get_json(silent=True) or {}
    count = data.get("count")
    if not isinstance(count, int) or count < 1:
        return fail("count must be a positive integer")
    try:
        scale_instances(count)
        return ok(message=f"Scaled to {count} instances")
    except Exception as e:
        log.exception("[SCALE]")
        return fail(str(e), 500)

# ── Assignment ────────────────────────────────────────────────────────────────

@flask_app.route("/bot/assign", methods=["POST"])
@require_key
def ep_assign():
    data       = request.get_json(silent=True) or {}
    session_id = data.get("session_id", "").strip()
    user_id    = data.get("user_id")
    if not session_id or not user_id:
        return fail("session_id and user_id are required")
    try:
        inst = assign_bot(session_id, int(user_id))
        return ok(instance=inst)
    except RuntimeError as e:
        return fail(str(e), 409)
    except Exception as e:
        log.exception("[ASSIGN]")
        return fail(str(e), 500)

@flask_app.route("/bot/release", methods=["POST"])
@require_key
def ep_release():
    data       = request.get_json(silent=True) or {}
    session_id = data.get("session_id", "").strip()
    if not session_id:
        return fail("session_id is required")
    try:
        inst = release_bot(session_id)
        return ok(instance=inst)
    except RuntimeError as e:
        return fail(str(e), 404)
    except Exception as e:
        log.exception("[RELEASE]")
        return fail(str(e), 500)

# ── Per-instance controls ─────────────────────────────────────────────────────

def _get_inst(inst_id: int):
    vps_id = get_vps_id()
    return run_sql(
        "SELECT * FROM bot_instances WHERE id=%s AND vps_id=%s",
        (inst_id, vps_id), fetch="one"
    )

@flask_app.route("/bot/<int:inst_id>/start", methods=["POST"])
@require_key
def ep_start(inst_id):
    r = _get_inst(inst_id)
    if not r:
        return fail("Instance not found", 404)
    svc_start(r["service_name"])
    run_sql("UPDATE bot_instances SET status='starting', updated_at=NOW() WHERE id=%s", (inst_id,))
    return ok(message=f"Started {r['service_name']}")

@flask_app.route("/bot/<int:inst_id>/stop", methods=["POST"])
@require_key
def ep_stop(inst_id):
    r = _get_inst(inst_id)
    if not r:
        return fail("Instance not found", 404)
    svc_stop(r["service_name"])
    run_sql("UPDATE bot_instances SET status='stopped', updated_at=NOW() WHERE id=%s", (inst_id,))
    return ok(message=f"Stopped {r['service_name']}")

@flask_app.route("/bot/<int:inst_id>/restart", methods=["POST"])
@require_key
def ep_restart(inst_id):
    r = _get_inst(inst_id)
    if not r:
        return fail("Instance not found", 404)
    write_env(Path(r["install_path"]))
    svc_restart(r["service_name"])
    run_sql("UPDATE bot_instances SET updated_at=NOW() WHERE id=%s", (inst_id,))
    return ok(message=f"Restarted {r['service_name']}")

@flask_app.route("/bot/<int:inst_id>", methods=["DELETE"])
@require_key
def ep_delete(inst_id):
    r = _get_inst(inst_id)
    if not r:
        return fail("Instance not found", 404)
    if r["status"] in ("starting", "connected"):
        return fail("Stop the bot first: POST /bot/{id}/stop", 409)

    svc_remove(r["service_name"])

    data    = request.get_json(silent=True) or {}
    rm_dir  = data.get("remove_files", False)
    if rm_dir:
        idir = Path(r["install_path"])
        if idir.exists():
            shutil.rmtree(idir)

    run_sql("DELETE FROM bot_instances WHERE id=%s", (inst_id,))
    vps_id = get_vps_id()
    run_sql("""
        UPDATE vps_servers SET total_capacity=(
            SELECT COUNT(*) FROM bot_instances WHERE vps_id=%s
        ) WHERE id=%s
    """, (vps_id,))
    return ok(message=f"Instance {inst_id} deleted")

# ── Code update ───────────────────────────────────────────────────────────────

@flask_app.route("/update", methods=["POST"])
@require_key
def ep_update():
    try:
        result = update_bot_code()
        return ok(**result)
    except Exception as e:
        log.exception("[UPDATE]")
        return fail(str(e), 500)

# ── Config sync ───────────────────────────────────────────────────────────────

@flask_app.route("/config/sync", methods=["POST"])
@require_key
def ep_config_sync():
    try:
        vps_id = get_vps_id()
        rows   = run_sql(
            "SELECT install_path FROM bot_instances WHERE vps_id=%s",
            (vps_id,), fetch="all"
        ) or []
        n = 0
        for r in rows:
            write_env(Path(r["install_path"]))
            n += 1
        return ok(message=f"Synced .env on {n} instances")
    except Exception as e:
        return fail(str(e), 500)

# ── Logs ──────────────────────────────────────────────────────────────────────

@flask_app.route("/bot/<int:inst_id>/logs", methods=["GET"])
@require_key
def ep_logs(inst_id):
    r = _get_inst(inst_id)
    if not r:
        return fail("Instance not found", 404)
    lines = int(request.args.get("lines", 100))
    try:
        lf = LOG_DIR / f"bot_{r['instance_num']}.log"
        if not lf.exists():
            return ok(logs="No log file yet.")
        _, out, _ = run_cmd(f"tail -n {lines} {lf}", capture=True, check=False)
        return ok(logs=out)
    except Exception as e:
        return fail(str(e), 500)

# ── Config get/set ────────────────────────────────────────────────────────────

@flask_app.route("/config", methods=["GET"])
@require_key
def ep_config_get():
    try:
        rows = run_sql("SELECT key, value, description FROM bot_config ORDER BY key", fetch="all") or []
        return ok(config=[dict(r) for r in rows])
    except Exception as e:
        return fail(str(e), 500)

@flask_app.route("/config", methods=["POST"])
@require_key
def ep_config_set():
    data = request.get_json(silent=True) or {}
    updates = {k: v for k, v in data.items() if k not in ("api_key",)}
    if not updates:
        return fail("No config keys provided")
    try:
        for k, v in updates.items():
            set_config(k, str(v))
        return ok(updated=list(updates.keys()))
    except Exception as e:
        return fail(str(e), 500)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SalonFlow Bot Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  sudo python3 manager.py 5            ensure 5 instances + start server
  sudo python3 manager.py 10           scale up to 10
  sudo python3 manager.py --status     show status table
  sudo python3 manager.py --update-code sync reply.js from backend, restart all
  sudo python3 manager.py --serve      web server only
        """,
    )
    parser.add_argument("instances", nargs="?", type=int, default=None,
                        help="Number of instances to ensure")
    parser.add_argument("--serve",        action="store_true", help="Web server only")
    parser.add_argument("--status",       action="store_true", help="Print status and exit")
    parser.add_argument("--update-code",  action="store_true", help="Sync reply.js+package.json from backend folder, restart all instances")
    parser.add_argument("--no-server",    action="store_true", help="Scale but don't start server")
    args = parser.parse_args()

    # Root check
    if os.geteuid() != 0:
        print("ERROR: manager.py must run as root.  sudo python3 manager.py <N>")
        sys.exit(1)

    missing_env = [name for name, value in (
        ("DATABASE_URL", DATABASE_URL),
        ("BOT_API_KEY", API_KEY),
    ) if not value]
    if missing_env:
        print(f"ERROR: Missing required environment variable(s): {', '.join(missing_env)}")
        sys.exit(1)

    # DB check
    try:
        get_conn().close()
    except Exception as e:
        print(f"ERROR: Cannot connect to database: {e}")
        print("       Set DATABASE_URL in the environment before starting manager.py")
        sys.exit(1)

    # Status-only path
    if args.status:
        ensure_schema()
        print_status()
        sys.exit(0)

    # ── Full setup ──────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("  SalonFlow Bot Manager  —  starting up")
    log.info("=" * 60)

    ensure_schema()

    # --update-code: fast path — only download + restart, skip all system setup
    if args.update_code:
        log.info("[UPDATE] Downloading latest bot files and restarting instances…")
        update_bot_code()
        print_status()
        sys.exit(0)

    # Full setup path (first run / scaling)
    install_system_deps()
    install_nodejs()
    ensure_bot_user()
    ensure_directories()

    if not (SHARED_DIR / "reply.js").exists():
        log.info("[SETUP] First run — downloading bot files…")
        setup_shared_bot()
    else:
        log.info(f"[SETUP] Shared bot code already at {SHARED_DIR}")

    # Scale
    if args.instances:
        scale_instances(args.instances)
    elif not args.serve:
        ip  = get_public_ip()
        row = run_sql("SELECT total_capacity FROM vps_servers WHERE public_ip=%s", (ip,), fetch="one")
        register_vps(row["total_capacity"] if row else 0)

    install_cron()
    print_status()

    if args.no_server:
        # --no-server: scale/setup only, let ExecStartPre finish, then gunicorn takes over
        log.info("[DONE] Setup complete (--no-server). Gunicorn will serve the API.")
        sys.exit(0)

    start_heartbeat()

    # Install / restart the manager systemd service (gunicorn, 1 worker)
    install_manager_service()

    log.info("")
    log.info("=" * 60)
    log.info("  Setup complete!")
    log.info(f"  Manager API: http://{get_public_ip()}:{MANAGER_PORT}")
    log.info(f"  Service:     systemctl status {MANAGER_SERVICE}")
    log.info(f"  Logs:        {LOG_DIR}/manager.log")
    log.info("=" * 60)
    log.info("")
    log.info("The manager is now running as a background systemd service.")
    log.info("You can safely close this SSH session.")

if __name__ == "__main__":
    main()
