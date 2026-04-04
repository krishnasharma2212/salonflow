"""
SalonFlow – Main Flask Application
===================================
Handles: user auth, admin auth, dashboard, profile, billing, email verification,
         password reset, WhatsApp bot status, and admin panel.
"""

import os
import secrets
import threading
import urllib.request
import urllib.parse
import json as _json
import hmac
import hashlib
import razorpay
from datetime import datetime, timezone, timedelta
from functools import wraps

from flask import (
    Flask, render_template, redirect, url_for, request,
    flash, session, jsonify, abort, send_file, Response,
)
from flask_login import (
    LoginManager, login_user, logout_user,
    login_required, current_user,
)
from flask_mail import Mail, Message
from flask_wtf.csrf import CSRFProtect
from flask_migrate import Migrate
from flask_limiter import Limiter
from dotenv import load_dotenv


from config import config
from models import db, bcrypt, User, Admin, Subscription, Plan, BlockedIP, Service, Payment

load_dotenv()

# Module-level extensions (initialised inside create_app via init_app)
csrf = CSRFProtect()

def _get_client_ip():
    """Return real client IP, respecting trusted proxy headers."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.headers.get("X-Real-IP", "") or request.remote_addr or "unknown"

limiter = Limiter(
    key_func=_get_client_ip,
    default_limits=[],
    storage_uri="memory://",  # swap to "redis://..." in production
)

# ─────────────────────────────────────────────────────────
#  APP FACTORY
# ─────────────────────────────────────────────────────────

def create_app(config_name: str = None) -> Flask:
    env = config_name or os.environ.get("FLASK_ENV", "development")
    app = Flask(__name__)
    app.config.from_object(config.get(env, config["default"]))

    # Extensions
    db.init_app(app)
    bcrypt.init_app(app)
    csrf.init_app(app)
    limiter.init_app(app)
    mail = Mail(app)
    migrate = Migrate(app, db)

    # Login manager – user
    login_manager = LoginManager(app)
    login_manager.login_view = "auth_login"
    login_manager.login_message = "Please sign in to continue."
    login_manager.login_message_category = "info"

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # ── Register routes ───────────────────────────────
    register_auth_routes(app, mail)
    register_user_routes(app)
    register_admin_routes(app, mail)
    register_api_routes(app)
    register_webhook_routes(app)
    register_error_handlers(app)

    # CLI: create tables + seed admin
    @app.cli.command("init-db")
    def init_db():
        db.create_all()
        print("✅  Tables created.")

    @app.cli.command("create-admin")
    def create_admin_cli():
        """Seed a default superadmin. Run once after init-db."""
        username = input("Admin username: ").strip()
        email = input("Admin email: ").strip()
        pw = input("Admin password: ").strip()
        if Admin.query.filter_by(email=email).first():
            print("Admin already exists.")
            return
        admin = Admin(username=username, email=email, is_superadmin=True)
        admin.password = pw
        db.session.add(admin)
        db.session.commit()
        print(f"✅  Admin '{username}' created.")

    return app


# ─────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────

def _send_email(mail: Mail, subject: str, recipient: str, html_body: str, app=None):
    """
    Send an email in a background thread so it never blocks the HTTP response.
    Falls back to logging on any SMTP error — the user still gets their 201/redirect.
    """
    from flask import current_app
    _app = app or current_app._get_current_object()

    def _worker():
        with _app.app_context():
            try:
                msg = Message(subject, recipients=[recipient], html=html_body)
                mail.send(msg)
                print(f"[Email] Sent '{subject}' -> {recipient}")
            except Exception as exc:
                # Log clearly but never propagate — registration already succeeded.
                print(f"[Email error] {type(exc).__name__}: {exc}")

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def _gen_token(n: int = 48) -> str:
    return secrets.token_urlsafe(n)


# ── IP block helpers ──────────────────────────────────────────────────────────

def _is_ip_blocked(ip: str) -> bool:
    """Return True if this IP is permanently blocked."""
    try:
        return db.session.query(BlockedIP).filter_by(
            ip_address=ip, is_blocked=True
        ).first() is not None
    except Exception:
        return False


def _record_violation(ip: str, reason: str = "rate_limit") -> bool:
    """
    Increment violation count for this IP.
    Permanently blocks after IP_BLOCK_THRESHOLD violations.
    Returns True if IP is now permanently blocked.
    """
    from flask import current_app
    threshold = current_app.config.get("IP_BLOCK_THRESHOLD", 3)
    try:
        rec = db.session.query(BlockedIP).filter_by(ip_address=ip).first()
        if rec is None:
            rec = BlockedIP(ip_address=ip, reason=reason, violation_count=1)
            db.session.add(rec)
        else:
            rec.violation_count = (rec.violation_count or 0) + 1
            rec.reason = reason
        if (rec.violation_count >= threshold) and not rec.is_blocked:
            rec.is_blocked = True
            rec.blocked_at = datetime.now(timezone.utc)
        db.session.commit()
        return rec.is_blocked
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return False


# ── reCAPTCHA v3 verification ─────────────────────────────────────────────────

def _verify_recaptcha(token: str, action: str = "signup") -> tuple:
    """
    Verify reCAPTCHA v3 token with Google.
    Returns (passed: bool, score: float).
    Fails open (True) on network errors so users are never locked out.
    """
    from flask import current_app
    secret = current_app.config.get("RECAPTCHA_SECRET_KEY", "")
    enabled = current_app.config.get("RECAPTCHA_ENABLED", True)
    threshold = current_app.config.get("RECAPTCHA_SCORE_THRESHOLD", 0.5)

    if not enabled or not secret:
        return True, 1.0   # skip in dev if key not configured

    if not token:
        # reCAPTCHA script failed to load (domain not whitelisted, ad-blocker, slow network).
        # Fail open so legitimate users on unlisted domains are never permanently blocked.
        # Real bots will still be caught by rate limiting and IP blocking.
        print(f"[reCAPTCHA] empty token from {_get_client_ip()} — failing open")
        return True, 0.5

    try:
        params = urllib.parse.urlencode({
            "secret":   secret,
            "response": token,
            "remoteip": _get_client_ip(),
        }).encode()
        req = urllib.request.Request(
            "https://www.google.com/recaptcha/api/siteverify",
            data=params, method="POST"
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = _json.loads(resp.read().decode())

        if not data.get("success"):
            return False, 0.0

        score = float(data.get("score", 0.0))
        return score >= threshold, score

    except Exception as e:
        print(f"[reCAPTCHA] verification error: {e} — failing open")
        return True, 1.0   # fail open so genuine users aren't blocked


def admin_required(f):
    """Decorator: protect admin routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("admin_id"):
            flash("Please sign in as admin.", "warning")
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated


def get_current_admin():
    admin_id = session.get("admin_id")
    if admin_id:
        return Admin.query.get(admin_id)
    return None


# ─────────────────────────────────────────────────────────
#  AUTH ROUTES (user)
# ─────────────────────────────────────────────────────────

def register_auth_routes(app: Flask, mail: Mail):

    # ── Register (JSON API) ───────────────────────────
    @app.route("/api/register", methods=["POST"])
    @csrf.exempt
    @limiter.limit("5 per hour", error_message="Too many registrations from this IP. Try again later.")
    def api_register():
        ip = _get_client_ip()

        # Permanently blocked IPs are rejected immediately
        if _is_ip_blocked(ip):
            return jsonify({"error": "Your IP has been blocked due to repeated abuse. Contact support@salonflow.in."}), 403

        data = request.get_json(silent=True) or {}

        # ── reCAPTCHA v3 verification ──────────────────
        token = data.get("recaptcha_token", "")
        passed, score = _verify_recaptcha(token, action="signup")
        if not passed:
            _record_violation(ip, reason=f"recaptcha_fail score={score:.2f}")
            return jsonify({"error": "Bot activity detected. Please refresh the page and try again."}), 400

        salon_name = (data.get("salon_name") or "").strip()
        email = (data.get("email") or "").strip().lower()
        phone = (data.get("phone") or "").strip()
        password = data.get("password", "")

        # Validation
        if not salon_name:
            return jsonify({"error": "Salon name is required."}), 400
        if not email or "@" not in email:
            return jsonify({"error": "Valid email is required."}), 400
        if not phone:
            return jsonify({"error": "Phone number is required."}), 400
        if len(password) < 8:
            return jsonify({"error": "Password must be at least 8 characters."}), 400
        if User.query.filter_by(email=email).first():
            return jsonify({"error": "An account with this email already exists."}), 409

        # Create user
        token = _gen_token()
        expires = datetime.now(timezone.utc) + timedelta(
            hours=app.config["EMAIL_VERIFY_EXPIRE_HOURS"]
        )
        user = User(
            salon_name=salon_name,
            email=email,
            phone=phone,
            verify_token=token,
            verify_token_expires=expires,
            whatsapp_session_id=secrets.token_hex(24),   # unique bot session ID
        )
        user.password = password
        db.session.add(user)

        # Create default subscription
        sub = Subscription(plan=Plan.STARTER, status="active", amount_inr=0)
        sub.user = user
        db.session.add(sub)
        db.session.commit()

        # Send verification email
        verify_url = url_for("verify_email", token=token, _external=True)
        html = render_template("emails/verify.html", user=user, verify_url=verify_url)
        _send_email(mail, "Verify your SalonFlow account", email, html)

        return jsonify({"message": "Account created. Check your email to verify."}), 201

    # ── Login (JSON API) ──────────────────────────────
    @app.route("/api/login", methods=["POST"])
    @limiter.limit("10 per minute; 30 per hour", error_message="Too many login attempts. Please wait before trying again.")
    def api_login():
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip().lower()
        password = data.get("password", "")

        user = User.query.filter_by(email=email).first()
        if not user or not user.check_password(password):
            return jsonify({"error": "Invalid email or password."}), 401
        if not user.is_active:
            return jsonify({"error": "Your account has been suspended."}), 403

        if not user.is_verified:
            # Allow login so the session exists, but send to verify page
            login_user(user, remember=True)
            user.record_login()
            return jsonify({"message": "Please verify your email first.", "redirect": "/verify-required"}), 200
        login_user(user, remember=True)
        user.record_login()
        return jsonify({"message": "Signed in successfully.", "redirect": "/dashboard"}), 200

    # ── Logout ────────────────────────────────────────
    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        flash("You have been signed out.", "info")
        return redirect(url_for("index"))

    # ── Verify email ──────────────────────────────────
    @app.route("/verify/<token>")
    def verify_email(token):
        user = User.query.filter_by(verify_token=token).first()
        if not user:
            flash("Invalid or expired verification link.", "danger")
            return redirect(url_for("index"))
        if user.verify_token_expires and user.verify_token_expires < datetime.now(timezone.utc):
            flash("Verification link has expired. Request a new one.", "warning")
            return redirect(url_for("verify_required"))
        user.is_verified = True
        user.verify_token = None
        user.verify_token_expires = None
        db.session.commit()
        login_user(user)
        return render_template("verify_ok.html")

    # ── Resend verification ───────────────────────────
    @app.route("/api/resend-verification", methods=["POST"])
    @csrf.exempt
    @limiter.limit("3 per hour", error_message="Too many resend requests. Wait an hour before trying again.")
    def resend_verification():
        ip = _get_client_ip()
        if _is_ip_blocked(ip):
            return jsonify({"message": "If the email exists, a link was sent."}), 200
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip().lower()
        user = User.query.filter_by(email=email).first()
        if not user or user.is_verified:
            # Silently succeed to prevent email enumeration
            return jsonify({"message": "If the email exists, a link was sent."}), 200
        token = _gen_token()
        expires = datetime.now(timezone.utc) + timedelta(
            hours=app.config["EMAIL_VERIFY_EXPIRE_HOURS"]
        )
        user.verify_token = token
        user.verify_token_expires = expires
        db.session.commit()
        verify_url = url_for("verify_email", token=token, _external=True)
        html = render_template("emails/verify.html", user=user, verify_url=verify_url)
        _send_email(mail, "Verify your SalonFlow account", email, html)
        return jsonify({"message": "Verification email sent."}), 200

    # ── Google Sign-In (Firebase ID token) ───────────
    @app.route("/api/auth/google", methods=["POST"])
    @csrf.exempt
    def api_google_auth():
        """
        Frontend sends Firebase ID token after Google sign-in.
        We verify it with Google, create/find user, log them in.
        No email verification required — Google already verified the email.
        """
        data = request.get_json(silent=True) or {}
        id_token = (data.get("id_token") or "").strip()
        if not id_token:
            return jsonify({"error": "Missing ID token."}), 400

        ip = _get_client_ip()
        if _is_ip_blocked(ip):
            return jsonify({"error": "Your IP has been blocked."}), 403

        # Verify the Firebase ID token using google-auth library.
        # oauth2.googleapis.com/tokeninfo only works for Google OAuth2 tokens —
        # Firebase Auth tokens are signed by Firebase (different key), so we must
        # use google.oauth2.id_token.verify_firebase_token instead.
        try:
            from google.oauth2.id_token import verify_firebase_token
            from google.auth.transport.requests import Request as GoogleRequest
            import google.auth.exceptions

            FIREBASE_PROJECT_ID = "salonflow-6d47a"
            decoded = verify_firebase_token(
                id_token,
                GoogleRequest(),
                audience=FIREBASE_PROJECT_ID,
            )
        except google.auth.exceptions.TransportError as e:
            print(f"[Google auth] network error verifying token: {e}")
            return jsonify({"error": "Could not reach Google to verify. Check your connection."}), 503
        except Exception as e:
            print(f"[Google auth] token invalid: {e}")
            return jsonify({"error": "Invalid or expired Google token. Please sign in again."}), 401

        google_email     = (decoded.get("email") or "").strip().lower()
        google_name      = decoded.get("name") or decoded.get("given_name") or ""
        email_verified   = decoded.get("email_verified", False)

        if not google_email:
            return jsonify({"error": "Google did not provide an email address."}), 400
        if not email_verified:
            return jsonify({"error": "Google account email is not verified."}), 400

        print(f"[Google auth] ✅ verified: {google_email} (name={google_name})")

        # Find existing user or create one
        user = User.query.filter_by(email=google_email).first()
        if user is None:
            # New user — create without password, mark verified
            user = User(
                salon_name=google_name or google_email.split("@")[0].title() + " Salon",
                email=google_email,
                phone=None,
                is_verified=True,   # Google already verified the email
                whatsapp_session_id=secrets.token_hex(24),   # unique bot session ID
            )
            # Set a bcrypt-hashed random password (they log in via Google only)
            user.password = _gen_token(32)   # uses the @password.setter → bcrypt hash
            db.session.add(user)
            sub = Subscription(plan=Plan.STARTER, status="active", amount_inr=0)
            sub.user = user
            db.session.add(sub)
            try:
                db.session.commit()
            except Exception as db_err:
                db.session.rollback()
                print(f"[Google auth] DB error creating user: {db_err}")
                return jsonify({"error": "Account creation failed. Please try again."}), 500
            print(f"[Google auth] new user created: {google_email}")
            is_new = True
        else:
            # Existing user — ensure verified
            if not user.is_verified:
                user.is_verified = True
                db.session.commit()
            is_new = False

        if not user.is_active:
            return jsonify({"error": "Your account has been suspended."}), 403

        login_user(user, remember=True)
        user.record_login()

        # Safely check onboarding_complete (column may not exist in older DB schemas)
        try:
            onboarding_done = bool(getattr(user, "onboarding_complete", False))
        except Exception:
            onboarding_done = False

        redirect_to = url_for("onboarding") if (is_new or not onboarding_done) else url_for("dashboard")
        print(f"[Google auth] login ok — is_new={is_new} onboarding_done={onboarding_done} → {redirect_to}")
        return jsonify({
            "message": "Signed in with Google.",
            "redirect": redirect_to,
            "is_new": is_new,
            "salon_name": user.salon_name,
        }), 200

    # ── Forgot password page ──────────────────────────
    @app.route("/forgot", methods=["GET", "POST"])
    @limiter.limit("5 per hour", methods=["POST"], error_message="Too many password reset requests. Try again in an hour.")
    def forgot_password():
        if current_user.is_authenticated:
            return redirect(url_for("dashboard"))
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            user = User.query.filter_by(email=email).first()
            if user:
                token = _gen_token()
                expires = datetime.now(timezone.utc) + timedelta(
                    hours=app.config["PASSWORD_RESET_EXPIRE_HOURS"]
                )
                user.reset_token = token
                user.reset_token_expires = expires
                db.session.commit()
                reset_url = url_for("reset_password", token=token, _external=True)
                html = render_template("emails/reset.html", user=user, reset_url=reset_url)
                _send_email(mail, "Reset your SalonFlow password", email, html)
            flash("If that email is registered, a reset link has been sent.", "info")
            return redirect(url_for("forgot_password"))
        return render_template("forgot.html")

    # ── Reset password page ───────────────────────────
    @app.route("/reset-password/<token>", methods=["GET", "POST"])
    def reset_password(token):
        if current_user.is_authenticated:
            return redirect(url_for("dashboard"))
        user = User.query.filter_by(reset_token=token).first()
        if not user or (
            user.reset_token_expires and user.reset_token_expires < datetime.now(timezone.utc)
        ):
            flash("This reset link is invalid or has expired.", "danger")
            return redirect(url_for("forgot_password"))
        if request.method == "POST":
            pw = request.form.get("password", "")
            cpw = request.form.get("confirm_password", "")
            if len(pw) < 8:
                flash("Password must be at least 8 characters.", "danger")
                return render_template("resetpassword.html", token=token)
            if pw != cpw:
                flash("Passwords do not match.", "danger")
                return render_template("resetpassword.html", token=token)
            user.password = pw
            user.reset_token = None
            user.reset_token_expires = None
            db.session.commit()
            flash("Password reset successfully. Please sign in.", "success")
            return redirect(url_for("auth_login"))
        return render_template("resetpassword.html", token=token)

    # ── Auth page (login/signup) ───────────────────────
    @app.route("/auth")
    def auth_login():
        if current_user.is_authenticated:
            return redirect(url_for("dashboard"))
        return render_template("auth.html")

    # ── Verify required page ─────────────────────────
    @app.route("/verify-required")
    def verify_required():
        return render_template("verify_required.html")

    # ── Index ─────────────────────────────────────────
    @app.route("/")
    def index():
        return render_template("index.html")

    # ── Policy pages (public, no login required) ───────
    @app.route("/api/whatsapp/session-id")
    @login_required
    def wa_session_id():
        """Return (and lazily create) the current user's WhatsApp session ID."""
        from sqlalchemy import text as _t
        if not current_user.whatsapp_session_id:
            new_sid = secrets.token_hex(24)
            db.session.execute(
                _t("UPDATE users SET whatsapp_session_id=:s WHERE id=:id"),
                {"s": new_sid, "id": current_user.id}
            )
            db.session.commit()
            db.session.expire(current_user)
        return jsonify({"session_id": current_user.whatsapp_session_id}), 200

    # ════════════════════════════════════════════════════
    #  BOT MANAGEMENT — VPS & instance endpoints
    # ════════════════════════════════════════════════════

    # ── Bot file download (used by manager.py on VPS) ───────────────────────
    BOT_API_KEY = app.config.get("BOT_API_KEY", os.environ.get("BOT_API_KEY", "sf-bot-manager-secret-change-this"))

    def _check_bot_key():
        key = request.headers.get("X-API-Key", "").strip()
        return key == BOT_API_KEY

    @app.route("/bot/reply.js", methods=["GET"])
    @csrf.exempt
    def serve_reply_js():
        if not _check_bot_key():
            return jsonify({"error": "Unauthorized"}), 401
        path = os.path.join(os.path.dirname(__file__), "bot", "reply.js")
        if not os.path.exists(path):
            return jsonify({"error": "reply.js not found"}), 404
        return send_file(path, mimetype="application/javascript", download_name="reply.js")

    @app.route("/bot/package.json", methods=["GET"])
    @csrf.exempt
    def serve_package_json():
        if not _check_bot_key():
            return jsonify({"error": "Unauthorized"}), 401
        path = os.path.join(os.path.dirname(__file__), "bot", "package.json")
        if not os.path.exists(path):
            return jsonify({"error": "package.json not found"}), 404
        return send_file(path, mimetype="application/json", download_name="package.json")

    # ── VPS list ─────────────────────────────────────────────────────────────
    @app.route("/api/vps/list", methods=["GET"])
    @login_required
    def api_vps_list():
        from sqlalchemy import text as _t
        rows = db.session.execute(_t("""
            SELECT v.id, v.hostname, v.public_ip, v.port, v.total_capacity, v.is_active, v.last_seen,
                   COUNT(CASE WHEN b.status='free' THEN 1 END) AS free_count,
                   COUNT(CASE WHEN b.status='connected' THEN 1 END) AS connected_count,
                   COUNT(b.id) AS total_bots
            FROM vps_servers v
            LEFT JOIN bot_instances b ON b.vps_id = v.id
            WHERE v.is_active = TRUE
            GROUP BY v.id
            ORDER BY v.id
        """)).fetchall()
        result = []
        for r in rows:
            result.append({
                "id":             r.id,
                "hostname":       r.hostname,
                "public_ip":      r.public_ip,
                "port":           r.port,
                "total_capacity": r.total_capacity,
                "free_count":     r.free_count,
                "connected_count":r.connected_count,
                "total_bots":     r.total_bots,
                "last_seen":      r.last_seen.isoformat() if r.last_seen else None,
                "manager_url":    f"http://{r.public_ip}:{r.port}",
            })
        return jsonify({"ok": True, "vps_servers": result}), 200

    # ── Pick best VPS (most free slots) ──────────────────────────────────────
    @app.route("/api/vps/best", methods=["GET"])
    @login_required
    def api_vps_best():
        from sqlalchemy import text as _t
        row = db.session.execute(_t("""
            SELECT v.id, v.public_ip, v.port, v.api_key,
                   COUNT(CASE WHEN b.status='free' THEN 1 END) AS free_count
            FROM vps_servers v
            LEFT JOIN bot_instances b ON b.vps_id = v.id
            WHERE v.is_active = TRUE
            GROUP BY v.id
            HAVING COUNT(CASE WHEN b.status='free' THEN 1 END) > 0
            ORDER BY free_count DESC
            LIMIT 1
        """)).fetchone()
        if not row:
            return jsonify({"ok": False, "error": "No VPS with free capacity"}), 503
        return jsonify({"ok": True, "vps": {
            "id":          row.id,
            "public_ip":   row.public_ip,
            "port":        row.port,
            "free_count":  row.free_count,
            "manager_url": f"http://{row.public_ip}:{row.port}",
        }}), 200

    # ── Assign bot — Flask picks best VPS, calls manager ─────────────────────
    @app.route("/api/bot/assign", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_bot_assign():
        import urllib.request as _ur
        import urllib.error  as _ue
        from sqlalchemy import text as _t

        import secrets as _sec_assign
        from sqlalchemy import text as _t_sid
        user_id    = current_user.id
        session_id = current_user.whatsapp_session_id

        # No session ID yet (fresh user or after disconnect) — generate one now
        if not session_id:
            session_id = _sec_assign.token_hex(24)
            db.session.execute(_t_sid(
                "UPDATE users SET whatsapp_session_id=:sid WHERE id=:uid"
            ), {"sid": session_id, "uid": user_id})
            db.session.commit()
            # Refresh the user object so later reads see the new session_id
            db.session.refresh(current_user)

        # Find best VPS
        row = db.session.execute(_t("""
            SELECT v.public_ip, v.port, v.api_key,
                   COUNT(CASE WHEN b.status='free' THEN 1 END) AS free_count
            FROM vps_servers v
            LEFT JOIN bot_instances b ON b.vps_id = v.id
            WHERE v.is_active = TRUE
            GROUP BY v.id
            HAVING COUNT(CASE WHEN b.status='free' THEN 1 END) > 0
            ORDER BY free_count DESC
            LIMIT 1
        """)).fetchone()

        if not row:
            # Check if ANY VPS is configured at all
            any_vps = db.session.execute(_t("SELECT COUNT(*) FROM vps_servers WHERE is_active=TRUE")).scalar()
            if not any_vps:
                return jsonify({
                    "ok": False,
                    "error": "Bot service not configured. Please contact support at support@salonflow.in to activate your bot.",
                    "code": "no_vps"
                }), 200
            return jsonify({
                "ok": False,
                "error": "All bot slots are busy right now. Please try again in a few minutes.",
                "code": "no_free_slots"
            }), 200

        manager_url = f"http://{row.public_ip}:{row.port}/bot/assign"
        payload = _json.dumps({
            "session_id": session_id,
            "user_id":    user_id,
            "api_key":    row.api_key or BOT_API_KEY,
        }).encode()

        # If this session_id is already assigned (duplicate key guard): release first
        try:
            existing = db.session.execute(_t("""
                SELECT v.public_ip, v.port, v.api_key
                FROM bot_instances bi
                JOIN vps_servers v ON v.id = bi.vps_id
                WHERE bi.session_id = :sid LIMIT 1
            """), {"sid": session_id}).fetchone()
            if existing:
                _rel_url = f"http://{existing.public_ip}:{existing.port}/bot/release"
                _rel_pay = _json.dumps({"session_id": session_id}).encode()
                _rel_req = _ur.Request(
                    _rel_url, data=_rel_pay, method="POST",
                    headers={"Content-Type": "application/json",
                             "X-API-Key": existing.api_key or BOT_API_KEY}
                )
                try:
                    with _ur.urlopen(_rel_req, timeout=8): pass
                    print(f"[BotAssign] Released existing instance for {session_id}")
                except Exception as _re:
                    print(f"[BotAssign] Release failed (continuing): {_re}")
        except Exception as _ce:
            print(f"[BotAssign] Pre-release check error (continuing): {_ce}")

        try:
            req = _ur.Request(
                manager_url, data=payload, method="POST",
                headers={"Content-Type": "application/json",
                         "X-API-Key": row.api_key or BOT_API_KEY}
            )
            with _ur.urlopen(req, timeout=15) as resp:
                result = _json.loads(resp.read().decode())
            return jsonify({"ok": True, "result": result, "vps": row.public_ip}), 200
        except _ue.HTTPError as e:
            body = e.read().decode()[:200]
            print(f"[BotAssign] Manager HTTP error {e.code}: {body}")
            return jsonify({"ok": False, "error": "Bot service temporarily unavailable. Please try again."}), 200
        except Exception as e:
            print(f"[BotAssign] Exception: {e}")
            return jsonify({"ok": False, "error": "Could not reach bot service. Please try again."}), 200

    # ── Release bot ───────────────────────────────────────────────────────────
    @app.route("/api/bot/release", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_bot_release():
        import urllib.request as _ur
        import urllib.error  as _ue
        from sqlalchemy import text as _t

        session_id = current_user.whatsapp_session_id
        if not session_id:
            return jsonify({"ok": False, "error": "No active bot session"}), 400

        # Find which VPS is running this session
        row = db.session.execute(_t("""
            SELECT v.public_ip, v.port, v.api_key
            FROM bot_instances b
            JOIN vps_servers v ON v.id = b.vps_id
            WHERE b.session_id = :sid
            LIMIT 1
        """), {"sid": session_id}).fetchone()

        if not row:
            return jsonify({"ok": False, "error": "Bot instance not found"}), 404

        manager_url = f"http://{row.public_ip}:{row.port}/bot/release"
        payload     = _json.dumps({
            "session_id": session_id,
            "api_key":    row.api_key or BOT_API_KEY,
        }).encode()

        try:
            req = _ur.Request(
                manager_url, data=payload, method="POST",
                headers={"Content-Type": "application/json",
                         "X-API-Key": row.api_key or BOT_API_KEY}
            )
            with _ur.urlopen(req, timeout=15) as resp:
                result = _json.loads(resp.read().decode())
            return jsonify({"ok": True, "result": result}), 200
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 502

    # ── Overall bot stats ─────────────────────────────────────────────────────
    @app.route("/api/bot/stats", methods=["GET"])
    @login_required
    def api_bot_stats():
        from sqlalchemy import text as _t
        row = db.session.execute(_t("""
            SELECT
                COUNT(*) AS total,
                COUNT(CASE WHEN status='free'      THEN 1 END) AS free,
                COUNT(CASE WHEN status='connected'  THEN 1 END) AS connected,
                COUNT(CASE WHEN status='starting'   THEN 1 END) AS starting,
                COUNT(CASE WHEN status='error'      THEN 1 END) AS error,
                COUNT(CASE WHEN status='stopped'    THEN 1 END) AS stopped
            FROM bot_instances
        """)).fetchone()
        vps_count = db.session.execute(_t(
            "SELECT COUNT(*) AS n FROM vps_servers WHERE is_active=TRUE"
        )).fetchone()
        return jsonify({"ok": True, "stats": {
            "total":      row.total,
            "free":       row.free,
            "connected":  row.connected,
            "starting":   row.starting,
            "error":      row.error,
            "stopped":    row.stopped,
            "vps_count":  vps_count.n,
        }}), 200

    # ── Manual appointment creation ───────────────────────────────────────────
    @app.route("/api/appointments/manual", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_appointments_manual():
        import json as _j
        from sqlalchemy import text as _t
        data = request.get_json(silent=True) or {}

        name     = data.get("customer_name", "").strip()
        service  = data.get("service", "").strip()
        date     = data.get("date", "").strip()
        time     = data.get("time", "").strip()
        dur      = int(data.get("duration_min", 30))
        notes    = data.get("notes", "").strip()

        if not all([name, service, date, time]):
            return jsonify({"ok": False, "error": "name, service, date, time are required"}), 400

        # Build appointment object (same shape as bot bookings)
        import secrets as _s
        appt_id = _s.token_hex(8)
        appt = {
            "id":            appt_id,
            "customer_name": name,
            "service":       service,
            "date":          date,
            "time":          time,
            "duration_min":  dur,
            "notes":         notes,
            "phone":         "",
            "remoteJid":     "",
            "timestamp":     datetime.now(timezone.utc).isoformat(),
            "remindersSent": {},
            "source":        "manual",
        }

        # Load existing wa_appointments, append, save
        row = db.session.execute(
            _t("SELECT wa_appointments FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        existing = []
        try:
            raw = row.wa_appointments if row else None
            if raw:
                data2 = _j.loads(raw) if isinstance(raw, str) else raw
                existing = data2 if isinstance(data2, list) else list(data2.values())
        except Exception:
            existing = []

        existing.append(appt)
        db.session.execute(
            _t("UPDATE users SET wa_appointments=:d WHERE id=:uid"),
            {"d": _j.dumps(existing), "uid": current_user.id}
        )
        db.session.commit()
        return jsonify({"ok": True, "appointment": appt}), 200

    # ── Appointments API (real data from wa_appointments) ────────────────────
    @app.route("/api/appointments", methods=["GET"])
    @login_required
    def api_appointments():
        from sqlalchemy import text as _t
        row = db.session.execute(
            _t("SELECT wa_appointments FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        raw = row.wa_appointments if row else None
        appts = []
        if raw:
            import json as _j
            data = _j.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, list):
                appts = data
            elif isinstance(data, dict):
                appts = list(data.values())
        # Sort by date+time descending
        appts.sort(key=lambda a: (a.get("date",""), a.get("time","")), reverse=True)
        return jsonify({"ok": True, "appointments": appts}), 200

    # ── Payment settings (UPI ID + QR upload) ────────────────────────────────
    @app.route("/api/payment-settings", methods=["GET"])
    @login_required
    def api_payment_settings_get():
        from sqlalchemy import text as _t
        import json as _j

        # Auto-migrate all required columns
        for col in [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS upi_id VARCHAR(100)",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS upi_qr_code TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS bot_settings TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS advance_amount NUMERIC(10,2) DEFAULT 0",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS payment_enabled BOOLEAN DEFAULT FALSE",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS schedule_mode VARCHAR(20) DEFAULT 'hourly'",
        ]:
            try:
                db.session.execute(_t(col))
                db.session.commit()
            except Exception:
                db.session.rollback()

        row = db.session.execute(
            _t("""SELECT upi_id, upi_qr_code, advance_amount, payment_enabled
                  FROM users WHERE id=:uid"""),
            {"uid": current_user.id}
        ).fetchone()

        return jsonify({
            "ok":              True,
            "upi_id":          (row.upi_id or "") if row else "",
            "upi_qr_code":     (row.upi_qr_code or "") if row else "",
            "advance_amount":  str((row.advance_amount or 0) if row else 0),
            "payment_enabled": bool((row.payment_enabled) if row else False),
        }), 200

    @app.route("/api/payment-settings", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_payment_settings_post():
        from sqlalchemy import text as _t
        import json as _j
        data        = request.get_json(silent=True) or {}
        upi_id      = data.get("upi_id", "").strip()
        upi_qr      = data.get("upi_qr_code", "").strip()
        adv_amount  = max(0, int(float(data.get("advance_amount", 0) or 0)))
        pay_enabled = bool(data.get("payment_enabled", False))

        # Auto-migrate
        for col in [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS upi_id VARCHAR(100)",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS upi_qr_code TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS advance_amount NUMERIC(10,2) DEFAULT 0",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS payment_enabled BOOLEAN DEFAULT FALSE",
        ]:
            try:
                db.session.execute(_t(col)); db.session.commit()
            except Exception:
                db.session.rollback()

        if upi_qr:
            db.session.execute(_t("""
                UPDATE users SET upi_id=:uid, upi_qr_code=:qr,
                                 advance_amount=:amt, payment_enabled=:pe
                WHERE id=:id
            """), {"uid": upi_id, "qr": upi_qr, "amt": adv_amount, "pe": pay_enabled, "id": current_user.id})
        else:
            db.session.execute(_t("""
                UPDATE users SET upi_id=:uid, advance_amount=:amt, payment_enabled=:pe
                WHERE id=:id
            """), {"uid": upi_id, "amt": adv_amount, "pe": pay_enabled, "id": current_user.id})
        db.session.commit()
        return jsonify({"ok": True}), 200

    # ── DB schema migration (called on first boot) ───────────────────────────
    @app.route("/api/admin/migrate-upi", methods=["POST"])
    @login_required
    def api_migrate_upi():
        if not current_user.is_active:
            return jsonify({"error": "Forbidden"}), 403
        from sqlalchemy import text as _t
        for col_sql in [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS upi_id VARCHAR(100)",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS upi_qr_code TEXT",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS bot_settings TEXT",
        ]:
            db.session.execute(_t(col_sql))
        db.session.commit()
        return jsonify({"ok": True, "message": "Columns added"}), 200

    # ── Extract UPI ID from QR image ─────────────────────────────────────────
    @app.route("/api/payment-settings/extract-upi", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_extract_upi():
        import re as _re, base64 as _b64
        data      = request.get_json(silent=True) or {}
        qr_image  = data.get("qr_image", "")
        if not qr_image:
            return jsonify({"ok": False}), 400
        try:
            if "," in qr_image:
                qr_image = qr_image.split(",", 1)[1]
            raw  = _b64.b64decode(qr_image)
            text = raw.decode("latin-1", errors="replace")
            # UPI QR standard: upi://pay?pa=VPA&...
            m = _re.search(r"pa=([A-Za-z0-9._-]+@[A-Za-z0-9]+)", text, _re.IGNORECASE)
            if m:
                return jsonify({"ok": True, "upi_id": m.group(1)}), 200
            # Fallback: any VPA-like string
            m2 = _re.search(r"([A-Za-z0-9._-]{3,}@[A-Za-z0-9]{3,})", text)
            if m2:
                return jsonify({"ok": True, "upi_id": m2.group(1)}), 200
        except Exception as e:
            print(f"[UPI extract] {e}")
        return jsonify({"ok": False, "upi_id": ""}), 200

    # ── Account deletion ──────────────────────────────────────────────────────
    @app.route("/api/account/delete", methods=["POST"])
    @login_required
    def api_account_delete():
        import json as _j, urllib.request as _ur, secrets as _sec
        from sqlalchemy import text as _t

        data = request.get_json(silent=True) or {}
        confirm_email = (data.get("confirm_email") or "").strip().lower()
        if confirm_email != current_user.email.lower():
            return jsonify({"ok": False, "error": "Email does not match"}), 400

        uid  = current_user.id
        sid  = current_user.whatsapp_session_id

        # ── 1. Disconnect WhatsApp bot if assigned ──────────────────────────
        if sid:
            try:
                row = db.session.execute(_t("""
                    SELECT v.public_ip, v.port, v.api_key
                    FROM bot_instances bi
                    JOIN vps_servers v ON v.id = bi.vps_id
                    WHERE bi.session_id = :sid LIMIT 1
                """), {"sid": sid}).fetchone()
                if row:
                    bot_key  = row.api_key or os.environ.get("BOT_API_KEY","")
                    base_url = f"http://{row.public_ip}:{row.port}"
                    payload  = _j.dumps({"session_id": sid}).encode()
                    req = _ur.Request(
                        f"{base_url}/bot/release", data=payload, method="POST",
                        headers={"Content-Type":"application/json","X-API-Key":bot_key}
                    )
                    with _ur.urlopen(req, timeout=8): pass
            except Exception as e:
                print(f"[AccountDelete] bot release failed: {e}")

        # ── 2. Revoke Google/Firebase token if stored ─────────────────────
        try:
            row = db.session.execute(
                _t("SELECT wa_gcal_creds FROM users WHERE id=:uid"), {"uid": uid}
            ).fetchone()
            if row and row.wa_gcal_creds:
                creds = _j.loads(row.wa_gcal_creds) if isinstance(row.wa_gcal_creds, str) else row.wa_gcal_creds
                token = creds.get("token") or creds.get("access_token")
                if token:
                    try:
                        revoke_req = _ur.Request(
                            f"https://oauth2.googleapis.com/revoke?token={token}",
                            method="POST"
                        )
                        with _ur.urlopen(revoke_req, timeout=5): pass
                    except Exception: pass
        except Exception: pass

        # ── 3. Cancel Razorpay subscription if active ─────────────────────
        try:
            sub = Subscription.query.filter_by(user_id=uid, status="active").first()
            if sub and sub.gateway_subscription_id:
                rz_key = os.environ.get("RAZORPAY_KEY_ID","")
                rz_sec = os.environ.get("RAZORPAY_KEY_SECRET","")
                if rz_key and rz_sec:
                    import base64 as _b64
                    auth = _b64.b64encode(f"{rz_key}:{rz_sec}".encode()).decode()
                    cancel_req = _ur.Request(
                        f"https://api.razorpay.com/v1/subscriptions/{sub.gateway_subscription_id}/cancel",
                        data=b"{}",method="POST",
                        headers={"Content-Type":"application/json","Authorization":f"Basic {auth}"}
                    )
                    try:
                        with _ur.urlopen(cancel_req, timeout=8): pass
                    except Exception: pass
        except Exception: pass

        # ── 4. Delete user (cascades via FK) ──────────────────────────────
        logout_user()
        from sqlalchemy import text as _t2
        # Hard-delete with raw SQL to ensure cascade triggers
        db.session.execute(_t2("DELETE FROM users WHERE id = :uid"), {"uid": uid})
        db.session.commit()

        return jsonify({"ok": True}), 200

    # ── Payment screenshot upload (called by reply.js bot) ──────────────────
    @app.route("/api/payment/screenshot", methods=["POST"])
    @csrf.exempt
    def api_payment_screenshot():
        import base64 as _b64, uuid as _uuid, json as _j
        from sqlalchemy import text as _t

        # Verify API key
        api_key = request.headers.get("X-API-Key", "").strip()
        if api_key != os.environ.get("BOT_API_KEY", ""):
            return jsonify({"ok": False, "error": "Unauthorized"}), 401

        data = request.get_json(silent=True) or {}
        user_id        = data.get("user_id")
        appointment_id = data.get("appointment_id", "")
        phone          = data.get("phone", "")
        screenshot_b64 = data.get("screenshot_b64", "")
        transaction_id = data.get("transaction_id")
        amount_verified= data.get("amount_verified")
        verified       = bool(data.get("verified", False))
        fake_score     = float(data.get("fake_score", 0))
        ai_notes       = data.get("ai_notes", "")

        if not user_id or not screenshot_b64:
            return jsonify({"ok": False, "error": "Missing user_id or screenshot_b64"}), 400

        # Save screenshot to static/screenshots/
        screenshot_path = None
        try:
            static_dir = os.path.join(app.static_folder, "screenshots")
            os.makedirs(static_dir, exist_ok=True)
            filename = f"pay_{user_id}_{appointment_id[:8]}_{_uuid.uuid4().hex[:8]}.jpg"
            filepath = os.path.join(static_dir, filename)
            img_bytes = _b64.b64decode(screenshot_b64)
            with open(filepath, "wb") as fh:
                fh.write(img_bytes)
            screenshot_path = f"/static/screenshots/{filename}"
        except Exception as e:
            print(f"[SCREENSHOT] save error: {e}")

        # Insert into payment_screenshots table
        screenshot_id = None
        try:
            row = db.session.execute(_t("""
                INSERT INTO payment_screenshots
                  (user_id, appointment_id, phone, screenshot_path, screenshot_b64,
                   transaction_id, amount_verified, verified, fake_score, ai_notes)
                VALUES (:uid, :appt, :phone, :path, :b64, :txn, :amt, :ver, :fs, :notes)
                RETURNING id
            """), {
                "uid":   user_id,   "appt":  appointment_id,
                "phone": phone,     "path":  screenshot_path,
                "b64":   screenshot_b64 if not screenshot_path else None,
                "txn":   transaction_id, "amt":   amount_verified,
                "ver":   verified,  "fs":    fake_score,
                "notes": ai_notes,
            }).fetchone()
            screenshot_id = row.id if row else None
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"[SCREENSHOT] DB error: {e}")

        # Update wa_appointments JSONB to mark payment
        try:
            import json as _jj
            row = db.session.execute(
                _t("SELECT wa_appointments FROM users WHERE id=:uid"), {"uid": user_id}
            ).fetchone()
            if row and row.wa_appointments:
                appts = row.wa_appointments if isinstance(row.wa_appointments, list) else                         list(row.wa_appointments.values()) if isinstance(row.wa_appointments, dict) else []
                for a in appts:
                    if a.get("id") == appointment_id:
                        a["payment_status"]  = "paid" if verified else "manual_review"
                        a["transaction_id"]  = transaction_id
                        a["screenshot_id"]   = screenshot_id
                        a["screenshot_path"] = screenshot_path
                        if amount_verified:
                            a["amount_paid"] = amount_verified
                db.session.execute(
                    _t("UPDATE users SET wa_appointments=:d WHERE id=:uid"),
                    {"d": _jj.dumps(appts), "uid": user_id}
                )
                db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"[SCREENSHOT] appt update error: {e}")

        return jsonify({"ok": True, "screenshot_id": screenshot_id, "path": screenshot_path}), 200

    # ── AI usage stats endpoint ────────────────────────────────────────────────
    @app.route("/api/ai-usage", methods=["GET"])
    @login_required
    def api_ai_usage():
        from sqlalchemy import text as _t
        try:
            rows = db.session.execute(_t("""
                SELECT session_date, model, prompt_tokens, completion_tokens,
                       total_tokens, cost_usd, calls
                FROM ai_usage
                WHERE user_id = :uid
                ORDER BY session_date DESC
                LIMIT 30
            """), {"uid": current_user.id}).fetchall()
            total = db.session.execute(_t("""
                SELECT SUM(cost_usd) AS total_cost, SUM(calls) AS total_calls,
                       SUM(total_tokens) AS total_tokens
                FROM ai_usage WHERE user_id = :uid
            """), {"uid": current_user.id}).fetchone()
            return jsonify({
                "ok": True,
                "daily": [
                    {"date": str(r.session_date), "model": r.model,
                     "prompt_tokens": r.prompt_tokens, "completion_tokens": r.completion_tokens,
                     "total_tokens": r.total_tokens,
                     "cost_usd": float(r.cost_usd or 0),
                     "cost_inr": round(float(r.cost_usd or 0) * 84, 4),
                     "calls": r.calls}
                    for r in rows
                ],
                "totals": {
                    "cost_usd": float(total.total_cost or 0) if total else 0,
                    "cost_inr": round(float(total.total_cost or 0) * 84, 2) if total else 0,
                    "calls":    int(total.total_calls or 0)  if total else 0,
                    "tokens":   int(total.total_tokens or 0) if total else 0,
                }
            }), 200
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    # ── Bot settings (language + schedule override) ───────────────────────────
    @app.route("/api/bot-settings", methods=["GET"])
    @login_required
    def api_bot_settings_get():
        import json as _j
        from sqlalchemy import text as _t
        row = db.session.execute(
            _t("SELECT bot_settings FROM users WHERE id=:uid"), {"uid": current_user.id}
        ).fetchone()
        raw = row.bot_settings if row else None
        settings = {}
        try:
            if raw: settings = _j.loads(raw) if isinstance(raw, str) else raw
        except Exception: pass
        return jsonify({"ok": True, **settings}), 200

    @app.route("/api/bot-settings", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_bot_settings_post():
        import json as _j
        from sqlalchemy import text as _t
        data = request.get_json(silent=True) or {}
        # Read existing
        row = db.session.execute(
            _t("SELECT bot_settings FROM users WHERE id=:uid"), {"uid": current_user.id}
        ).fetchone()
        try:
            existing = _j.loads(row.bot_settings) if row and row.bot_settings else {}
        except Exception:
            existing = {}
        existing.update({k: v for k, v in data.items()})
        db.session.execute(
            _t("UPDATE users SET bot_settings=:s WHERE id=:uid"),
            {"s": _j.dumps(existing), "uid": current_user.id}
        )
        db.session.commit()
        return jsonify({"ok": True}), 200

    @app.route("/privacy")
    def privacy():
        return render_template("privacy.html")

    @app.route("/terms")
    def terms():
        return render_template("terms.html")

    @app.route("/refund")
    def refund():
        return render_template("refund.html")




# ─────────────────────────────────────────────────────────
#  USER ROUTES
# ─────────────────────────────────────────────────────────

def register_user_routes(app: Flask):

    @app.route("/dashboard")
    @login_required
    def dashboard():
        import json as _j
        if not current_user.is_verified:
            return redirect(url_for("verify_required"))
        if not getattr(current_user, "onboarding_complete", False):
            return redirect(url_for("onboarding"))

        # Dashboard is always accessible after onboarding is complete.
        # WhatsApp connection state is managed via the WhatsApp tab UI.
        from flask import make_response as _mkr
        _resp = _mkr(render_template(
            "dashboard.html",
            user=current_user,
            google_places_key=app.config.get("GOOGLE_PLACES_API_KEY", ""),
        ))
        _resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        _resp.headers["Pragma"] = "no-cache"
        _resp.headers["Expires"] = "0"
        return _resp

    @app.route("/onboarding")
    @login_required
    def onboarding():
        import json
        from sqlalchemy import text as sa_text
        if not current_user.is_verified:
            return redirect(url_for("verify_required"))
        # Read fresh from DB — never trust ORM cache for onboarding state
        row = db.session.execute(
            sa_text("SELECT onboarding_complete, onboarding_step, onboarding_data FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        if row and row[0]:  # onboarding_complete
            return redirect(url_for("dashboard"))
        onboarding_data = {}
        try:
            raw = row[2] if row else None
            if raw:
                onboarding_data = json.loads(raw)
        except (ValueError, TypeError):
            pass
        current_step = int(row[1] or 1) if row else 1
        current_step = max(1, min(current_step, 6))
        return render_template(
            "onboarding.html",
            user=current_user,
            current_step=current_step,
            onboarding_data=onboarding_data,
            google_places_key=app.config.get("GOOGLE_PLACES_API_KEY", ""),
        )

    # ── Onboarding: save step ─────────────────────────
    @app.route("/api/onboarding/save", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_onboarding_save():
        import json
        from sqlalchemy import text as sa_text
        data = request.get_json(silent=True) or {}
        step = int(data.get("step", 1))
        step_data = data.get("data", {})

        # Read current state DIRECTLY from DB — never rely on ORM cached value
        row = db.session.execute(
            sa_text("SELECT onboarding_data, onboarding_step FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        existing = {}
        try:
            if row and row[0]:
                existing = json.loads(row[0])
        except (ValueError, TypeError):
            pass
        existing[f"step_{step}"] = step_data
        current_db_step = int(row[1] or 1) if row else 1
        new_step = max(current_db_step, step + 1)

        # Write back with a single atomic UPDATE
        extra_fields = ""
        extra_params = {}
        if step == 1:
            sn = (step_data.get("salon_name") or "").strip()
            extra_fields = ", address=:addr, city=:city" + (", salon_name=:sn" if sn else "")
            extra_params = {"addr": step_data.get("address",""), "city": step_data.get("city","")}
            if sn:
                extra_params["sn"] = sn
        elif step == 6:
            extra_fields = ", whatsapp_phone=:wp, whatsapp_instructions=:wi"
            extra_params = {"wp": step_data.get("phone",""), "wi": step_data.get("instructions","")}

        params = {"data": json.dumps(existing), "new_step": new_step, "uid": current_user.id}
        params.update(extra_params)
        db.session.execute(
            sa_text(f"UPDATE users SET onboarding_data=:data, onboarding_step=:new_step{extra_fields} WHERE id=:uid"),
            params
        )
        db.session.commit()
        db.session.expire(current_user)
        return jsonify({"ok": True, "saved_step": step, "next_step": new_step})

    # ── Onboarding: mark complete ─────────────────────
    @app.route("/api/onboarding/complete", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_onboarding_complete():
        import json
        from sqlalchemy import text as sa_text
        data = request.get_json(silent=True) or {}
        step_data = data.get("data", {})

        # Read fresh from DB before merging
        row = db.session.execute(
            sa_text("SELECT onboarding_data FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        existing = {}
        try:
            if row and row[0]:
                existing = json.loads(row[0])
        except (ValueError, TypeError):
            pass
        existing["step_6"] = step_data
        plan_choice = existing.get("step_5", {}).get("plan", "starter")
        if plan_choice not in ("starter", "pro", "business"):
            plan_choice = "starter"

        db.session.execute(sa_text("""
            UPDATE users SET
              onboarding_data      = :data,
              onboarding_step      = 7,
              onboarding_complete  = TRUE,
              plan                 = :plan,
              whatsapp_phone       = COALESCE(NULLIF(:wp, ''), whatsapp_phone),
              whatsapp_instructions= COALESCE(NULLIF(:wi, ''), whatsapp_instructions)
            WHERE id = :uid
        """), {
            "data": json.dumps(existing),
            "plan": plan_choice,
            "wp":   step_data.get("phone", ""),
            "wi":   step_data.get("instructions", ""),
            "uid":  current_user.id,
        })
        db.session.commit()
        db.session.expire(current_user)

        # Write services from step_2 into the services table so the
        # salon settings tab shows them immediately after onboarding
        try:
            svc_list = existing.get("step_2", {}).get("services", [])
            if svc_list:
                Service.query.filter_by(user_id=current_user.id).delete()
                for i, s in enumerate(svc_list):
                    name = (s.get("name") or "").strip()
                    if not name:
                        continue
                    try:
                        price = max(0, int(float(s.get("price") or 0)))
                    except (ValueError, TypeError):
                        price = 0
                    try:
                        dur = max(5, int(s.get("duration_min") or 30))
                    except (ValueError, TypeError):
                        dur = 30
                    db.session.add(Service(
                        user_id      = current_user.id,
                        name         = name[:150],
                        price        = price,
                        duration_min = dur,
                        is_active    = True,
                        sort_order   = i,
                    ))
                db.session.commit()
        except Exception as e:
            print(f"[Onboarding] service sync error: {e}")

        return jsonify({"ok": True, "redirect": "/dashboard"})

    # ── Coupon validation ─────────────────────────────
    @app.route("/api/coupon/validate", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_coupon_validate():
        data = request.get_json(silent=True) or {}
        code = (data.get("code") or "").strip().upper()
        # Static demo coupons — replace with DB lookup when ready
        DEMO_COUPONS = {
            "LAUNCH50": 50,
            "SALON20":  20,
            "WELCOME10": 10,
        }
        if code in DEMO_COUPONS:
            return jsonify({"valid": True, "discount": DEMO_COUPONS[code]}), 200
        return jsonify({"valid": False, "error": "Invalid or expired coupon code."}), 400

    @app.route("/profile", methods=["GET", "POST"])
    @login_required
    def profile():
        if request.method == "POST":
            action = request.form.get("action")

            if action == "update_profile":
                current_user.salon_name = request.form.get("salon_name", "").strip() or current_user.salon_name
                current_user.owner_name = request.form.get("owner_name", "").strip()
                current_user.phone = request.form.get("phone", "").strip()
                current_user.city = request.form.get("city", "").strip()
                current_user.address = request.form.get("address", "").strip()
                db.session.commit()
                flash("Profile updated successfully.", "success")

            elif action == "change_password":
                old_pw = request.form.get("old_password", "")
                new_pw = request.form.get("new_password", "")
                conf_pw = request.form.get("confirm_new_password", "")
                if not current_user.check_password(old_pw):
                    flash("Current password is incorrect.", "danger")
                elif len(new_pw) < 8:
                    flash("New password must be at least 8 characters.", "danger")
                elif new_pw != conf_pw:
                    flash("Passwords do not match.", "danger")
                else:
                    current_user.password = new_pw
                    db.session.commit()
                    flash("Password changed successfully.", "success")

            return redirect(url_for("profile"))
        return render_template("profile.html", user=current_user,
                               google_places_key=app.config.get("GOOGLE_PLACES_API_KEY", ""))

    @app.route("/billing")
    @login_required
    def billing():
        sub = current_user.subscription
        if not sub:
            sub = Subscription(plan=Plan.STARTER, status="active", amount_inr=0)
            sub.user = current_user
            db.session.add(sub)
            db.session.commit()
        return render_template("billing.html", user=current_user, sub=sub, Plan=Plan)

    # ── Create Razorpay Subscription (recurring) ───────
    @app.route("/billing/create-order", methods=["POST"])
    @login_required
    @csrf.exempt
    def billing_create_order():
        """
        Creates a Razorpay Subscription for recurring monthly/annual billing.
        Supports UPI AutoPay mandates, saved cards, netbanking standing instructions.
        Falls back to one-time order if plan IDs are not configured yet.
        """
        data  = request.get_json(silent=True) or {}
        plan  = data.get("plan", "").strip().lower()
        cycle = data.get("billing_cycle", "monthly")

        if plan not in (Plan.PRO, Plan.BUSINESS):
            return jsonify({"error": "Invalid plan selected."}), 400

        key_id  = app.config.get("RAZORPAY_KEY_ID", "")
        key_sec = app.config.get("RAZORPAY_KEY_SECRET", "")
        if not key_id or not key_sec:
            return jsonify({"error": "Payment gateway not configured. Contact support."}), 503

        base_price   = Plan.PRICES.get(plan, 0)
        amount_inr   = int(base_price * 12 * 0.8) if cycle == "annual" else base_price
        amount_paise = amount_inr * 100

        # Determine Razorpay Plan ID for this plan+cycle combination
        plan_id_key = f"RAZORPAY_PLAN_{plan.upper()}_{cycle.upper()}"
        rzp_plan_id = app.config.get(plan_id_key, "")

        client = razorpay.Client(auth=(key_id, key_sec))

        # ── PATH A: Razorpay Subscription (recurring) ──────────────────────────
        # Used when Razorpay Plan IDs are configured in .env
        if rzp_plan_id:
            try:
                # Create or fetch Razorpay customer
                # Razorpay raises an error if customer already exists for the email.
                # We catch that and search for the existing customer instead.
                customer_id = None
                sub = current_user.subscription
                if sub and sub.gateway_customer_id:
                    customer_id = sub.gateway_customer_id
                else:
                    try:
                        cust = client.customer.create(data={
                            "name":    current_user.salon_name,
                            "email":   current_user.email,
                            "contact": current_user.phone or "",
                            "fail_existing": "0",  # return existing customer instead of error
                        })
                        customer_id = cust["id"]
                    except Exception as cust_err:
                        err_str = str(cust_err).lower()
                        if "already exists" in err_str or "duplicate" in err_str:
                            # Fetch existing customer by email
                            try:
                                existing = client.customer.all({"email": current_user.email})
                                items = existing.get("items", [])
                                if items:
                                    customer_id = items[0]["id"]
                                    print(f"[Razorpay] reusing existing customer {customer_id}")
                            except Exception as fetch_err:
                                print(f"[Razorpay] could not fetch existing customer: {fetch_err}")
                        if not customer_id:
                            raise  # re-raise original error

                # Create subscription
                total_count = 12 if cycle == "annual" else 120  # max billing cycles
                rzp_sub = client.subscription.create(data={
                    "plan_id":     rzp_plan_id,
                    "customer_id": customer_id,
                    "total_count": total_count,
                    "quantity":    1,
                    "notes": {
                        "user_id":    str(current_user.id),
                        "user_email": current_user.email,
                        "salon_name": current_user.salon_name,
                        "plan":       plan,
                        "cycle":      cycle,
                    },
                })

                # Store gateway IDs
                if sub:
                    sub.gateway_customer_id      = customer_id
                    sub.gateway_subscription_id  = rzp_sub["id"]
                    db.session.commit()

                # Persist payment record (no order_id yet for subscriptions)
                payment = Payment(
                    user_id           = current_user.id,
                    razorpay_order_id = rzp_sub["id"],   # use sub ID as reference
                    plan              = plan,
                    amount_paise      = amount_paise,
                    billing_cycle     = cycle,
                    status            = "created",
                    ip_address        = _get_client_ip(),
                )
                db.session.add(payment)
                db.session.commit()

                return jsonify({
                    "mode":            "subscription",
                    "subscription_id": rzp_sub["id"],
                    "amount":          amount_paise,
                    "currency":        "INR",
                    "plan":            plan,
                    "plan_label":      Plan.LABELS[plan],
                    "key_id":          key_id,
                    "user_name":       current_user.salon_name,
                    "user_email":      current_user.email,
                    "user_phone":      current_user.phone or "",
                }), 200

            except Exception as e:
                print(f"[Razorpay] subscription creation error: {e}")
                # Fall through to one-time order below

        # ── PATH B: One-time order (fallback if plan IDs not yet configured) ──
        try:
            rzp_order = client.order.create(data={
                "amount":   amount_paise,
                "currency": "INR",
                "receipt":  f"sf_{current_user.id}_{plan}_{secrets.token_hex(6)}",
                "notes": {
                    "user_id":    str(current_user.id),
                    "user_email": current_user.email,
                    "salon_name": current_user.salon_name,
                    "plan":       plan,
                    "cycle":      cycle,
                },
            })
        except Exception as e:
            print(f"[Razorpay] order creation error: {e}")
            return jsonify({"error": "Could not initiate payment. Please try again."}), 500

        payment = Payment(
            user_id           = current_user.id,
            razorpay_order_id = rzp_order["id"],
            plan              = plan,
            amount_paise      = amount_paise,
            billing_cycle     = cycle,
            status            = "created",
            ip_address        = _get_client_ip(),
        )
        db.session.add(payment)
        db.session.commit()

        return jsonify({
            "mode":     "order",
            "order_id": rzp_order["id"],
            "amount":   amount_paise,
            "currency": "INR",
            "plan":     plan,
            "plan_label": Plan.LABELS[plan],
            "key_id":   key_id,
            "user_name":  current_user.salon_name,
            "user_email": current_user.email,
            "user_phone": current_user.phone or "",
        }), 200

    # ── Verify Payment ─────────────────────────────────
    @app.route("/billing/verify-payment", methods=["POST"])
    @login_required
    @csrf.exempt
    def billing_verify_payment():
        """
        Handles both subscription and order payment verification.
        Subscription: HMAC(razorpay_payment_id + "|" + razorpay_subscription_id)
        Order:        HMAC(razorpay_order_id    + "|" + razorpay_payment_id)
        """
        data            = request.get_json(silent=True) or {}
        payment_id      = data.get("razorpay_payment_id", "").strip()
        subscription_id = data.get("razorpay_subscription_id", "").strip()
        order_id        = data.get("razorpay_order_id", "").strip()
        signature       = data.get("razorpay_signature", "").strip()

        if not payment_id or not signature:
            return jsonify({"error": "Missing payment parameters."}), 400

        key_sec = app.config.get("RAZORPAY_KEY_SECRET", "")

        # Verify HMAC — message differs for subscription vs order
        if subscription_id:
            msg = f"{payment_id}|{subscription_id}".encode()
            ref_id = subscription_id   # look up Payment by sub ID
        elif order_id:
            msg = f"{order_id}|{payment_id}".encode()
            ref_id = order_id
        else:
            return jsonify({"error": "Missing order_id or subscription_id."}), 400

        expected = hmac.new(key_sec.encode(), msg, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            print(f"[Razorpay] signature mismatch ref={ref_id}")
            return jsonify({"error": "Signature verification failed. Contact support if money was deducted."}), 400

        # Find payment record
        payment = Payment.query.filter_by(
            razorpay_order_id=ref_id,
            user_id=current_user.id
        ).first()
        if not payment:
            return jsonify({"error": "Payment record not found."}), 404

        # Idempotent
        if payment.status == "captured":
            return jsonify({"ok": True, "redirect": url_for("billing_success")}), 200

        payment.razorpay_payment_id = payment_id
        payment.status              = "captured"
        _activate_subscription(current_user, payment.plan, payment.billing_cycle, payment_id)

        # Store subscription ID for future renewal webhook matching
        if subscription_id:
            sub = current_user.subscription
            if sub: sub.gateway_subscription_id = subscription_id

        db.session.commit()
        print(f"[Razorpay] verified: {payment_id} sub={subscription_id} user={current_user.id} plan={payment.plan}")

        # Auto-assign a free bot instance now that payment is confirmed
        _assign_bot_for_user(current_user)

        return jsonify({"ok": True, "redirect": url_for("billing_success")}), 200

    # ── Success / Failed pages ──────────────────────────
    @app.route("/billing/success")
    @login_required
    def billing_success():
        return render_template("payment_success.html", user=current_user)

    @app.route("/billing/failed")
    @login_required
    def billing_failed():
        reason = request.args.get("reason", "Payment was not completed.")
        return render_template("payment_failed.html", user=current_user, reason=reason)

    # ── Cancel Subscription ─────────────────────────────
    @app.route("/billing/cancel", methods=["POST"])
    @login_required
    def billing_cancel():
        sub = current_user.subscription
        if sub and sub.is_active:
            sub.status      = "cancelled"
            sub.cancelled_at = datetime.now(timezone.utc)
            db.session.commit()
            flash("Subscription cancelled. Access continues until the billing period ends.", "info")
        return redirect(url_for("billing"))


# ─────────────────────────────────────────────────────────
#  ADMIN ROUTES
# ─────────────────────────────────────────────────────────

def register_admin_routes(app: Flask, mail: Mail):

    @app.route("/admin/login", methods=["GET", "POST"])
    @limiter.limit("5 per minute; 20 per hour", methods=["POST"],
                   error_message="Too many admin login attempts. Try again later.")
    def admin_login():
        if session.get("admin_id"):
            return redirect(url_for("admin_dashboard"))
        error = None
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            pw = request.form.get("password", "")
            admin = Admin.query.filter_by(email=email).first()
            if admin and admin.is_active and admin.check_password(pw):
                session["admin_id"] = admin.id
                session["admin_username"] = admin.username
                session.permanent = True
                admin.last_login = datetime.now(timezone.utc)
                db.session.commit()
                return redirect(url_for("admin_dashboard"))
            error = "Invalid credentials."
        return render_template("admin/admin_login.html", error=error)

    @app.route("/admin/logout")
    def admin_logout():
        session.pop("admin_id", None)
        session.pop("admin_username", None)
        flash("Signed out of admin panel.", "info")
        return redirect(url_for("admin_login"))

    @app.route("/admin")
    @app.route("/admin/dashboard")
    @admin_required
    def admin_dashboard():
        admin = get_current_admin()
        total_users = User.query.count()
        verified_users = User.query.filter_by(is_verified=True).count()
        pro_users = User.query.filter_by(plan=Plan.PRO).count()
        biz_users = User.query.filter_by(plan=Plan.BUSINESS).count()
        recent_users = (
            User.query.order_by(User.created_at.desc()).limit(10).all()
        )
        stats = {
            "total_users": total_users,
            "verified_users": verified_users,
            "starter_users": total_users - pro_users - biz_users,
            "pro_users": pro_users,
            "biz_users": biz_users,
            "unverified_users": total_users - verified_users,
        }
        return render_template(
            "admin/admin_dashboard.html",
            admin=admin,
            stats=stats,
            recent_users=recent_users,
            Plan=Plan,
        )

    @app.route("/admin/users")
    @admin_required
    def admin_users():
        page = request.args.get("page", 1, type=int)
        q = request.args.get("q", "").strip()
        query = User.query
        if q:
            query = query.filter(
                (User.email.ilike(f"%{q}%"))
                | (User.salon_name.ilike(f"%{q}%"))
                | (User.phone.ilike(f"%{q}%"))
            )
        users = query.order_by(User.created_at.desc()).paginate(page=page, per_page=20)
        return render_template("admin/admin_dashboard.html",
                               admin=get_current_admin(),
                               users_page=users, q=q,
                               stats=None, recent_users=None, Plan=Plan,
                               view="users")

    @app.route("/admin/users/<int:user_id>/toggle", methods=["POST"])
    @admin_required
    def admin_toggle_user(user_id):
        user = User.query.get_or_404(user_id)
        user.is_active = not user.is_active
        db.session.commit()
        status = "activated" if user.is_active else "deactivated"
        flash(f"User {user.email} has been {status}.", "success")
        return redirect(url_for("admin_dashboard"))

    @app.route("/admin/users/<int:user_id>/verify", methods=["POST"])
    @admin_required
    def admin_verify_user(user_id):
        user = User.query.get_or_404(user_id)
        user.is_verified = True
        user.verify_token = None
        db.session.commit()
        flash(f"User {user.email} manually verified.", "success")
        return redirect(url_for("admin_dashboard"))


# ─────────────────────────────────────────────────────────
#  API ROUTES (AJAX)
# ─────────────────────────────────────────────────────────

def register_api_routes(app: Flask):

    @app.route("/api/profile", methods=["PUT"])
    @login_required
    def api_profile_update():
        data = request.get_json(silent=True) or {}
        if salon_name := data.get("salon_name", "").strip():
            current_user.salon_name = salon_name
        if phone := data.get("phone", "").strip():
            current_user.phone = phone
        current_user.owner_name = data.get("owner_name", "").strip()
        current_user.city = data.get("city", "").strip()
        db.session.commit()
        return jsonify({"message": "Profile updated."}), 200

    @app.route("/api/whatsapp/status")
    @login_required
    def wa_status():
        from sqlalchemy import text as _t
        # Also return current QR code and wa_status for real-time polling
        row = db.session.execute(
            _t("SELECT wa_status, wa_qr_code FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        wa_st  = row.wa_status  if row else 1
        qr_b64 = row.wa_qr_code if row else None
        # If connected, authoritative: clear any stale QR from DB
        is_connected = bool(current_user.whatsapp_connected) or wa_st == 3
        if is_connected and qr_b64:
            try:
                db.session.execute(
                    _t("UPDATE users SET wa_qr_code=NULL, wa_status=3 WHERE id=:uid"),
                    {"uid": current_user.id}
                )
                db.session.commit()
            except Exception:
                db.session.rollback()
            qr_b64 = None
            wa_st  = 3

        return jsonify({
            "connected":  is_connected,
            "phone":      current_user.whatsapp_phone,
            "wa_status":  wa_st,
            "qr":         None if is_connected else qr_b64,
        })

    @app.route("/api/whatsapp/qr")
    @login_required
    def wa_qr():
        """Return the latest QR code for this user's WhatsApp session."""
        from sqlalchemy import text as _t
        row = db.session.execute(
            _t("SELECT wa_status, wa_qr_code FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        if not row:
            return jsonify({"ok": False, "error": "User not found"}), 404
        return jsonify({
            "ok":        True,
            "wa_status": row.wa_status,   # 1=idle 2=qr_ready 3=connected
            "qr":        row.wa_qr_code,
            "connected": bool(current_user.whatsapp_connected),
        })

    @app.route("/api/bot/control", methods=["POST"])
    @login_required
    @csrf.exempt
    def bot_control():
        """
        action: start | stop | restart | disconnect
        Finds which VPS / instance is running this user's session and
        sends the appropriate command to manager.py.
        disconnect = stop + release (clears session, frees slot).
        """
        data   = request.get_json(silent=True) or {}
        action = data.get("action", "").strip()
        if action not in ("start", "stop", "restart", "disconnect"):
            return jsonify({"ok": False, "error": "Invalid action"}), 400

        session_id = current_user.whatsapp_session_id
        if not session_id:
            # For disconnect: nothing to stop, just clear state and give a fresh session_id
            if action == "disconnect":
                from sqlalchemy import text as _t2
                import secrets as _sec2
                new_sid = _sec2.token_hex(24)
                db.session.execute(_t2("""
                    UPDATE users SET whatsapp_connected=FALSE,
                        whatsapp_session_id=:s, wa_status=1, wa_qr_code=NULL
                    WHERE id=:uid
                """), {"s": new_sid, "uid": current_user.id})
                db.session.commit()
                return jsonify({"ok": True, "result": {"message": "Cleared, ready for new connection"}}), 200
            return jsonify({"ok": False, "error": "No bot session linked to your account"}), 400

        from sqlalchemy import text as _t
        row = db.session.execute(_t("""
            SELECT v.public_ip, v.port, v.api_key, bi.id AS inst_id
            FROM bot_instances bi
            JOIN vps_servers v ON v.id = bi.vps_id
            WHERE bi.session_id = :sid
            LIMIT 1
        """), {"sid": session_id}).fetchone()

        if not row:
            return jsonify({"ok": False, "error": "No active bot instance found"}), 404

        bot_key     = row.api_key or os.environ.get("BOT_API_KEY", "")
        base_url    = f"http://{row.public_ip}:{row.port}"
        inst_id     = row.inst_id

        try:
            if action == "disconnect":
                # Release the slot entirely — frees the instance for another user
                url     = f"{base_url}/bot/release"
                payload = _json.dumps({"session_id": session_id}).encode()
            elif action == "restart":
                # Signal bot via DB (wa_status=9) — the bot's poll will pick it up
                # and do a clean restart. Also call manager as a fallback.
                from sqlalchemy import text as _t_restart
                db.session.execute(_t_restart(
                    "UPDATE users SET wa_status=9 WHERE id=:uid"
                ), {"uid": current_user.id})
                db.session.commit()
                url     = f"{base_url}/bot/{inst_id}/{action}"
                payload = b"{}"
            else:
                url     = f"{base_url}/bot/{inst_id}/{action}"
                payload = b"{}"

            req = urllib.request.Request(
                url, data=payload, method="POST",
                headers={"Content-Type": "application/json", "X-API-Key": bot_key}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = _json.loads(resp.read().decode())

            # After disconnect: clear WA state and assign a fresh session_id
            # so the user can immediately scan a new QR without manual steps.
            if action == "disconnect":
                import secrets as _sec
                new_sid = _sec.token_hex(24)
                db.session.execute(_t("""
                    UPDATE users
                    SET whatsapp_connected = FALSE,
                        whatsapp_session_id = :new_sid,
                        wa_status = 1,
                        wa_qr_code = NULL
                    WHERE id = :uid
                """), {"new_sid": new_sid, "uid": current_user.id})
                db.session.commit()

            return jsonify({"ok": True, "result": result}), 200

        except Exception as e:
            print(f"[BotControl] {action} failed for user {current_user.id}: {e}")
            return jsonify({"ok": False, "error": str(e)}), 502

    # ── Schedule mode (anytime vs hourly) ────────────────────────────────────
    @app.route("/api/schedule-mode", methods=["GET","POST"])
    @login_required
    @csrf.exempt
    def api_schedule_mode():
        from sqlalchemy import text as _t
        # Ensure column exists
        try:
            db.session.execute(_t("ALTER TABLE users ADD COLUMN IF NOT EXISTS schedule_mode VARCHAR(20) DEFAULT 'hourly'"))
            db.session.commit()
        except Exception:
            db.session.rollback()
        if request.method == "GET":
            row = db.session.execute(
                _t("SELECT schedule_mode FROM users WHERE id=:uid"), {"uid": current_user.id}
            ).fetchone()
            mode = (row.schedule_mode or "hourly") if row else "hourly"
            return jsonify({"ok": True, "schedule_mode": mode}), 200
        data = request.get_json(silent=True) or {}
        mode = data.get("schedule_mode","hourly")
        if mode not in ("anytime","hourly"): mode = "hourly"
        db.session.execute(
            _t("UPDATE users SET schedule_mode=:m WHERE id=:uid"), {"m": mode, "uid": current_user.id}
        )
        db.session.commit()
        return jsonify({"ok": True}), 200

    # ── Services CRUD ─────────────────────────────────
    @app.route("/api/services", methods=["GET"])
    @login_required
    @csrf.exempt
    def api_services_get():
        try:
            svcs = Service.query.filter_by(
                user_id=current_user.id, is_active=True
            ).order_by(Service.sort_order, Service.id).all()
            return jsonify({"services": [s.to_dict() for s in svcs]}), 200
        except Exception as e:
            print(f"[api_services_get] error: {e}")
            return jsonify({"services": [], "error": str(e)}), 200

    @app.route("/api/services", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_services_save():
        """Upsert the whole services list sent from the dashboard."""
        import json as _j
        data = request.get_json(silent=True) or {}
        services_data = data.get("services", [])
        # Soft-delete all existing, then recreate from payload
        Service.query.filter_by(user_id=current_user.id).delete()
        for i, s in enumerate(services_data):
            name = (s.get("name") or "").strip()
            if not name:
                continue
            svc = Service(
                user_id    = current_user.id,
                name       = name[:150],
                price      = max(0, int(s.get("price") or 0)),
                duration_min = max(5, int(s.get("duration_min") or 30)),
                is_active  = True,
                sort_order = i,
            )
            db.session.add(svc)
        db.session.commit()
        # Also persist into onboarding_data step_2 for consistency
        try:
            from sqlalchemy import text as sa_text
            import json as _j2
            row = db.session.execute(
                sa_text("SELECT onboarding_data FROM users WHERE id=:uid"),
                {"uid": current_user.id}
            ).fetchone()
            od = {}
            try: od = _j2.loads(row[0]) if row and row[0] else {}
            except: pass
            od["step_2"] = {"services": [{"name": s.get("name",""), "price": str(s.get("price",""))} for s in services_data if s.get("name","").strip()]}
            db.session.execute(
                sa_text("UPDATE users SET onboarding_data=:d WHERE id=:uid"),
                {"d": _j2.dumps(od), "uid": current_user.id}
            )
            db.session.commit()
        except Exception:
            pass
        updated = Service.query.filter_by(user_id=current_user.id, is_active=True)                               .order_by(Service.sort_order, Service.id).all()
        return jsonify({"ok": True, "services": [s.to_dict() for s in updated]}), 200

    # ── Schedule save/load ────────────────────────────
    @app.route("/api/schedule", methods=["GET"])
    @login_required
    def api_schedule_get():
        import json as _j
        from sqlalchemy import text as sa_text
        row = db.session.execute(
            sa_text("SELECT schedule_data FROM users WHERE id=:uid"),
            {"uid": current_user.id}
        ).fetchone()
        data = {}
        try:
            if row and row[0]: data = _j.loads(row[0])
        except: pass
        return jsonify({"schedule": data}), 200

    @app.route("/api/schedule", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_schedule_save():
        import json as _j
        from sqlalchemy import text as sa_text
        data = request.get_json(silent=True) or {}
        schedule = data.get("schedule", {})
        db.session.execute(
            sa_text("UPDATE users SET schedule_data=:d WHERE id=:uid"),
            {"d": _j.dumps(schedule), "uid": current_user.id}
        )
        db.session.commit()
        db.session.expire(current_user)
        return jsonify({"ok": True}), 200

    # ── Profile update (JSON) ─────────────────────────
    @app.route("/api/profile/update", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_profile_update_json():
        from sqlalchemy import text as sa_text
        data = request.get_json(silent=True) or {}
        sn = (data.get("salon_name") or "").strip()
        db.session.execute(sa_text("""
            UPDATE users SET
              salon_name = CASE WHEN :sn != '' THEN :sn ELSE salon_name END,
              owner_name = :on,
              phone      = :ph,
              city       = :ci,
              address    = :ad
            WHERE id = :uid
        """), {
            "sn": sn,
            "on": (data.get("owner_name") or "").strip(),
            "ph": (data.get("phone") or "").strip(),
            "ci": (data.get("city") or "").strip(),
            "ad": (data.get("address") or "").strip(),
            "uid": current_user.id,
        })
        db.session.commit()
        db.session.expire(current_user)
        return jsonify({"ok": True}), 200

    # ── Bot instructions update ───────────────────────
    @app.route("/api/profile/bot-instructions", methods=["POST"])
    @login_required
    @csrf.exempt
    def api_bot_instructions():
        from sqlalchemy import text as sa_text
        data = request.get_json(silent=True) or {}
        instr = (data.get("instructions") or "").strip()
        db.session.execute(
            sa_text("UPDATE users SET whatsapp_instructions=:i WHERE id=:uid"),
            {"i": instr, "uid": current_user.id}
        )
        db.session.commit()
        db.session.expire(current_user)
        return jsonify({"ok": True}), 200


# ─────────────────────────────────────────────────────────
#  ERROR HANDLERS
# ─────────────────────────────────────────────────────────

def register_error_handlers(app: Flask):

    @app.errorhandler(429)
    def rate_limited(e):
        ip = _get_client_ip()
        # Record violation — blocks permanently after threshold
        permanently_blocked = _record_violation(ip, reason="rate_limit_exceeded")
        msg = (
            "Your IP has been permanently blocked due to repeated abuse."
            if permanently_blocked
            else str(e.description) if hasattr(e, 'description') else
                 "Too many requests. Please slow down."
        )
        return jsonify({"error": msg}), 429

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template("404.html"), 404

    @app.errorhandler(403)
    def forbidden(e):
        return render_template("404.html", code=403, msg="Access Denied"), 403

    @app.errorhandler(500)
    def server_error(e):
        return render_template("404.html", code=500, msg="Internal Server Error"), 500


# ─────────────────────────────────────────────────────────
#  PAYMENT HELPERS
# ─────────────────────────────────────────────────────────

def _assign_bot_for_user(user):
    """
    Find the VPS with most free slots and tell its manager to assign a bot.
    Silently ignores errors so payment flow never fails due to bot issues.
    """
    try:
        from sqlalchemy import text as _t
        row = db.session.execute(_t("""
            SELECT v.public_ip, v.port, v.api_key,
                   COUNT(CASE WHEN b.status='free' THEN 1 END) AS free_count
            FROM vps_servers v
            LEFT JOIN bot_instances b ON b.vps_id = v.id
            WHERE v.is_active = TRUE
            GROUP BY v.id
            HAVING COUNT(CASE WHEN b.status='free' THEN 1 END) > 0
            ORDER BY free_count DESC
            LIMIT 1
        """)).fetchone()
        if not row:
            print(f"[BotAssign] No free VPS slots available for user {user.id}")
            return

        session_id = user.whatsapp_session_id
        if not session_id:
            print(f"[BotAssign] User {user.id} has no whatsapp_session_id")
            return

        manager_url = f"http://{row.public_ip}:{row.port}/bot/assign"
        bot_key     = row.api_key or os.environ.get("BOT_API_KEY", "")
        payload     = _json.dumps({"session_id": session_id, "user_id": user.id}).encode()
        req = urllib.request.Request(
            manager_url, data=payload, method="POST",
            headers={"Content-Type": "application/json", "X-API-Key": bot_key}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = _json.loads(resp.read().decode())
        print(f"[BotAssign] Assigned bot for user {user.id}: {result}")
    except Exception as e:
        print(f"[BotAssign] Failed for user {user.id}: {e}")


def _activate_subscription(user, plan, billing_cycle, payment_id):
    """Upgrade user plan and subscription after verified payment."""
    from datetime import timedelta
    sub = user.subscription
    if not sub:
        sub = Subscription(user_id=user.id)
        db.session.add(sub)
    sub.plan                = plan
    sub.status              = "active"
    sub.billing_cycle       = billing_cycle
    sub.last_payment_id     = payment_id
    sub.amount_inr          = Plan.PRICES.get(plan, 0)
    sub.current_period_start = datetime.now(timezone.utc)
    sub.current_period_end  = (
        datetime.now(timezone.utc) + timedelta(days=365)
        if billing_cycle == "annual"
        else datetime.now(timezone.utc) + timedelta(days=31)
    )
    user.plan = plan


# ─────────────────────────────────────────────────────────
#  WEBHOOK ROUTES
# ─────────────────────────────────────────────────────────

def register_webhook_routes(app: Flask):

    @app.route("/webhooks/razorpay", methods=["POST"])
    def razorpay_webhook():
        """Verify Razorpay webhook signature then dispatch event."""
        import json as _wj
        raw_body       = request.get_data()
        sig_header     = request.headers.get("X-Razorpay-Signature", "")
        webhook_secret = app.config.get("RAZORPAY_WEBHOOK_SECRET", "")

        if not webhook_secret:
            print("[Webhook] RAZORPAY_WEBHOOK_SECRET not set")
            return jsonify({"error": "Not configured"}), 503

        expected = hmac.new(webhook_secret.encode(), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig_header):
            print("[Webhook] invalid signature — rejected")
            return jsonify({"error": "Invalid signature"}), 400

        try:
            event = _wj.loads(raw_body.decode())
        except Exception:
            return jsonify({"error": "Invalid JSON"}), 400

        event_id   = event.get("id", "")
        event_type = event.get("event", "")
        payload    = event.get("payload", {})
        print(f"[Webhook] event={event_type} id={event_id}")

        if   event_type == "payment.captured":         _wh_payment_captured(event_id, payload)
        elif event_type == "payment.failed":            _wh_payment_failed(event_id, payload)
        elif event_type == "subscription.activated":    _wh_subscription_activated(event_id, payload)
        elif event_type == "subscription.charged":      _wh_subscription_charged(event_id, payload)
        elif event_type == "subscription.cancelled":    _wh_subscription_cancelled(event_id, payload)
        elif event_type == "subscription.halted":       _wh_subscription_cancelled(event_id, payload)
        else:
            print(f"[Webhook] unhandled event type: {event_type}")

        return jsonify({"ok": True}), 200


def _wh_payment_captured(event_id: str, payload: dict):
    """Handle payment.captured — idempotent."""
    import json as _wj
    try:
        entity         = payload.get("payment", {}).get("entity", {})
        rzp_payment_id = entity.get("id", "")
        rzp_order_id   = entity.get("order_id", "")
        if not rzp_payment_id or not rzp_order_id:
            print("[Webhook] payment.captured missing IDs"); return

        payment = Payment.query.filter_by(razorpay_order_id=rzp_order_id).first()
        if not payment:
            print(f"[Webhook] no Payment for order {rzp_order_id}"); return

        # Idempotency — skip if already seen this event
        processed = []
        try: processed = _wj.loads(payment.webhook_events or "[]")
        except Exception: pass
        if event_id in processed:
            print(f"[Webhook] duplicate event {event_id} — skipped"); return
        if payment.status == "captured":
            print("[Webhook] already captured — skipped"); return

        payment.razorpay_payment_id = rzp_payment_id
        payment.status              = "captured"
        processed.append(event_id)
        payment.webhook_events      = _wj.dumps(processed)

        user = User.query.get(payment.user_id)
        if user:
            _activate_subscription(user, payment.plan, payment.billing_cycle, rzp_payment_id)
            db.session.commit()
            print(f"[Webhook] subscription activated user={user.id} plan={payment.plan}")
        else:
            print(f"[Webhook] user not found for payment {rzp_payment_id}")
    except Exception as e:
        print(f"[Webhook] payment.captured error: {e}")
        try: db.session.rollback()
        except Exception: pass


def _wh_payment_failed(event_id: str, payload: dict):
    """Handle payment.failed — log reason and update status."""
    import json as _wj
    try:
        entity         = payload.get("payment", {}).get("entity", {})
        rzp_payment_id = entity.get("id", "")
        rzp_order_id   = entity.get("order_id", "")
        reason         = entity.get("error_description", "Unknown failure")

        payment = Payment.query.filter_by(razorpay_order_id=rzp_order_id).first()
        if not payment or payment.status == "captured": return

        processed = []
        try: processed = _wj.loads(payment.webhook_events or "[]")
        except Exception: pass
        if event_id in processed: return

        payment.razorpay_payment_id = rzp_payment_id
        payment.status              = "failed"
        payment.failure_reason      = reason[:300]
        processed.append(event_id)
        payment.webhook_events      = _wj.dumps(processed)
        db.session.commit()
        print(f"[Webhook] payment failed: {rzp_payment_id} reason={reason}")
    except Exception as e:
        print(f"[Webhook] payment.failed error: {e}")
        try: db.session.rollback()
        except Exception: pass


def _wh_subscription_activated(event_id: str, payload: dict):
    """subscription.activated — first payment collected, subscription live."""
    import json as _wj
    try:
        sub_entity = payload.get("subscription", {}).get("entity", {})
        rzp_sub_id = sub_entity.get("id", "")
        plan_id    = sub_entity.get("plan_id", "")
        print(f"[Webhook] subscription activated: {rzp_sub_id}")

        sub = Subscription.query.filter_by(gateway_subscription_id=rzp_sub_id).first()
        if not sub: return

        sub.status = "active"
        db.session.commit()
    except Exception as e:
        print(f"[Webhook] subscription.activated error: {e}")
        try: db.session.rollback()
        except Exception: pass


def _wh_subscription_charged(event_id: str, payload: dict):
    """subscription.charged — recurring charge succeeded, extend period."""
    import json as _wj
    from datetime import timedelta
    try:
        sub_entity = payload.get("subscription", {}).get("entity", {})
        pay_entity = payload.get("payment",      {}).get("entity", {})
        rzp_sub_id = sub_entity.get("id", "")
        payment_id = pay_entity.get("id", "")

        sub = Subscription.query.filter_by(gateway_subscription_id=rzp_sub_id).first()
        if not sub: return

        sub.status          = "active"
        sub.last_payment_id = payment_id
        sub.current_period_start = datetime.now(timezone.utc)
        delta = timedelta(days=365) if sub.billing_cycle == "annual" else timedelta(days=31)
        sub.current_period_end = datetime.now(timezone.utc) + delta
        db.session.commit()
        print(f"[Webhook] subscription charged: {payment_id} sub={rzp_sub_id}")
    except Exception as e:
        print(f"[Webhook] subscription.charged error: {e}")
        try: db.session.rollback()
        except Exception: pass


def _wh_subscription_cancelled(event_id: str, payload: dict):
    """subscription.cancelled / halted — update status."""
    try:
        sub_entity = payload.get("subscription", {}).get("entity", {})
        rzp_sub_id = sub_entity.get("id", "")

        sub = Subscription.query.filter_by(gateway_subscription_id=rzp_sub_id).first()
        if not sub: return

        sub.status       = "cancelled"
        sub.cancelled_at = datetime.now(timezone.utc)
        db.session.commit()
        print(f"[Webhook] subscription cancelled: {rzp_sub_id}")
    except Exception as e:
        print(f"[Webhook] subscription.cancelled error: {e}")
        try: db.session.rollback()
        except Exception: pass


# ─────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────

app = create_app()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)