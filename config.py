import os
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── Core ──────────────────────────────────────────
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key-CHANGE-IN-PROD")
    WTF_CSRF_SECRET_KEY = os.environ.get("WTF_CSRF_SECRET_KEY", SECRET_KEY)

    # ── Database ──────────────────────────────────────
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        "DATABASE_URL", "postgresql://localhost/salonflow"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_recycle": 300,
        "pool_pre_ping": True,
    }

    # ── Session ───────────────────────────────────────
    PERMANENT_SESSION_LIFETIME = timedelta(days=7)
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"

    # ── Email ─────────────────────────────────────────
    MAIL_SERVER = os.environ.get("MAIL_SERVER", "smtp.hostinger.com")
    MAIL_PORT = int(os.environ.get("MAIL_PORT", 587))
    MAIL_USERNAME = os.environ.get("MAIL_USERNAME", "support@salonflow.in")
    MAIL_PASSWORD = os.environ.get("MAIL_PASSWORD", "")
    MAIL_DEFAULT_SENDER = os.environ.get(
        "MAIL_DEFAULT_SENDER", "SalonFlow <support@salonflow.in>"
    )
    # Port 465 = implicit SSL (SMTPS). Port 587 = STARTTLS.
    # Never enable both simultaneously - Flask-Mail breaks if you do.
    MAIL_USE_SSL = (MAIL_PORT == 465)
    MAIL_USE_TLS = (not MAIL_USE_SSL) and (
        os.environ.get("MAIL_USE_TLS", "True").lower() == "true"
    )
    MAIL_TIMEOUT = 10  # fail fast instead of blocking for 30s

    # ── App ───────────────────────────────────────────
    APP_NAME = os.environ.get("APP_NAME", "SalonFlow")
    APP_URL = os.environ.get("APP_URL", "http://localhost:5000")
    SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL", "support@salonflow.in")
    ADMIN_SECRET_KEY = os.environ.get("ADMIN_SECRET_KEY", "admin-secret-CHANGE")

    # ── Google Places API ────────────────────────────────────
    GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")

    # ── reCAPTCHA v3 ─────────────────────────────────────
    RECAPTCHA_SITE_KEY   = os.environ.get("RECAPTCHA_SITE_KEY",   "")
    RECAPTCHA_SECRET_KEY = os.environ.get("RECAPTCHA_SECRET_KEY", "")
    RECAPTCHA_SCORE_THRESHOLD = 0.5   # scores below this are flagged as bots
    RECAPTCHA_ENABLED    = True        # set False in tests

    # Firebase / Google sign-in
    FIREBASE_API_KEY = os.environ.get("FIREBASE_API_KEY", "")
    FIREBASE_AUTH_DOMAIN = os.environ.get("FIREBASE_AUTH_DOMAIN", "")
    FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "")
    FIREBASE_STORAGE_BUCKET = os.environ.get("FIREBASE_STORAGE_BUCKET", "")
    FIREBASE_MESSAGING_SENDER_ID = os.environ.get("FIREBASE_MESSAGING_SENDER_ID", "")
    FIREBASE_APP_ID = os.environ.get("FIREBASE_APP_ID", "")

    # ── Bot Manager ──────────────────────────────────────
    BOT_API_KEY = os.environ.get("BOT_API_KEY", "")

    # ── Razorpay ─────────────────────────────────────────
    RAZORPAY_KEY_ID     = os.environ.get("RAZORPAY_KEY_ID", "")
    RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET", "")
    RAZORPAY_WEBHOOK_SECRET = os.environ.get("RAZORPAY_WEBHOOK_SECRET", "")
    # Razorpay Plan IDs for recurring subscriptions
    # Create these once in Razorpay Dashboard → Subscriptions → Plans
    RAZORPAY_PLAN_PRO_MONTHLY  = os.environ.get("RAZORPAY_PLAN_PRO_MONTHLY",  "")
    RAZORPAY_PLAN_PRO_ANNUAL   = os.environ.get("RAZORPAY_PLAN_PRO_ANNUAL",   "")

    # ── Rate limiting & IP blocking ───────────────────────
    # Violations before permanent block
    IP_BLOCK_THRESHOLD   = 3

    # ── Token expiry ──────────────────────────────────────
    PASSWORD_RESET_EXPIRE_HOURS = 2
    EMAIL_VERIFY_EXPIRE_HOURS = 48


class DevelopmentConfig(Config):
    DEBUG = True
    SESSION_COOKIE_SECURE = False


class ProductionConfig(Config):
    DEBUG = False
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True


config = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "default": DevelopmentConfig,
}
