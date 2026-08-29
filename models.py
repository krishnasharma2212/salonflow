from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from flask_bcrypt import Bcrypt

db = SQLAlchemy()
bcrypt = Bcrypt()


# ─────────────────────────────────────────────────────────
#  PLAN CONSTANTS
# ─────────────────────────────────────────────────────────
class Plan:
    STARTER = "starter"
    PRO = "pro"
    BUSINESS = "business"

    PRICES = {STARTER: 0, PRO: 999, BUSINESS: 2499}
    LABELS = {STARTER: "Starter", PRO: "Pro", BUSINESS: "Business"}


# ─────────────────────────────────────────────────────────
#  USER MODEL
# ─────────────────────────────────────────────────────────
class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)

    # Basic info
    salon_name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(200), unique=True, nullable=False, index=True)
    phone = db.Column(db.String(20), nullable=True)
    _password = db.Column("password_hash", db.String(255), nullable=False)

    # Profile extras
    owner_name = db.Column(db.String(120), nullable=True)
    city = db.Column(db.String(80), nullable=True)
    address = db.Column(db.Text, nullable=True)
    google_maps_url = db.Column(db.Text, nullable=True)
    avatar_url = db.Column(db.String(300), nullable=True)

    # Verification / status
    is_verified = db.Column(db.Boolean, default=False, nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    verify_token = db.Column(db.String(200), nullable=True, unique=True, index=True)
    verify_token_expires = db.Column(db.DateTime(timezone=True), nullable=True)

    # Password reset
    reset_token = db.Column(db.String(200), nullable=True, unique=True, index=True)
    reset_token_expires = db.Column(db.DateTime(timezone=True), nullable=True)

    # Plan
    plan = db.Column(db.String(20), default=Plan.STARTER, nullable=False)

    # Timestamps
    created_at = db.Column(
        db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = db.Column(
        db.DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    last_login = db.Column(db.DateTime(timezone=True), nullable=True)

    # WhatsApp
    whatsapp_connected    = db.Column(db.Boolean, default=False)
    whatsapp_phone        = db.Column(db.String(20), nullable=True)
    whatsapp_instructions = db.Column(db.Text, nullable=True)
    whatsapp_session_id   = db.Column(db.String(64), nullable=True, unique=True, index=True)

    # Payment collection (advance booking fee)
    upi_id                = db.Column(db.String(100), nullable=True)
    upi_qr_code           = db.Column(db.Text, nullable=True)   # base64 image data URL

    # Bot behaviour settings (JSON blob)
    # { language: 'auto'|'english'|'hindi'|'hinglish',
    #   ignore_schedule: false }
    bot_settings          = db.Column(db.Text, nullable=True)

    # Onboarding
    onboarding_complete = db.Column(db.Boolean, default=False, nullable=False)
    onboarding_step = db.Column(db.Integer, default=1, nullable=False)
    onboarding_data = db.Column(db.Text, nullable=True)  # JSON blob

    schedule_data = db.Column(db.Text, nullable=True)  # JSON blob for schedule manager

    # Relationships
    subscription = db.relationship(
        "Subscription", back_populates="user", uselist=False, cascade="all, delete-orphan"
    )

    # ── Password ──────────────────────────────────────
    @property
    def password(self):
        raise AttributeError("Password is write-only.")

    @password.setter
    def password(self, plain: str):
        self._password = bcrypt.generate_password_hash(plain).decode("utf-8")

    def check_password(self, plain: str) -> bool:
        return bcrypt.check_password_hash(self._password, plain)

    # ── Helpers ───────────────────────────────────────
    @property
    def plan_label(self):
        return Plan.LABELS.get(self.plan, "Starter")

    @property
    def plan_price(self):
        return Plan.PRICES.get(self.plan, 0)

    @property
    def initials(self):
        parts = (self.salon_name or "S").split()
        return "".join(p[0].upper() for p in parts[:2])

    def record_login(self):
        self.last_login = datetime.now(timezone.utc)
        db.session.commit()

    def __repr__(self):
        return f"<User {self.email}>"


# ─────────────────────────────────────────────────────────
#  SUBSCRIPTION MODEL
# ─────────────────────────────────────────────────────────
class Subscription(db.Model):
    __tablename__ = "subscriptions"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer, db.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, unique=True
    )

    plan = db.Column(db.String(20), default=Plan.STARTER, nullable=False)
    status = db.Column(db.String(30), default="active", nullable=False)
    # Status values: active | trialing | cancelled | expired | past_due

    amount_inr = db.Column(db.Integer, default=0)  # in rupees
    billing_cycle = db.Column(db.String(20), default="monthly")  # monthly | annual

    # Razorpay / payment gateway refs
    gateway_customer_id = db.Column(db.String(100), nullable=True)
    gateway_subscription_id = db.Column(db.String(100), nullable=True)
    last_payment_id = db.Column(db.String(100), nullable=True)

    trial_ends_at = db.Column(db.DateTime(timezone=True), nullable=True)
    current_period_start = db.Column(db.DateTime(timezone=True), nullable=True)
    current_period_end = db.Column(db.DateTime(timezone=True), nullable=True)
    cancelled_at = db.Column(db.DateTime(timezone=True), nullable=True)

    created_at = db.Column(
        db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = db.Column(
        db.DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationship
    user = db.relationship("User", back_populates="subscription")

    @property
    def is_active(self):
        return self.status in ("active", "trialing")

    def __repr__(self):
        return f"<Subscription user={self.user_id} plan={self.plan}>"


# ─────────────────────────────────────────────────────────
#  ADMIN MODEL
# ─────────────────────────────────────────────────────────
class Admin(UserMixin, db.Model):
    __tablename__ = "admins"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(200), unique=True, nullable=False, index=True)
    _password = db.Column("password_hash", db.String(255), nullable=False)
    is_superadmin = db.Column(db.Boolean, default=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(
        db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    last_login = db.Column(db.DateTime(timezone=True), nullable=True)

    @property
    def password(self):
        raise AttributeError("Password is write-only.")

    @password.setter
    def password(self, plain: str):
        self._password = bcrypt.generate_password_hash(plain).decode("utf-8")

    def check_password(self, plain: str) -> bool:
        return bcrypt.check_password_hash(self._password, plain)

    def __repr__(self):
        return f"<Admin {self.username}>"


# ─────────────────────────────────────────────────────────
#  BLOCKED IP MODEL
# ─────────────────────────────────────────────────────────
class BlockedIP(db.Model):
    __tablename__ = "blocked_ips"

    id            = db.Column(db.Integer, primary_key=True)
    ip_address    = db.Column(db.String(50), unique=True, nullable=False, index=True)
    violation_count = db.Column(db.Integer, default=1, nullable=False)
    is_blocked    = db.Column(db.Boolean, default=False, nullable=False)
    reason        = db.Column(db.String(200), nullable=True)
    first_seen    = db.Column(db.DateTime(timezone=True),
                              default=lambda: datetime.now(timezone.utc))
    blocked_at    = db.Column(db.DateTime(timezone=True), nullable=True)

    def __repr__(self):
        return f"<BlockedIP {self.ip_address} blocked={self.is_blocked}>"

# ─────────────────────────────────────────────────────────
#  SERVICE MODEL
# ─────────────────────────────────────────────────────────
class Service(db.Model):
    __tablename__ = "services"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="CASCADE"),
                           nullable=False, index=True)
    name       = db.Column(db.String(150), nullable=False)
    price      = db.Column(db.Integer, default=0)        # price in rupees (0 = free/TBD)
    duration_min = db.Column(db.Integer, default=30)     # in minutes
    is_active  = db.Column(db.Boolean, default=True)
    sort_order = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime(timezone=True),
                           default=lambda: datetime.now(timezone.utc))

    user = db.relationship("User", backref=db.backref("services",
                            cascade="all, delete-orphan", lazy="dynamic"))

    def to_dict(self):
        return {
            "id":           self.id,
            "name":         self.name,
            "price":        self.price,
            "duration_min": self.duration_min,
            "is_active":    self.is_active,
            "sort_order":   self.sort_order,
        }

    def __repr__(self):
        return f"<Service {self.name} user={self.user_id}>"

# ─────────────────────────────────────────────────────────
#  PAYMENT MODEL
# ─────────────────────────────────────────────────────────
class Payment(db.Model):
    __tablename__ = "payments"

    id             = db.Column(db.Integer, primary_key=True)
    user_id        = db.Column(db.Integer, db.ForeignKey("users.id", ondelete="CASCADE"),
                                nullable=False, index=True)

    # Razorpay identifiers
    razorpay_order_id   = db.Column(db.String(100), unique=True, nullable=False, index=True)
    razorpay_payment_id = db.Column(db.String(100), nullable=True, index=True)  # set after capture

    # What was bought
    plan           = db.Column(db.String(20), nullable=False)
    amount_paise   = db.Column(db.Integer, nullable=False)   # amount in paise (₹ × 100)
    currency       = db.Column(db.String(10), default="INR")
    billing_cycle  = db.Column(db.String(20), default="monthly")

    # Status lifecycle: created → captured | failed
    status         = db.Column(db.String(30), default="created", nullable=False, index=True)

    # Webhook idempotency — store raw event IDs we have processed
    webhook_events = db.Column(db.Text, nullable=True)  # JSON list of processed event IDs

    # Metadata
    notes          = db.Column(db.Text, nullable=True)  # JSON blob for extra info
    failure_reason = db.Column(db.String(300), nullable=True)
    ip_address     = db.Column(db.String(50), nullable=True)

    created_at     = db.Column(db.DateTime(timezone=True),
                                default=lambda: datetime.now(timezone.utc))
    updated_at     = db.Column(db.DateTime(timezone=True),
                                default=lambda: datetime.now(timezone.utc),
                                onupdate=lambda: datetime.now(timezone.utc))

    user = db.relationship("User", backref=db.backref("payments",
                            cascade="all, delete-orphan", lazy="dynamic"))

    @property
    def amount_rupees(self):
        return self.amount_paise // 100

    def to_dict(self):
        return {
            "id":                   self.id,
            "razorpay_order_id":    self.razorpay_order_id,
            "razorpay_payment_id":  self.razorpay_payment_id,
            "plan":                 self.plan,
            "amount_rupees":        self.amount_rupees,
            "status":               self.status,
            "created_at":           self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<Payment order={self.razorpay_order_id} status={self.status}>"