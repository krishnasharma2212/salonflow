<p align="center">
  <img src="docs/assets/logo.svg" alt="SalonFlow" width="760">
</p>

<p align="center">
  <strong>SalonFlow is a salon automation platform that lets customers book appointments through WhatsApp while owners manage services, schedules, payments, and subscriptions from a web dashboard.</strong>
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-Flask-2563eb">
  <img alt="Node" src="https://img.shields.io/badge/Node.js-WhatsApp%20Bot-0f766e">
  <img alt="Database" src="https://img.shields.io/badge/PostgreSQL-Database-334155">
  <img alt="Payments" src="https://img.shields.io/badge/Razorpay-Payments-f59e0b">
</p>

## What SalonFlow Does

SalonFlow is built for salon owners who lose time answering repeated booking messages, checking available slots, confirming appointments, and collecting advance payments manually.

With SalonFlow, an owner sets up their salon once: services, prices, opening hours, payment settings, and WhatsApp instructions. After that, customers can message the salon on WhatsApp and the bot can answer questions, check availability, reserve slots, request payment proof, and keep the dashboard updated.

<p align="center">
  <img src="docs/assets/product-flow.svg" alt="SalonFlow product workflow" width="900">
</p>

## Who This Is For

This project is useful for:

- Salon owners who want bookings to happen even when staff are busy.
- Small teams that want one dashboard for appointments, services, and payment tracking.
- Developers building a SaaS product around WhatsApp-based appointment automation.
- Operators who need a scalable bot manager that can run multiple WhatsApp sessions across VPS machines.

## Main Features

- WhatsApp booking assistant: customers can book, reschedule, cancel, and ask about services directly in WhatsApp.
- QR-based WhatsApp connection: salon owners connect a WhatsApp number from the dashboard.
- Smart appointment handling: stores appointment data, checks schedule conflicts, supports buffers, and shows upcoming bookings.
- Service management: owners can configure service names, prices, duration, and active status.
- Onboarding flow: step-by-step setup for salon profile, services, schedule, payments, and WhatsApp.
- Advance payment flow: sends branded UPI QR codes and verifies payment screenshots with AI.
- Google Calendar support: bot can create, update, and delete calendar events when user credentials are available.
- Billing and plans: Starter, Pro, and Business plans with Razorpay checkout and webhook handling.
- Admin panel: view users, verification status, plan distribution, and account controls.
- Security basics: CSRF protection, password hashing, rate limiting, IP blocking, email verification, and reset tokens.

## Simple Example

A customer sends:

```text
Hi, can I book a haircut tomorrow at 4 PM?
```

SalonFlow can:

1. Read the salon's working hours and service list.
2. Check whether 4 PM is available.
3. Ask for missing details if needed.
4. Reserve the appointment.
5. Send payment instructions if advance payment is enabled.
6. Confirm the booking after payment proof is verified.
7. Show the appointment in the owner dashboard.

## Architecture

<p align="center">
  <img src="docs/assets/architecture.svg" alt="SalonFlow architecture" width="940">
</p>

### High-Level Components

| Area | Files | Purpose |
| --- | --- | --- |
| Flask web app | `app.py`, `config.py`, `models.py` | Authentication, dashboard APIs, billing, admin panel, webhooks, and server-rendered pages. |
| Templates | `templates/` | Public site, login/signup, onboarding, dashboard, billing, profile, admin, and email views. |
| Static assets | `static/` | CSS, favicon, screenshots uploaded during payment verification. |
| WhatsApp bot | `bot/reply.js`, `bot/package.json` | Baileys WhatsApp session, OpenAI replies, booking tools, reminders, payment screenshot checks. |
| Bot manager | `manager.py` | Provisions and controls bot instances across VPS machines. |
| Documentation assets | `docs/assets/` | Logo and diagrams used by this README. |

## Tech Stack

- Backend: Python, Flask, SQLAlchemy, Flask-Login, Flask-Mail, Flask-WTF, Flask-Limiter.
- Database: PostgreSQL.
- Bot runtime: Node.js 18+, Baileys, OpenAI SDK, Google APIs, PostgreSQL client.
- Payments: Razorpay orders/subscriptions and webhooks.
- Authentication: Email/password plus optional Firebase Google sign-in.
- Maps: Google Places autocomplete during onboarding.
- Deployment: Gunicorn/systemd for web app and manager, PM2/systemd style process control for bot instances.

## Project Structure

```text
SalonFlow/
  app.py                  Flask application factory and routes
  config.py               Environment-driven configuration
  models.py               SQLAlchemy models for users, plans, payments, services
  manager.py              VPS bot manager and instance scaler
  requirements.txt        Python dependencies
  .env.example            Safe environment template
  bot/
    reply.js              WhatsApp AI booking bot
    package.json          Bot dependencies and scripts
  static/
    css/main.css          Shared CSS helpers
    icons/favicon.svg     App favicon
  templates/              HTML pages and email templates
  docs/assets/            README logo and diagrams
```

## Required Services

SalonFlow can run locally with the core Flask app and PostgreSQL. For the full production feature set, configure these services:

| Service | Needed For |
| --- | --- |
| PostgreSQL | User accounts, appointments, subscriptions, services, bot state. |
| SMTP email account | Email verification and password reset. |
| OpenAI API | WhatsApp bot replies, voice transcription, payment screenshot verification. |
| Firebase Auth | Optional Google sign-in on the web app. |
| Google Places API | Address autocomplete during onboarding. |
| Razorpay | Paid plans, subscriptions, payment verification, webhooks. |
| VPS server | Running one or more WhatsApp bot instances. |

## Local Setup

### 1. Clone and enter the project

```bash
git clone https://github.com/your-org/salonflow.git
cd salonflow
```

### 2. Create a Python virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Create your environment file

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Open `.env` and replace every `replace-with-...` value with your real local credentials.

### 4. Create the PostgreSQL database

Create a database named `salonflow` and make sure `DATABASE_URL` in `.env` points to it.

Example:

```text
DATABASE_URL=postgresql://salonflow:your-password@localhost:5432/salonflow
```

### 5. Initialize tables

```bash
flask init-db
```

Then create an admin user:

```bash
flask create-admin
```

### 6. Run the Flask app

```bash
python app.py
```

Open:

```text
http://localhost:5000
```

## Running The WhatsApp Bot Locally

The bot lives in `bot/` and is a separate Node.js process.

```bash
cd bot
npm install
npm run dev
```

The bot needs these values in its environment:

```text
DATABASE_URL
OPENAI_API_KEY
OPENAI_MODEL
DEBOUNCE_MS
APPT_BUFFER_MIN
LOG_LEVEL
```

The first run creates WhatsApp credential files under `wa_credentials/`. These are intentionally ignored by Git because they are live session secrets.

## Bot Manager Deployment

`manager.py` is used when you want to run many WhatsApp bot instances on a VPS.

Before starting it on a server, set:

```text
DATABASE_URL
BOT_API_KEY
FLASK_APP_URL
MANAGER_PORT
```

Example commands:

```bash
sudo python3 manager.py 5
sudo python3 manager.py --status
sudo python3 manager.py --update-code
sudo python3 manager.py --serve
```

What it does:

- Registers the VPS in the database.
- Downloads the latest bot source from the Flask app.
- Creates isolated bot instance folders.
- Writes per-instance `.env` files.
- Starts, stops, restarts, and releases bot instances.
- Exposes a small manager API protected by `BOT_API_KEY`.

## Environment Variables

The app is designed so real secrets are never stored in source code. Use `.env` locally and your hosting provider's secret manager in production.

| Variable | Required | Description |
| --- | --- | --- |
| `SECRET_KEY` | Yes | Flask session signing secret. |
| `WTF_CSRF_SECRET_KEY` | Yes | CSRF signing secret. |
| `DATABASE_URL` | Yes | PostgreSQL connection string. |
| `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USERNAME`, `MAIL_PASSWORD` | Recommended | SMTP settings for verification and reset email. |
| `APP_URL` | Recommended | Public app URL used for links. |
| `SUPPORT_EMAIL` | Recommended | Public support email shown to users. |
| `GOOGLE_PLACES_API_KEY` | Optional | Enables Google Places autocomplete. |
| `RECAPTCHA_SITE_KEY`, `RECAPTCHA_SECRET_KEY` | Optional | Enables reCAPTCHA during signup. |
| `FIREBASE_*` | Optional | Enables Google sign-in. |
| `BOT_API_KEY` | Required for bot fleet | Shared secret between Flask and the bot manager. |
| `OPENAI_API_KEY` | Required for bot | Powers AI replies, transcription, and screenshot checks. |
| `RAZORPAY_*` | Required for paid plans | Checkout, subscriptions, and webhooks. |

See `.env.example` for the complete list.

## Security Checklist Before Publishing

- Keep `.env` private. Only `.env.example` belongs on GitHub.
- Rotate any credentials that were ever stored in code or shared outside the deployment machine.
- Use long random values for `SECRET_KEY`, `WTF_CSRF_SECRET_KEY`, `ADMIN_SECRET_KEY`, and `BOT_API_KEY`.
- Restrict Google/Firebase keys by domain in the Google/Firebase console.
- Restrict Razorpay webhooks to the production endpoint and verify webhook signatures.
- Do not commit `wa_credentials/`, screenshots, logs, virtual environments, or `node_modules/`.
- Use HTTPS in production and run Flask with `FLASK_ENV=production`.
- Back up PostgreSQL before running schema-changing bot migrations in production.

## GitHub Publishing Notes

This repository includes a `publish/` bundle when prepared for upload. That folder should contain only safe project files:

- Source code.
- Templates and static assets.
- README and diagrams.
- `.env.example`.
- `.gitignore`.

It should not contain:

- `.env`
- `.git/`
- `.venv/`
- `__pycache__/`
- `node_modules/`
- WhatsApp credential folders
- payment screenshots
- logs

## Common Workflows

### Add or edit services

Owners use the dashboard or onboarding flow to add service names, prices, and duration. The bot reads those services from PostgreSQL when answering customers.

### Connect WhatsApp

The dashboard requests a bot assignment, shows a QR code, and stores the WhatsApp session status on the user record. The owner scans the QR from WhatsApp Linked Devices.

### Collect advance payment

When advance payment is enabled, the bot reserves the slot, generates a branded UPI QR code, waits for a screenshot, verifies it, and marks the booking as paid or requiring review.

### Upgrade a plan

The billing page creates a Razorpay order or subscription. After signature verification, the user's plan and subscription are updated, and a bot instance can be assigned.

## Operational Notes

- The bot uses PostgreSQL JSON fields for WhatsApp appointments and chat history.
- The bot creates several `wa_*` columns on first boot if they do not exist.
- Rate limiting and IP blocking exist in Flask, but production should use a shared backend such as Redis instead of in-memory limits.
- The manager API must not be exposed without firewall rules and a strong `BOT_API_KEY`.
- Baileys links a normal WhatsApp session. Respect WhatsApp's policies and avoid unsolicited messaging.

## License

No license file is included yet. Add the license you want before publishing publicly. Without a license, others do not automatically receive permission to copy, modify, or reuse the code.
