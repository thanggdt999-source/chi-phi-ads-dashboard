# Ads Report Viewer

Production-ready internal tool for viewing Google Sheets-based ads reports.

## Stack

- **Next.js 14** (App Router) + TypeScript
- **TailwindCSS** — minimal, clean UI
- **PostgreSQL** + **Prisma ORM**
- **JWT auth** (httpOnly cookie, 10-min rolling session)
- **bcrypt** password hashing
- **Google Sheets API** — parse "Chi phí ADS" sheet data
- **Telegram Bot API** — daily + realtime notifications
- **node-cron** — scheduled jobs (daily 07:00, every 10 min)

## Roles

| Role    | Access                  |
|---------|-------------------------|
| viewer  | Own reports only        |
| leader  | Own team reports        |
| admin   | All users, all data     |

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Copy env and fill in values
cp .env.example .env.local

# 3. Create DB tables
npm run db:push

# 4. Seed default users
npm run db:seed

# 5. Start dev server
npm run dev
```

## Default Users (after seed)

| Username    | Password         | Role   |
|-------------|------------------|--------|
| admin_root  | Admin@Hexi2026!  | admin  |
| lead_team_1 | LeadTeam1@2026   | leader |
| emp_thang   | Emp@123456       | viewer |

## Environment Variables

See `.env.example` for all required variables.

Key vars:
- `DATABASE_URL` — PostgreSQL connection string
- `JWT_SECRET` — min 32-char random string
- `GOOGLE_SERVICE_ACCOUNT_JSON` — full service account JSON (one line)
- `TELEGRAM_BOT_TOKEN` — Telegram bot token
- `CRON_SECRET` — protects `/api/cron/update`

## Cron Jobs

The app uses `node-cron` for local/VPS deployments.  
For Vercel, call `/api/cron/update` via Vercel Cron Jobs with `Authorization: Bearer <CRON_SECRET>`.

| Schedule      | Action                                  |
|---------------|-----------------------------------------|
| Daily 07:00   | Update yesterday's costs + notify TG   |
| Every 10 min  | Update today's costs (no notification) |

## Deploy

**Vercel:**
```
npm run build
vercel deploy
```

**VPS / Render:**
```
npm run build
npm start
```
