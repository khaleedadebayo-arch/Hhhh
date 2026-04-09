# Kraven KOL Campaign Manager Bot

A full-featured Telegram bot for managing KOL (Key Opinion Leader) link-drop campaigns, queue enforcement, auto-moderation, and campaign tracking — built for Web3 communities.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Setup & Deployment](#setup--deployment)
- [Environment Variables](#environment-variables)
- [Command Reference](#command-reference)
- [Super Admin System](#super-admin-system)
- [Subscription & Payment System](#subscription--payment-system)
- [Database Schema](#database-schema)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Kraven KOL Campaign Manager is a Telegram bot designed to run structured link-drop sessions in groups and topic-based supergroups. It enforces queue fairness, tracks campaign submissions, auto-moderates rule violations, and gives admins full control over sessions, campaigns, and payouts.

Built on **Python**, **python-telegram-bot v21**, and **PostgreSQL**.

---

## Features

### Session Management
- Start and stop link-drop sessions with configurable queue sizes (15 or 28 by default)
- Queue enforcement — users must wait for N unique posters before reposting
- Whitelisted users bypass the queue
- Automatic duplicate link detection
- Twitter/X tracking parameter stripping — all `?s=&t=` params removed before storing
- Anonymous `/i/` links automatically replaced with the poster's username

### Campaign Tracking
- Create named campaigns with targets, rewards, and deadlines
- Track submission progress with a visual progress bar
- Per-user campaign stats and rankings
- Export all submissions as a `.txt` file
- Verify and remove individual submissions
- Log and view payout records

### Auto-Moderation
- Warning system with escalating penalties:
  - Warning 1–2: Notice only
  - Warning 3: 24-hour mute
  - Warning 4: 72-hour mute
  - Warning 5: Permanent ban
- Ban, unban, and unmute commands
- Message deletion for rule violations

### Admin Controls
- Per-topic command permissions — enable or disable user commands per topic thread
- Whitelist users for queue exemption
- Reset individual user stats
- Tag all tracked members

### Super Admin (Kraven Network)
- Broadcast sponsored messages to all known groups (main chat only, never topics)
- View broadcast history and group reach stats
- Dynamically add and remove super admins
- View all groups the bot is currently active in
- Super admin commands restricted to private chat only

### Group Awareness
- Automatically detects when the bot is added to or removed from a group
- Tracks known groups for broadcast targeting
- Supports groups, supergroups, and topic-based chats

### Private Menu
- Interactive inline button menu in private chat
- Super admin panel hidden from non-super-admins with hard enforcement

---

## Requirements

- Python 3.11+
- PostgreSQL database
- Telegram Bot Token (from [@BotFather](https://t.me/BotFather))

Python dependencies:
```
python-telegram-bot==21.6
python-dotenv==1.0.1
psycopg2-binary
```

---

## Setup & Deployment

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/kraven-bot-railway.git
cd kraven-bot-railway
```

### 2. Create a Telegram bot

- Message [@BotFather](https://t.me/BotFather) on Telegram
- Use `/newbot` and follow the prompts
- Copy the bot token

### 3. Set up PostgreSQL

You can use any PostgreSQL provider. [Railway](https://railway.app) is recommended:
- Create a new Railway project
- Add a **PostgreSQL** database service
- Copy the `DATABASE_URL` from the database's Variables tab

### 4. Configure environment variables

Create a `.env` file locally (never commit this):

```env
BOT_TOKEN=your_telegram_bot_token
DATABASE_URL=postgresql://user:password@host:5432/dbname
KRAVEN_CHANNEL=https://t.me/yourchannel
DEFAULT_QUEUE_SIZE=15
```

### 5. Deploy to Railway

- Push your code to GitHub
- Connect your Railway project to the GitHub repo
- Add the environment variables in Railway → your service → **Variables** tab
- Railway will auto-deploy on every push

### 6. First boot

On first startup, `init_db()` runs automatically and creates all required tables. No manual migration needed.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `BOT_TOKEN` | ✅ | Telegram bot token from BotFather |
| `DATABASE_URL` | ✅ | PostgreSQL connection string |
| `KRAVEN_CHANNEL` | ✅ | Your Telegram channel URL (shown in footer and menu) |
| `DEFAULT_QUEUE_SIZE` | ❌ | Default queue size per session (default: `15`) |

---

## Command Reference

### User Commands
> These are disabled by default. Admins must enable them per topic using `/enablecmd`.

| Command | Description |
|---|---|
| `/mystatus` | Your current queue position and stats |
| `/leaderboard` | Top 10 posters by link count |
| `/campaignstatus` | Active campaign progress and top contributors |
| `/mycampaignstats` | Your personal submissions and rank in the active campaign |
| `/stats` | Group-wide stats including session status |

---

### Admin Commands
> Require administrator or creator status in the group.

#### Sessions
| Command | Description |
|---|---|
| `/startsession` | Start a 15-link queue session |
| `/startsession15` | Start a 15-link queue session |
| `/startsession28` | Start a 28-link queue session |
| `/stopsession` | Stop the current session |
| `/setqueue [n]` | Set a custom queue size |
| `/setpoints [n]` | Set points awarded per valid link drop |

#### User Management
| Command | Description |
|---|---|
| `/warn @user` | Issue a warning (escalates to mute then ban) |
| `/ban @user` | Immediately ban a user |
| `/unban @user` | Unban a user and clear their warnings |
| `/unmute @user` | Remove a mute early |
| `/reset @user` | Reset a user's stats, warnings, and queue position |
| `/whitelist @user` | Toggle queue exemption for a user |
| `/tagall [message]` | Mention all tracked members in the group |

#### Command Permissions
| Command | Description |
|---|---|
| `/enablecmd [command]` | Enable a user command in the current topic |
| `/disablecmd [command]` | Disable a user command in the current topic |
| `/cmdstatus` | View which user commands are on or off |

#### Campaigns
| Command | Description |
|---|---|
| `/newcampaign Name \| Desc \| Target \| Reward \| Deadline` | Launch a new campaign |
| `/endcampaign` | End the active campaign |
| `/exportlinks` | Export all campaign submissions as a `.txt` file |
| `/verifysub @user` | Mark a user's submissions as verified |
| `/removesub @user [partial link]` | Remove a specific submission |
| `/logpayout @user [amount] [reason]` | Log a payout record |
| `/payouts` | View the 20 most recent payout records |

---

### Super Admin Commands
> Restricted to authorised super admins. Must be used in **private chat** only.

| Command | Description |
|---|---|
| `/broadcast [message]` | Send a sponsored message to all known groups |
| `/broadcaststats` | View broadcast history and total group reach |
| `/addadmin [user_id]` | Grant super admin access to a Telegram user ID |
| `/removeadmin [user_id]` | Revoke super admin access |
| `/listadmins` | List all current super admins |

Group stats are also accessible via the **Super Admin** button in the private menu.

---

## Super Admin System

The bot has two tiers of elevated access:

**Founders** — hardcoded user IDs set directly in the source code. These cannot be removed via any command and have permanent super admin access.

**Dynamic super admins** — added and removed via `/addadmin` and `/removeadmin`. Stored in the `super_admins` database table.

To set up founders, update `FOUNDER_IDS` in `bot.py` with your Telegram user IDs before deploying. You can find your Telegram user ID by messaging [@userinfobot](https://t.me/userinfobot).

The Super Admin inline panel in private chat is only visible to super admins. Access is enforced server-side — constructing callback data manually will not bypass it.

---

## Subscription & Payment System

> 🚧 **Coming soon**

A crypto payment system is planned that will allow groups or individual users to subscribe to the bot as a paid service. Planned features:

- Per-group subscription tiers
- On-chain payment confirmation (TON / SOL)
- Automatic access granting and expiry
- Payment history and subscription status tracking

This section will be updated when the feature is released.

---

## Database Schema

The bot uses the following PostgreSQL tables:

| Table | Description |
|---|---|
| `users` | Per-user stats per group (warnings, points, links, ban status) |
| `link_queue` | Every accepted link drop per session |
| `topic_settings` | Queue size, session state, and points per topic |
| `campaigns` | Campaign definitions per group/topic |
| `campaign_submissions` | Individual link submissions per campaign |
| `rewards` | Payout log records |
| `cmd_permissions` | Per-topic user command enable/disable flags |
| `super_admins` | Dynamically added super admin user IDs |
| `known_groups` | Groups the bot is currently active in |
| `broadcast_log` | History of all broadcasts sent |

All tables are created automatically on first boot via `init_db()`.

---

## Contributing

Pull requests are welcome. For major changes please open an issue first to discuss what you'd like to change.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

Please make sure your changes do not break existing command behaviour and that all SQL queries use parameterised statements.

---

## License

MIT License

Copyright (c) 2026 Kraven KOL Network

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
