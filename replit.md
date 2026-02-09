# BLW Discord Bot

## Overview

This is a Discord bot project for community engagement, featuring an economy system, quizzes, a virtual shop, leaderboards, marriage system, and various interactive features. The application uses a hybrid architecture with a Node.js/Express web server that spawns a Python Discord bot process. The web frontend exists but is minimal, primarily serving as a status page while the main functionality runs through Discord.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Hybrid Runtime Architecture
- **Web Server**: Express.js (Node.js) runs on port 5000, serving a simple status page
- **Discord Bot**: Python process spawned by the Express server using `child_process.spawn`
- **Rationale**: Replit requires a web server to keep the application alive; the Express server fulfills this while delegating bot functionality to Python

### Frontend Stack
- **Framework**: React 18 with TypeScript
- **Build Tool**: Vite with custom build script
- **Styling**: Tailwind CSS with shadcn/ui component library (New York style)
- **State Management**: TanStack React Query for server state
- **Routing**: Wouter (lightweight router)
- **Path Aliases**: `@/` maps to `client/src/`, `@shared/` maps to `shared/`

### Backend Stack
- **Web Server**: Express 5 (TypeScript)
- **Bot Runtime**: Python 3 with discord.py
- **Database ORM**: Drizzle ORM (TypeScript) for schema definition
- **Database**: PostgreSQL (required, configured via `DATABASE_URL` environment variable)
- **Python Database**: asyncpg for async PostgreSQL access

### Discord Bot Architecture (Python)
- **Framework**: discord.py with commands extension
- **Structure**: Cog-based modular design in `bot/cogs/`
- **Features**:
  - Economy system (coins, XP, levels)
  - Quiz system with timed questions
  - Virtual shop with purchase flow
  - Leaderboard auto-updates
  - Marriage system
  - Admin commands (owner-only)
  - Random bunny spawn events

### Database Schema
- Users table with id, username, password (defined in `shared/schema.ts`)
- Additional tables managed by Python bot for Discord-specific data (coins, XP, marriages, shop items, questions)

### Build System
- **Development**: `npm run dev` runs tsx to execute TypeScript directly
- **Production Build**: Custom esbuild script bundles server with Vite for client
- **Database Migrations**: Drizzle Kit with `npm run db:push`

## External Dependencies

### Required Environment Variables
- `DATABASE_URL`: PostgreSQL connection string (required)
- `DISCORD_TOKEN`: Discord bot token (implied, used by Python bot)
- `OWNER_ID`: Discord user ID for admin commands
- `GUILD_ID`: Discord server ID for slash command sync (optional)
- `QUIZ_CHANNEL_ID`: Channel for quiz and economy features
- `LEADERBOARD_CHANNEL_ID`: Channel for auto-updating leaderboard

### Third-Party Services
- **Discord API**: Primary interaction platform via discord.py
- **PostgreSQL**: Primary data storage for both web app and bot

### Key npm Dependencies
- Express 5, Drizzle ORM, React 18, Vite, TanStack Query
- Full shadcn/ui component library with Radix UI primitives

### Key Python Dependencies
- discord.py with ext.commands and ext.tasks
- asyncpg for database connections
- nest_asyncio for event loop compatibility