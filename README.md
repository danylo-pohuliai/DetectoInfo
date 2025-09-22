<<<<<<< HEAD
# DetectoInfo
=======
# Telegram Community & Marketplace Bot (for polygraph professionals and HR's)

**Short summary**  
A production-grade Telegram bot that provides a classifieds / marketplace + community experience for polygraph professionals: posting/searching ads, applying for jobs/services, ratings & reviews, subscriptions to authors/categories, and a personal cabinet with statistics.

**Bot:** @DetectoInfo_bot  
**Last update:** 03.08.2025

---

## What it does
- Browse categorized ads with pagination and quick navigation.  
- Post new ads with city, price, description and optional photo; interactive multi-step flow (ConversationHandler).  
- Apply to ads; owners receive accept/reject buttons and can exchange contacts via Telegram deep links.  
- Leave and read reviews & ratings; average rating shown on author profile.  
- Subscribe to categories or particular authors to receive updates.  
- Personal dashboard: shows user stats (ads count, reviews, avg rating) and quick access to own ads, reviews, subscriptions.

---

## Key technical components
- **Language & libraries:** Python (async) with `python-telegram-bot`, `psycopg2` for PostgreSQL, `python-dotenv` for configuration.  
- **Database:** PostgreSQL for persistent data (users, ads, reviews, applications, subscriptions).  
- **Deployment:** Containerized with Docker / docker-compose. Designed to run on a VPS with system services for reliability.  
- **Hosting:** Deployed to a VPS and configured as a long-running service (webhook / workers).

---

## Data model
- `users` — Telegram user id, username, display name, quotas, timestamps.  
- `ads` — records with `user_id`, `city`, `price`, `description`, `photo_id`, `category`, `created_at`.  
- `applications` — user applications to ads, with `requester_id`, `executor_id`, `status`, and timestamps.  
- `reviews` — ratings and comments tied to users and ads.  
- `user_subscriptions` and `category_subscriptions` — for push/notification subscriptions.

---

## Behaviour & implementation notes
- UI uses inline keyboards and callback_data; many action routes encoded into callback payloads and decoded in handlers. Deep links are created and parsed for direct actions.  
- Pagination is implemented server-side with a helper that slices lists and builds navigation keyboards.  
- The bot performs explicit SQL queries via `psycopg2` rather than an ORM; this keeps DB interaction straightforward but benefits from connection pooling and a thin repository layer.

---

## DevOps & VPS / operational capabilities
This project is designed and maintained with production deployment in mind. Operational capabilities include:

- **Docker & docker-compose** for reproducible deployments and dependency isolation.  
- **VPS deployment:** experience provisioning and operating Ubuntu/Debian-based VPS instances.  
- **Reverse proxy & TLS:** Nginx as SSL terminator and request routing; automated SSL provisioning via Certbot.  
- **Persistent storage & backups:** PostgreSQL volumes with scheduled backups (cron / automated dump & remote storage).  
- **Logging & rotation:** centralized app logs (stdout → Docker logs / file) and logrotate policies for disk control.  
- **Security hardening:** firewall rules (ufw), secret management, limited service user accounts, and resource limits for containers.  
- **Scaling & reliability:** ability to run multiple worker processes, queue-based background jobs and horizontal scaling patterns for heavy workloads.

---

## Suggested improvements / roadmap
1. Connection pooling and a repository layer for DB queries.  
2. Background workers (Redis + RQ/Celery) for heavier tasks like media processing, report generation and notification dispatching.  
3. Rate-limiting and abuse protection (per-user limits, captchas for heavy actions).  
4. Admin web UI for managing ads, viewing logs, and moderating content.  
5. Improved image handling and short public links for ad sharing.  
6. Metrics & alerting: instrument key flows (requests/sec, DB latency, failed jobs).  
7. Automated backups with retention policies and restore tests.  
8. CI pipeline with test stages and deploy steps.
>>>>>>> 724794c (Initial commit)
