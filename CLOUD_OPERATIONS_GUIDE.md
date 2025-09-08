## SofaScore Odds System – Cloud Ops Guide (Beginner Friendly)

This is a practical, copy‑paste friendly guide with the exact commands you’ll use to run and maintain the app in the cloud (DigitalOcean). It assumes:
- You have a Droplet (Ubuntu 22.04, 1GB RAM) and can SSH as `root@143.244.179.129`.
- Your project folder on the server is `/opt/sofascore`.
- You use Windows PowerShell on your PC.

Where to run commands:
- On your PC: the prompt starts with `PS C:\...>`
- On the server (SSH): the prompt starts with `root@sofascore-prod:~#` (or similar)

---

### 1) Create or reuse an SSH key (on your PC)

Check if you already have a key:
```powershell
dir ~/.ssh
```

If you see `id_rsa` and `id_rsa.pub`, you already have a key. Copy your public key to clipboard:
```powershell
Get-Content ~/.ssh/id_rsa.pub | clip
```

If you don’t have one yet, create it:
```powershell
ssh-keygen -t ed25519 -C "your-email@example.com"
# Press Enter for defaults, passphrase optional
Get-Content ~/.ssh/id_ed25519.pub | clip
```

Add the public key in DigitalOcean → Settings → Security → Add SSH Key.

---

### 2) Connect to the server (from your PC)
```powershell
ssh -i ~/.ssh/id_rsa root@143.244.179.129
```

Optional keepalive (prevents idle disconnects):
```powershell
ssh -i ~/.ssh/id_rsa -o ServerAliveInterval=30 root@143.244.179.129
```

---

### 3) Install Docker + Compose (on the server)
```bash
apt-get update -y && apt-get upgrade -y
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker
docker --version
docker compose version
```

Create the project directory:
```bash
mkdir -p /opt/sofascore
cd /opt/sofascore
```

---

### 4) Make a fresh backup locally and upload the project (on your PC)

From your local project folder `C:\Users\...\projects\sofascore`:
```powershell
# Create a fresh dump from your local Postgres container
docker exec -t sofascore-pg pg_dump -U sofascore -d sofascore_odds -F c -f /tmp/latest_backup.dump
docker cp sofascore-pg:/tmp/latest_backup.dump .\latest_backup.dump
Copy-Item .\latest_backup.dump .\backup.dump -Force

# Upload the entire project to the server
scp -r . root@143.244.179.129:/opt/sofascore
```

What this does:
- `backup.dump` will be auto‑restored on the server the first time Postgres starts (because we mount `db-init/` in the container).

---

### 5) First start on the server (creates persistent DB volume and restores backup)
```bash
docker volume create sofascore_pgdata
cd /opt/sofascore
docker compose up -d
docker logs -f sofascore-pg
```

You should see Postgres initialize and run `01-restore.sh`. DROP errors about missing tables are normal during `--clean`. After it settles:
```bash
docker exec -it sofascore-pg psql -U sofascore -d sofascore_odds -c "\dt"
docker exec -it sofascore-pg psql -U sofascore -d sofascore_odds -c "SELECT COUNT(*) FROM events;"
docker exec -it sofascore-pg psql -U sofascore -d sofascore_odds -c "SELECT COUNT(*) FROM odds_snapshot;"
```

Check the app logs:
```bash
docker logs --tail 100 sofascore_app
```

If you see `PermissionError` for `/app/logs`, fix host folder permissions:
```bash
cd /opt/sofascore
mkdir -p logs
chown -R 1000:1000 logs     # user "app" inside the container
docker compose up -d
```

---

### 6) Timezone sanity checks (server should use Mexico City time)
```bash
docker exec -it sofascore_app date
docker exec -it sofascore-pg date
```

If both show local time (CST/CDT), you’re good. We set `TZ=America/Mexico_City` in `docker-compose.yml` for both services.

---

### 7) View the database with a GUI (from your PC)

Option A – Direct connection (quick):
1. Install DBeaver (or pgAdmin)
2. Connect with:
   - Host: 143.244.179.129
   - Port: 5432
   - Database: `sofascore_odds`
   - User: `sofascore`
   - Password: `Sofa12345`

Secure it with a firewall rule so only your home IP can access 5432 (on the server):
```bash
apt-get install -y ufw
ufw allow OpenSSH
ufw allow from YOUR_HOME_IP to any port 5432 proto tcp
ufw enable
ufw status numbered
```
Change your rule later (home IP changed):
```bash
ufw status numbered
ufw delete <RULE_NUMBER>
ufw allow from NEW_HOME_IP to any port 5432 proto tcp
```

Option B – SSH tunnel (safer, no public 5432):
```bash
# On the server: block 5432 publicly
ufw deny 5432
```
On your PC, open a tunnel and keep the window open:
```powershell
ssh -i ~/.ssh/id_rsa -L 5433:localhost:5432 root@143.244.179.129
```
In DBeaver/pgAdmin connect to:
- Host: `localhost`
- Port: `5433`
- Database/User/Password: as above

Note: Proxies used by the app are outbound connections; UFW allows outbound traffic by default. Your rotating Oxylabs proxies are unaffected.

---

### 8) Daily operations (most used commands)

Start/stop/recreate:
```bash
cd /opt/sofascore
docker compose up -d           # start
docker compose down            # stop (keeps data)
docker compose down -v         # stop AND delete data volume (be careful)
```

Logs:
```bash
docker logs -f sofascore_app
docker logs -f sofascore-pg
tail -f /opt/sofascore/logs/sofascore_odds.log
```

Health/status:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
docker exec -it sofascore-pg psql -U sofascore -d sofascore_odds -c "SELECT 1;"
df -h   # disk space
free -h # memory
```

Backups (on the server):
```bash
# Create a dump
docker exec -t sofascore-pg pg_dump -U sofascore -d sofascore_odds -F c -f /tmp/backup_$(date +%F).dump

# Copy to your PC (run on your PC)
scp root@143.244.179.129:/tmp/backup_YYYY-MM-DD.dump .
```

Restore manually (if needed):
```bash
docker exec -it sofascore-pg bash -lc \
  "pg_restore -U sofascore -d sofascore_odds --clean --if-exists --no-owner --no-privileges /docker-entrypoint-initdb.d/backup.dump"
```

Re‑trigger first‑run restore (wipe and restore from `backup.dump` again):
```bash
cd /opt/sofascore
docker compose down -v
docker volume create sofascore_pgdata
docker compose up -d
```

---

### 9) Updating the server with new code

Fastest way (re‑upload from your PC):
```powershell
scp -r . root@143.244.179.129:/opt/sofascore
```
Then on the server:
```bash
cd /opt/sofascore
docker compose up -d --build
```

Tip: You can also use Git (push locally, then `git pull` on the server) or `rsync --exclude` to avoid copying cache/logs.

---

### 10) Cleaning server folder (optional)

If you accidentally uploaded large local files (logs, SQLite, caches), you can clean them safely on the server:
```bash
cd /opt/sofascore
find . -type d -name '__pycache__' -prune -exec rm -rf {} +
find . -name '*.pyc' -delete
rm -rf .git .gitignore logs *.log data csv exports old \
       debug_discovery_*.json dropping_odds.json ejemplo.json *.ipynb \
       deploy_direct.sh *.service sofascore_odds.db *.db
```

Note: Do NOT delete `db-init/` or `backup.dump` unless you intend to change restore behavior.

---

### 11) Security notes

- Limiting 5432 to your IP (UFW) is good for quick access. If your IP changes often, use SSH tunnel (Option B) and keep 5432 blocked.
- Your database lives in the named volume `sofascore_pgdata`, which persists across `docker compose down`/`up`. Only `down -v` deletes it.
- Logs from the app are persisted on the host under `/opt/sofascore/logs`.

---

### 12) Quick troubleshooting

1) App can’t write logs (`PermissionError`):
```bash
cd /opt/sofascore
mkdir -p logs
chown -R 1000:1000 logs
docker compose up -d
```

2) App can’t connect to DB from compose:
```bash
# Ensure both containers are on the same default network and DATABASE_URL uses host "sofascore-pg"
docker ps --format "table {{.Names}}\t{{.Networks}}"
```

3) Health check failures:
```bash
docker logs sofascore_app
docker logs sofascore-pg
```

4) Verify timezone alignment:
```bash
docker exec -it sofascore_app date
docker exec -it sofascore-pg date
```

5) Free disk space or memory:
```bash
df -h
free -h
```

---

### 13) Command recap (cheat sheet)

On your PC (PowerShell):
```powershell
ssh -i ~/.ssh/id_rsa root@143.244.179.129
scp -r . root@143.244.179.129:/opt/sofascore
ssh -i ~/.ssh/id_rsa -L 5433:localhost:5432 root@143.244.179.129
```

On the server (SSH):
```bash
docker compose up -d
docker compose down
docker logs -f sofascore_app
docker exec -it sofascore-pg psql -U sofascore -d sofascore_odds -c "SELECT 1;"
ufw allow from YOUR_HOME_IP to any port 5432 proto tcp
ufw deny 5432
```

You’re set. Keep this file handy; it covers the most common operations you’ll need.

### 13) main.py cli commands
```
# Discovery (Job A) now
docker exec -it sofascore_app python /app/main.py discovery

# Pre-start check (Job C) now
docker exec -it sofascore_app python /app/main.py pre-start

# Midnight results sync now
docker exec -it sofascore_app python /app/main.py midnight

# Collect results (finished events) now
docker exec -it sofascore_app python /app/main.py results

# Collect results (all finished) now
docker exec -it sofascore_app python /app/main.py results-all

# Final odds collector (batch)
docker exec -it sofascore_app python /app/main.py final-odds-all

# Show status and scheduled jobs
docker exec -it sofascore_app python /app/main.py status

# Show recent events (override limit)
docker exec -it sofascore_app python /app/main.py events --limit 20
```

---

### 14) Refresh your LOCAL Docker database from the latest SERVER backup

Goal: overwrite your LOCAL Postgres (Docker, port 5435) with the newest dump from the server, so you can test locally with current data.

Warning: this replaces local data in `sofascore_odds`.

Steps (run on your PC, PowerShell, in `C:\Users\...\projects\sofascore`):

1) Ensure local Postgres is running on port 5435
```powershell
$env:POSTGRES_HOST_PORT = "5435"
docker compose up -d postgres
docker compose exec postgres pg_isready -U sofascore -d sofascore_odds
```

2) Download the latest backup from the server to your PC
```powershell
# Create a fresh backup INSIDE the container, then copy it to the SERVER host
ssh -i ~/.ssh/id_rsa root@143.244.179.129 "docker exec sofascore-pg bash -lc 'pg_dump -U sofascore -d sofascore_odds -F c -f /tmp/latest_backup.dump && gzip -f /tmp/latest_backup.dump' && docker cp sofascore-pg:/tmp/latest_backup.dump.gz /tmp/latest_backup.dump.gz && ls -lh /tmp/latest_backup.dump.gz"

# Pull the compressed backup from the SERVER to your PC
scp root@143.244.179.129:/tmp/latest_backup.dump.gz .\
```

3) Reset your LOCAL database (safe)
```powershell
# Copy/rename into db-init for convenience (optional)
Copy-Item .\latest_backup.dump.gz .\db-init\local_backup.dump.gz -Force

# Drop and recreate the local DB owned by 'sofascore' (avoids view/table dependency errors)
docker compose exec postgres psql -U sofascore -d template1 -v ON_ERROR_STOP=1 `
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='sofascore_odds' AND pid <> pg_backend_pid();" `
  -c "DROP DATABASE IF EXISTS sofascore_odds;" `
  -c "CREATE DATABASE sofascore_odds OWNER sofascore;"

# Alternative (if you prefer not to drop the DB): drop views first
# docker compose exec postgres psql -U sofascore -d sofascore_odds -c "DROP VIEW IF EXISTS event_up_odds, event_down_odds, event_flat_odds;"

```

4) Restore that backup into LOCAL Docker
```powershell
docker compose exec postgres bash -lc "gunzip -c /docker-entrypoint-initdb.d/local_backup.dump.gz | pg_restore -U sofascore -d sofascore_odds --clean --if-exists --no-owner --no-privileges"
```

5) Verify locally (port 5435)
```powershell
docker compose exec postgres psql -U sofascore -d sofascore_odds -c "SELECT COUNT(*) FROM events;"
docker compose exec postgres psql -U sofascore -d sofascore_odds -c "SELECT COUNT(*) FROM odds_snapshot;"
```

Notes:
- Your pgAdmin/DBeaver connection to LOCAL (127.0.0.1:5435) does not change; just click refresh to see the new data.
- If a local-only marker table exists from earlier tests, it may be dropped by `--clean`. Recreate it if you still want the marker.
- To start from an empty local DB and restore on first run, you can wipe the local volume and place a `backup.dump` in `db-init/`, then `docker compose up -d` (this rebuilds local data volume).
