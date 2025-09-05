## SofaScore Odds System – Cloud Ops Guide (Beginner Friendly)

This is a practical, copy‑paste friendly guide with the exact commands you’ll use to run and maintain the app in the cloud (DigitalOcean). It assumes:
- You have a Droplet (Ubuntu 22.04, 1GB RAM) and can SSH as `root@YOUR_SERVER_IP`.
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
scp -r . root@YOUR_SERVER_IP:/opt/sofascore
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
   - Host: YOUR_SERVER_IP
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
ssh -i ~/.ssh/id_rsa -L 5433:localhost:5432 root@YOUR_SERVER_IP
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
scp root@YOUR_SERVER_IP:/tmp/backup_YYYY-MM-DD.dump .
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
scp -r . root@YOUR_SERVER_IP:/opt/sofascore
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
ssh -i ~/.ssh/id_rsa root@YOUR_SERVER_IP
scp -r . root@YOUR_SERVER_IP:/opt/sofascore
ssh -i ~/.ssh/id_rsa -L 5433:localhost:5432 root@YOUR_SERVER_IP
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

### 14) Weekly Backups: create on the server and download to your PC (Beginner friendly)

We’ll keep a copy of your PostgreSQL database outside the server so you’re safe against server loss, mistakes, or corruption. You’ll use two small scripts that are already in this repo:

- `scripts/backup_server.py` (runs on the server): creates a compressed `.dump.gz` with `pg_dump` inside the Postgres Docker container and deletes backups older than N days.
- `scripts/pull_backup_windows.py` (runs on your PC): tells the server to create a fresh backup and then downloads the newest backup to your PC via SSH/SCP.

Where to run commands is explicitly stated before each block.

#### 14.1 One-time preparation

On your PC (PowerShell, any folder):
```powershell
# Your user home folder path
echo $env:USERPROFILE

# Ensure local backups folder exists
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\Documents\sofascore\backups"

# Ensure you have an SSH key (ED25519 recommended). If you already have a key, skip this.
ssh-keygen -t ed25519 -C "sofascore" -f "$env:USERPROFILE\.ssh\id_ed25519"
```

Install your public key on the server so you can log in without a password:
```powershell
# Replace YOUR_SERVER_IP
type "$env:USERPROFILE\.ssh\id_ed25519.pub" | ssh root@YOUR_SERVER_IP "mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"

# Test login using the key
ssh -i "$env:USERPROFILE\.ssh\id_ed25519" root@YOUR_SERVER_IP hostname
```

Upload the scripts to the server.
On your PC (PowerShell, in your local project folder, e.g. C:\Users\...\projects\sofascore):
```powershell
scp -i "$env:USERPROFILE\.ssh\id_ed25519" -r .\scripts root@YOUR_SERVER_IP:/opt/sofascore/
```

On the server (SSH as root@YOUR_SERVER_IP, any folder):
```bash
mkdir -p /opt/sofascore/backups
ls -l /opt/sofascore/scripts/backup_server.py
```

#### 14.2 Create a backup now (test)

On the server (SSH, any folder, e.g. /root):
```bash
python3 /opt/sofascore/scripts/backup_server.py \
  --container sofascore-pg \
  --db-name sofascore_odds \
  --db-user sofascore \
  --output-dir /opt/sofascore/backups \
  --retention-days 14

ls -lh /opt/sofascore/backups
```
You should see a file like `sofascore_odds_YYYYMMDD_HHMMSS.dump.gz`.

Tip (manual, without the script):
```bash
docker exec sofascore-pg pg_dump -U sofascore -d sofascore_odds -Fc | gzip -9 > /opt/sofascore/backups/sofascore_odds_$(date +%Y%m%d_%H%M%S).dump.gz
```

#### 14.3 Download the latest backup to your PC (manual fallback)

On your PC (PowerShell, in your local project folder):
```powershell
$Key       = "$env:USERPROFILE\.ssh\id_ed25519"   # or id_rsa if you use RSA
$Server    = "YOUR_SERVER_IP"
$RemoteDir = "/opt/sofascore/backups"
$LocalDir  = "$env:USERPROFILE\Documents\sofascore\backups"

# Ask the server which file is the newest
$Latest = ssh -i $Key "root@$Server" "ls -1t $RemoteDir/*.dump.gz 2>/dev/null | head -n 1"
if ([string]::IsNullOrWhiteSpace($Latest)) { throw 'No backups found on server.' }

# Download it (note ${Server} to avoid PowerShell parsing issues)
scp -i $Key "root@${Server}:$Latest" "$LocalDir"

# Verify locally
Get-Item (Join-Path $LocalDir (Split-Path $Latest -Leaf))
```

If your key has a passphrase and you don’t want to type it every time, use the SSH agent on your PC:
```powershell
Get-Service ssh-agent | Set-Service -StartupType Automatic
Start-Service ssh-agent
ssh-add "$env:USERPROFILE\.ssh\id_ed25519"
```

#### 14.4 Automate weekly on your PC (recommended)

Use the provided Windows script to trigger a fresh backup and download it automatically.

On your PC (PowerShell, in your local project folder):
```powershell
python .\scripts\pull_backup_windows.py \
  --server-ip YOUR_SERVER_IP \
  --server-user root \
  --ssh-key "$env:USERPROFILE\.ssh\id_ed25519" \
  --project-dir /opt/sofascore \
  --remote-backup-dir /opt/sofascore/backups \
  --local-backup-dir "$env:USERPROFILE\Documents\sofascore\backups" \
  --container sofascore-pg \
  --db-name sofascore_odds \
  --db-user sofascore \
  --retention-days 30
```

Schedule weekly with Windows Task Scheduler:
1) Create Basic Task → Name: “Pull backup sofascore”.
2) Trigger: Weekly, pick day/time when your PC is on.
3) Action: Start a program.
   - Program: `python`
   - Arguments: the exact arguments shown above.

Optional: also schedule a weekly server-side backup (redundancy) with cron.
On the server (SSH):
```bash
(crontab -l 2>/dev/null; echo "30 3 * * 0 python3 /opt/sofascore/scripts/backup_server.py --container sofascore-pg --db-name sofascore_odds --db-user sofascore --output-dir /opt/sofascore/backups --retention-days 14 >> /opt/sofascore/backup_cron.log 2>&1") | crontab -
```

#### 14.5 Restore from a backup (when needed)

On the server (SSH):
```bash
gunzip -c /path/to/backup.dump.gz | pg_restore -U sofascore -d sofascore_odds --clean --if-exists --no-owner
```

Notes:
- `--clean --if-exists` drops objects before re-creating them (safe for restoring over an existing DB).
- If you changed the DB name or user, adjust `-d` and `-U` accordingly.

#### 14.6 Troubleshooting (quick)

- PowerShell error around `root@$Server:` → use `${Server}` in the SCP string: `"root@${Server}:$Latest"`.
- “Permission denied (publickey)” → ensure your public key is in `~/.ssh/authorized_keys` on the server and that you use the correct private key path with `-i`.
- Key passphrase prompts → use the SSH agent (`ssh-add`) or create a dedicated key without passphrase only for this task.
- `python` not found on Windows → use `py -3` instead of `python`.
- Container name different from `sofascore-pg` → change `--container` accordingly.
- No backups listed → run the server script first to create one, then retry download.
