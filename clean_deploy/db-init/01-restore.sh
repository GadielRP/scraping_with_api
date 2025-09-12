#!/bin/bash
set -e
echo "[init] Restoring backup.dump into $POSTGRES_DB as $POSTGRES_USER"
pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --no-owner /docker-entrypoint-initdb.d/backup.dump