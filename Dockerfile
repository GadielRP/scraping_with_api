# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements_cloud.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements_cloud.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app
USER app

# Expose port for health checks
EXPOSE 8000

# Health check: open a DB connection based on DATABASE_URL (defaults to SQLite)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import os,sqlalchemy as sa; u=os.getenv('DATABASE_URL','sqlite:////app/data/sofascore_odds.db'); e=sa.create_engine(u); c=e.connect(); c.close()"

# Default command
CMD ["python", "main.py", "start"]
