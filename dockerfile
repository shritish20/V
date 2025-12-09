FROM python:3.11-slim

# Set working directory
WORKDIR /app

# CRITICAL FIX: Install system dependencies + SSL certificates
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    ca-certificates \
    openssl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# CRITICAL FIX: Create directory structure for logs and data
# Ensure data directory exists for instrument cache
RUN mkdir -p /app/data /app/dashboard_data /app/logs

# CRITICAL FIX: If you have pre-downloaded instrument file, copy it
# Uncomment this line if you place complete.json.gz in project root:
# COPY complete.json.gz /app/data/complete.json.gz

# Environment variables
ENV PYTHONPATH=/app
ENV ENV=production
ENV PYTHONUNBUFFERED=1

# CRITICAL FIX: SSL certificate environment variables
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["python", "main.py"]
