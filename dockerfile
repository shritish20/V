# Use a lightweight Python image to save RAM
FROM python:3.11-slim

# Set working directory inside the server
WORKDIR /app

# Install system tools needed for some Python libraries
# Added 'procps' so the Sheriff can use 'kill' commands
# Added 'curl' for healthchecks
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (to cache them)
COPY requirements.txt .

# Install Python dependencies including Supervisor
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Create persistent directories for logs and data
# This prevents "File not found" errors on startup
RUN mkdir -p logs data dashboard_data

# Copy Supervisor Configuration
COPY supervisord.conf /app/supervisord.conf

# EXPOSE the API Port
EXPOSE 8000

# SAFETY: Use Supervisord to run ALL processes (Engine, API, Sheriff, Sentinel)
CMD ["supervisord", "-c", "/app/supervisord.conf"]
