FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system build dependencies (required for some python packages)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directory structure for logs and data
RUN mkdir -p /app/data /app/dashboard_data /app/logs

# Environment variables
ENV PYTHONPATH=/app
ENV ENV=production
ENV PYTHONUNBUFFERED=1

# Expose API port
EXPOSE 8000

# Run the application
CMD ["python", "main.py"]
