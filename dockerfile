# Use a lightweight Python image to save RAM
FROM python:3.11-slim

# Set working directory inside the server
WORKDIR /app

# Install system tools needed for Python libraries
# HARDENING: Added build-essential and python3-dev for Protobuf/V3 compilation
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (to cache them and speed up builds)
COPY requirements.txt .

# Install Python dependencies 
# Note: Ensure requirements.txt has 'protobuf' and 'upstox-python-sdk>=2.19.0'
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Create essential directory structure for data persistence and logs
RUN mkdir -p logs data dashboard_data static_dashboard

# Run BOTH the API and the Engine in one container (Original VolGuard RAM-Save Mode)
CMD ["bash", "-c", "python main.py & python core/engine.py"]
