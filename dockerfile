# Use a lightweight Python image to save RAM
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system tools needed for Python libraries
# Added build-essential for Protobuf/scipy/arch compilation
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first
COPY requirements.txt .

# Install Python dependencies
# Note: pip will now find version 2.19.0
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Create essential directory structure
RUN mkdir -p logs data dashboard_data static_dashboard

# Run BOTH the API and the Engine
CMD ["bash", "-c", "python main.py & python core/engine.py"]
