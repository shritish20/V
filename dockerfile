# Use a lightweight Python image to save RAM
FROM python:3.11-slim

# Set working directory inside the server
WORKDIR /app

# Install system tools needed for some Python libraries
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (to cache them)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Create logs directory
RUN mkdir -p logs

# Run BOTH the API and the Engine in one container (Saves RAM)
CMD ["bash", "-c", "python main.py & python core/engine.py"]
