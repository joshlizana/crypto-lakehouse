FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY producers/ ./producers/

# Run the producer
CMD ["python", "-m", "producers.coinbase_producer"]
