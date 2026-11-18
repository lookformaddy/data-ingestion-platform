FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY scripts/ ./scripts/
ENV PYTHONPATH=/app/src
ENV INGEST_STATE_DIR=/app/.ingest-state
EXPOSE 8000
CMD ["python", "-m", "ingest", "serve", "--host", "0.0.0.0", "--port", "8000"]
