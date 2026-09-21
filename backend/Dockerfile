FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV RUN_ENV=docker
ENV PYTHONUNBUFFERED=1

# Actual command is overridden per-service in docker-compose.yml
CMD ["python", "server.py"]
