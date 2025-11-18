# # Dockerfile for agent
# # FROM python:3.11-slim
# # WORKDIR /app
# # COPY requirements.txt .
# # RUN pip install --no-cache-dir -r requirements.txt
# # COPY . .
# # ENV PORT=8080
# # CMD ["python", "agent.py"]
# # Use official Python image
# # FROM python:3.11-slim

# # WORKDIR /app
# # COPY requirements.txt .
# # RUN pip install --no-cache-dir -r requirements.txt

# # # Copy app
# # COPY . .

# # ENV PORT=8080
# # EXPOSE 8080

# # # Use gunicorn for production server
# # CMD exec gunicorn --bind :$PORT server:app
# # Dockerfile (overwrite existing Dockerfile)
# FROM python:3.11-slim
# WORKDIR /app
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
# COPY . .
# ENV PORT=8080
# EXPOSE 8080
# CMD ["gunicorn", "--bind", ":8080", "agent:app"]


# Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "server:app", "--workers=1", "--threads=8", "--timeout=120"]
