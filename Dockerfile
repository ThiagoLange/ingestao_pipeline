FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir --retries 5 --timeout 30 -r requirements.txt

COPY src/ ./src/

CMD ["python", "src/main.py"]
