FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY geocoding_worker.py .

CMD ["python", "-u", "geocoding_worker.py"]
