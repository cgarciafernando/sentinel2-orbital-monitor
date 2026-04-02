FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    libgdal-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "pipeline.py"]