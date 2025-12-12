FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    psycopg2-binary \
    pymysql \
    rich

COPY dashboard.py .

CMD ["python", "dashboard.py"]



