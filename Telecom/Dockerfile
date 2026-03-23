FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    mysql-connector-python \
    faker \
    python-dateutil

COPY generate_churn_data.py .

CMD ["python", "generate_churn_data.py", \
     "--host",     "mysql", \
     "--port",     "3306", \
     "--user",     "pg_user", \
     "--password", "pg_password_2024", \
     "--database", "telecom_churn", \
     "--customers","500"]
