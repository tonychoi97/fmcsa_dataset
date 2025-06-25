FROM python:3.12.10

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY import_data.py import_data.py

ENTRYPOINT [ "python", "import_data.py" ]