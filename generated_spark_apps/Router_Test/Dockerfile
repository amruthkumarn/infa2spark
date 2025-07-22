FROM apache/spark-py:v3.4.0

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/main/python/ ./
COPY config/ ./config/
COPY data/ ./data/

CMD ["spark-submit", "--master", "local[*]", "main.py"]
