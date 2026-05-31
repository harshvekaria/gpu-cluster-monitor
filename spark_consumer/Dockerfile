FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y python3-pip curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -o /opt/spark/jars/postgresql-42.6.0.jar

COPY consumer.py .

CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "--jars", "/opt/spark/jars/postgresql-42.6.0.jar", \
     "consumer.py"]