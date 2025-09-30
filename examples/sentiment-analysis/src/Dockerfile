FROM python:3.10-slim

# Install dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV MINIO_ENDPOINT="172.17.0.1:9000"
ENV MINIO_ACCESS_KEY=minio
ENV MINIO_SECRET_KEY=minio123
ENV MINIO_BUCKET=serverledge
ENV MINIO_SECURE=false

ARG HANDLER_ENV=retrieve
ENV HANDLER_ENV=$HANDLER_ENV

# Copy project files
COPY executor.py /
COPY extractor.py /
COPY minio_client.py /
COPY ml_model.py /
COPY retriever.py /

WORKDIR /

EXPOSE 8080

CMD ["python", "-u", "executor.py"]
