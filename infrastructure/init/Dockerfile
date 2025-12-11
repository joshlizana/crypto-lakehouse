FROM python:3.11-slim

WORKDIR /init

RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    confluent-kafka==2.3.0 \
    requests==2.31.0

COPY . /init/

CMD ["python", "/init/init-all.py"]
