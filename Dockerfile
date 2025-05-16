# Use an official Python runtime as a parent image
# Using python:3.10-slim for a balance of features and image size.
FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir requests

CMD ["sh", "-c", "echo 'Quay tools image ready. Use \"podman run\" to execute scripts like get_quay_logs.py or compile_log_stats.py. Available scripts:'; ls -l"]