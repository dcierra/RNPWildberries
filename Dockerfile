FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends cron && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN touch /var/log/cron.log && chmod 0644 /var/log/cron.log

RUN echo "0 3,9,18 * * * root cd /app && PYTHONPATH=/app /usr/local/bin/python3 -m wb.task_runner >> /var/log/cron.log 2>&1" > /etc/cron.d/taskrunner && chmod 0644 /etc/cron.d/taskrunner

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
CMD ["/entrypoint.sh"]
