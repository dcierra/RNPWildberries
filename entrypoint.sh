#!/bin/bash
set -e

echo ">>> Starting cron daemon"
cron -f &

echo ">>> Optionally run task_runner once at startup"
python /app/wb/task_runner.py || true &

echo ">>> Exec Streamlit in foreground"
streamlit run /app/dashboard/app.py --server.port=8501 --server.address=0.0.0.0 --browser.gatherUsageStats false
