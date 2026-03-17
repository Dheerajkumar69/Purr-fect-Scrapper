#!/bin/bash
set -e

# Start the server in the background
../.venv/bin/python main.py &
SERVER_PID=$!
sleep 3

echo "=> Submitting a slow job..."
JOB_RES=$(curl -s -X POST "http://localhost:8000/api/v2/scrape" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "http://httpbin.org/delay/10",
    "engines": ["static_httpx"],
    "depth": 1,
    "max_pages": 1,
    "respect_robots": false,
    "timeout_per_engine": 30
  }')

echo "Job Response: $JOB_RES"
JOB_ID=$(echo $JOB_RES | grep -o '"job_id":"[^"]*' | cut -d'"' -f4)

if [ -z "$JOB_ID" ]; then
    echo "Failed to get Job ID"
    kill $SERVER_PID
    exit 1
fi

echo "=> Got job ID: $JOB_ID. Waiting 2 seconds..."
sleep 2

echo "=> Sending Cancel command..."
CANCEL_RES=$(curl -s -X POST "http://localhost:8000/jobs/$JOB_ID/cancel")
echo "Cancel Response: $CANCEL_RES"

echo "=> Waiting 5 seconds to let the backend settle..."
sleep 5

echo "=> Checking Job Status..."
STATUS_RES=$(curl -s "http://localhost:8000/jobs/$JOB_ID")
echo "Status Response: $STATUS_RES"

kill $SERVER_PID
