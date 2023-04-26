#!/bin/bash

# Function to check if the Python process is running
is_process_running() {
  pgrep -f "/home/alextay96/Desktop/all_workspace/Zhihu-KOL/download_answer.py" > /dev/null
}

# Function to terminate the Python process
terminate_process() {
  pid=$(pgrep -f "/home/alextay96/Desktop/all_workspace/Zhihu-KOL/download_answer.py")
  if [ -n "$pid" ]; then
    echo "Terminating process $pid"
    kill -9 "$pid"
    sleep 5  # Wait for the process to terminate gracefully
  fi
}

# Loop to monitor and restart the Python process
while true; do
  # Initial startup of the Python script
  echo "Starting Python script"
  python /home/alextay96/Desktop/all_workspace/Zhihu-KOL/download_answer.py &

  # Sleep for 3 minutes
  sleep 300
  # Terminate and restart the Python process
  echo "Restarting Python script"
  terminate_process
done