#!/bin/bash

function check_pid_and_log() {
  sleep 30
  while [ ! $(ps -ef | grep 'go test -timeout=20m -v ./...' | grep -v 'grep' | awk '{ print $2 }' | wc -l) -eq 0 ]; do
    echo "test still running..."
    sleep 60
  done
  echo "looks like testing has finished"
}

check_pid_and_log &

go test -timeout=20m -v ./...
