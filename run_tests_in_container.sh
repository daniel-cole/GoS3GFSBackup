#!/bin/bash

yum update -q -y

curl -o /var/tmp/go1.9 https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz
tar -xvf /var/tmp/go1.9 -C /
export PATH=$PATH:/go/bin
export GOPATH=/gopath

# Check if test is still running
function check_pid_and_log() {
  sleep 30
  while [ ! $(ps -ef | grep 'go test ./...' | grep -v 'grep' | awk '{ print $2 }' | wc -l) -eq 0 ]; do
    echo "test still running..."
    sleep 60
  done
  echo "looks like testing has finished"
}

check_pid_and_log &

cd $GOPATH/src/github.com/daniel-cole/GoS3GFSBackup && go test ./...

echo "finished running tests"
