#!/bin/bash
#
max_attempts=60
echo "Waiting for command '$@' to be successful..."
while ! eval $@; do
  attempts=$(($attempts+1))

  if [[ $attempts -gt $max_attempts ]]; then
    echo "command '$@' took too long to be successful"
    exit 1
  fi

  sleep 1
done