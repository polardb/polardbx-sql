#!/bin/bash

# Copyright 2021 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GALAXYSQL_HOME=/home/admin/drds-server

function galaxysql_pid() {
  jps | grep Tddl | cut -d ' ' -f 1
}

function kill_and_clean() {
  local pid
  pid=$(galaxysql_pid)
  if [ -n "$pid" ]; then
    kill -9 "$pid"
  fi
  rm -f $GALAXYSQL_HOME/bin/*.pid
}

function prepare() {
  mkdir -p /home/admin/bin/
  declare -xp > /home/admin/bin/server_env.sh
}

function start_process() {
  $GALAXYSQL_HOME/bin/startup.sh -D
}

last_pid=0
function report_pid() {
  pid=$(galaxysql_pid)
  if [ -z "$pid" ]; then
    echo "Process dead. Exit."
    last_pid=0
    return 1
  else
    if [[ $pid -ne $last_pid ]]; then
      echo "Process alive: " "$pid"
    fi
    last_pid=pid
  fi
  return 0
}

function watch() {
  while report_pid; do
    sleep 5
  done
}

function start() {
  # Start
  prepare
  kill_and_clean
  start_process
}

# Retry start and watch

retry_interval=5
retry_cnt=0
retry_limit=1
if [[ "$#" -ge 1 ]]; then
  retry_limit=$1
fi

while [[ $retry_cnt -lt $retry_limit ]]; do
  start
  watch

  ((retry_cnt++))

  if [[ $retry_cnt -lt $retry_limit ]]; then
    sleep $retry_interval
  fi
done

# Abort.
exit 1