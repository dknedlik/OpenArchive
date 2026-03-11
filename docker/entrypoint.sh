#!/bin/sh
set -eu

command="${1:-serve}"

if [ "$command" = "serve" ]; then
  echo "running migrations before serve"
  until /usr/local/bin/open_archive migrate; do
    echo "migrate failed, retrying in 2s"
    sleep 2
  done
fi

exec /usr/local/bin/open_archive "$@"
