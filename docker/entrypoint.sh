#!/bin/sh
set -eu

command="${1:-api}"

if [ "${BIGNAME_INDEXER_CHAIN_RPC_URLS+x}" = "x" ] && [ -z "$BIGNAME_INDEXER_CHAIN_RPC_URLS" ]; then
  unset BIGNAME_INDEXER_CHAIN_RPC_URLS
fi

case "$command" in
  -*)
    exec bigname-api "$@"
    ;;
esac

case "$command" in
  api)
    shift
    exec bigname-api serve "$@"
    ;;
  indexer)
    shift
    exec bigname-indexer run "$@"
    ;;
  worker)
    shift
    exec bigname-worker run "$@"
    ;;
  migrate)
    shift
    exec bigname-worker migrate "$@"
    ;;
  print-openapi)
    shift
    exec bigname-api print-openapi "$@"
    ;;
  bigname-api | bigname-indexer | bigname-worker)
    exec "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
