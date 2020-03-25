#!/bin/bash -e -x

DATE="$(date +%Y%m%d)"
ESFILE="$DATE.es.json"

go run covid.go > "$ESFILE"

curl -H "Content-Type: application/json" -XDELETE "localhost:9200/covid"
curl -H "Content-Type: application/json" -XPOST "localhost:9200/covid/_bulk?pretty&refresh" --data-binary "@$ESFILE"
