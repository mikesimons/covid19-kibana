#!/bin/bash -e -x

DATE="$(date +%Y%m%d)"
FILE="$DATE.src.json"
ESFILE="$DATE.es.json"

if ! [ -e "$FILE" ]; then
  curl -o"$FILE" "https://api.covid19api.com/all?$DATE"
fi

go run covid.go "$FILE" esbulk > "$ESFILE"

curl -H "Content-Type: application/json" -XPOST "localhost:9200/covid/_bulk?pretty&refresh" --data-binary "@$ESFILE"
