#!/bin/bash
# Prints stored bytes (from Cosmos quota accounting) and storage amplification
# for the benchmark containers. Logical (raw ingested) bytes are the measured
# MSMarco sums: 980,653,742 B of text for the full 1M rows, plus 2,048 B/doc
# of f16 vector where applicable, avg 3,029 B/row while vectors is mid-ingest.
set -euo pipefail

RESOURCE_GROUP="${RESOURCE_GROUP:-tpuf-cosmos-bench}"
ACCOUNT_NAME="${ACCOUNT_NAME:-tpuf-cosmos-bench}"
ENDPOINT="https://$ACCOUNT_NAME.documents.azure.com"

KEY=$(az cosmosdb keys list -g "$RESOURCE_GROUP" -n "$ACCOUNT_NAME" --query primaryMasterKey -o tsv)

usage_for() {
  local coll=$1
  local date sig auth
  date=$(LC_ALL=C date -u '+%a, %d %b %Y %H:%M:%S GMT' | tr '[:upper:]' '[:lower:]')
  sig=$(printf 'get\ncolls\ndbs/tpufbench/colls/%s\n%s\n\n' "$coll" "$date" |
    openssl dgst -sha256 -mac HMAC -macopt "hexkey:$(printf %s "$KEY" | base64 -d | xxd -p -c 256)" -binary | base64)
  auth=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "type=master&ver=1.0&sig=$sig")
  curl -s -i "$ENDPOINT/dbs/tpufbench/colls/$coll" \
    -H "Authorization: $auth" -H "x-ms-date: $date" \
    -H "x-ms-version: 2018-12-31" -H "x-ms-documentdb-populatequotainfo: True" |
    grep -i 'x-ms-resource-usage' | grep -oE 'collectionSize=[0-9]+;documentsSize=[0-9]+;documentsCount=[0-9]+'
}

for coll in text vectors; do
  u=$(usage_for "$coll")
  stored_kb=$(echo "$u" | grep -oE 'collectionSize=[0-9]+' | cut -d= -f2)
  docs=$(echo "$u" | grep -oE 'documentsCount=[0-9]+' | cut -d= -f2)
  python3 - "$coll" "$stored_kb" "$docs" <<'EOF'
import sys
coll, stored_kb, docs = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
stored = stored_kb * 1024
TEXT_1M = 980_653_742
if coll == "text":
    logical, note = TEXT_1M, "final" if docs == 1_000_000 else "in progress"
else:
    if docs == 1_000_000:
        logical, note = TEXT_1M + 2048 * docs, "final"
    else:
        logical, note = 3029 * docs, f"in progress ({docs:,} docs, logical estimated)"
print(f"{coll}: stored {stored/2**30:.3f} GiB, logical {logical/2**30:.3f} GiB, "
      f"amp {stored/logical:.2f}x  [{note}]")
EOF
done
