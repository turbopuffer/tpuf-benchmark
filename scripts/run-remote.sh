#!/bin/bash

set -ex

NODE_NAME="github-actions-tpuf-benchmark"
PROJECT="turbopuffer-test"
ZONE="us-central1-c"

SSH="gcloud compute ssh $NODE_NAME --project=$PROJECT --zone=$ZONE --tunnel-through-iap --"
SCP="gcloud compute scp --project=$PROJECT --zone=$ZONE --tunnel-through-iap"

if [ -n "$DURATION" ]; then
	DURATION_FLAG="--duration $DURATION"
fi

if [ -n "$API_ENDPOINT" ]; then
	API_ENDPOINT_FLAG="--endpoint $API_ENDPOINT"
fi

# Start the instance if it's not already running.
STATUS=$(gcloud compute instances describe $NODE_NAME --project=$PROJECT --zone=$ZONE --format='get(status)')
if [ "$STATUS" != "RUNNING" ]; then
	echo "Instance is $STATUS, starting..."
	gcloud compute instances start $NODE_NAME --project=$PROJECT --zone=$ZONE
	# Wait for SSH to become available.
	echo "Waiting for SSH..."
	for i in $(seq 1 30); do
		if $SSH true 2>/dev/null; then
			break
		fi
		sleep 5
	done
fi

# Upload the binary and benchmark configs.
$SCP tpufbench $NODE_NAME:~/
$SCP --recurse benchmarks $NODE_NAME:~/

# Run each benchmark on the remote instance.
BENCHMARKS=$(find benchmarks -name '*.toml')
$SSH rm -rf results
$SSH mkdir -p results
for f in $BENCHMARKS; do
	name="${f#benchmarks/}"
	name="${name%.toml}"
	echo "Running benchmark: $name"
	$SSH TURBOPUFFER_API_KEY="$TURBOPUFFER_API_KEY" REGION="$REGION" DATASET_CACHE_DIR="~/dataset-cache" \
		./tpufbench run $DURATION_FLAG $API_ENDPOINT_FLAG \
		--namespace-prefix tpufbench-nightly \
		--namespace-setup-concurrency=16 \
		--if-nonempty=clear \
		--output-dir "results/$name" "$f"
done

# Download results.
rm -rf results
$SCP --recurse $NODE_NAME:~/results .
