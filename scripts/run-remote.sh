#!/bin/bash

set -ex

source "$(dirname "$0")/common.sh"

START_ATTEMPTS=5
START_RETRY_DELAY=120
ZONE=$(instance_zone)

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
	for attempt in $(seq 1 $START_ATTEMPTS); do
		if gcloud compute instances start $NODE_NAME --project=$PROJECT --zone=$ZONE; then
			break
		fi
		if [ "$attempt" -eq "$START_ATTEMPTS" ]; then
			echo "Instance failed to start after $START_ATTEMPTS attempts"
			exit 1
		fi
		echo "Start failed (attempt $attempt/$START_ATTEMPTS), retrying in ${START_RETRY_DELAY}s..."
		sleep $START_RETRY_DELAY
	done
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
$SSH rm -rf benchmarks
$SCP --recurse benchmarks $NODE_NAME:~/

# Run each benchmark on the remote instance. The set of nightly benchmarks is
# determined by the binary (definitions marked `nightly = true`); run via SSH
# since the uploaded binary is built for the remote's architecture.
#
# TODO: Move the loop below into the binary (e.g. `tpufbench run-suite
# --nightly`) so this script is just instance lifecycle + scp.
BENCHMARKS=$($SSH ./tpufbench list --nightly benchmarks)
$SSH rm -rf results
$SSH mkdir -p results
for f in $BENCHMARKS; do
	name="${f#benchmarks/}"
	name="${name%.toml}"
	echo "Running benchmark: $name"
	$SSH TURBOPUFFER_API_KEY="$TURBOPUFFER_API_KEY" REGION="$REGION" DATASET_CACHE_DIR="~/dataset-cache" \
		./tpufbench run $DURATION_FLAG $API_ENDPOINT_FLAG \
		--namespace-prefix "tpufbench-nightly_${name//\//-}" \
		--namespace-setup-concurrency=16 \
		--if-nonempty=clear \
		--output-dir "results/$name" "$f"
done

# Download results.
rm -rf results
$SCP --recurse $NODE_NAME:~/results .
