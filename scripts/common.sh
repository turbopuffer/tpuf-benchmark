#!/bin/bash

NODE_NAME="github-actions-tpuf-benchmark"
PROJECT="turbopuffer-test"
ZONES=(us-central1-a us-central1-b us-central1-c us-central1-f)

INSTANCE_MISSING=2

instance_zone() {
	local zone out
	for zone in "${ZONES[@]}"; do
		if out=$(gcloud compute instances describe "$NODE_NAME" --project="$PROJECT" --zone="$zone" --format='value(name)' 2>&1); then
			echo "$zone"
			return 0
		fi
		if ! grep -qi "was not found" <<<"$out"; then
			echo "Failed to look up $NODE_NAME in $zone: $out" >&2
			return 1
		fi
	done
	echo "Instance $NODE_NAME not found in project $PROJECT" >&2
	return $INSTANCE_MISSING
}
