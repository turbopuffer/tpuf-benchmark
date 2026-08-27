#!/bin/bash

NODE_NAME="github-actions-tpuf-benchmark"
PROJECT="turbopuffer-test"
ZONES=(us-central1-a us-central1-b us-central1-c us-central1-f)

INSTANCE_MISSING=2

instance_zone() {
	local zones
	if ! zones=$(gcloud compute instances list --project=$PROJECT --filter="name=$NODE_NAME" --format='value(zone)'); then
		echo "Failed to look up instance $NODE_NAME in project $PROJECT" >&2
		return 1
	fi
	if [ -z "$zones" ]; then
		echo "Instance $NODE_NAME not found in project $PROJECT" >&2
		return $INSTANCE_MISSING
	fi
	if [ "$(echo "$zones" | wc -l)" -gt 1 ]; then
		echo "Multiple instances named $NODE_NAME found in zones: $(echo "$zones" | tr '\n' ' ')" >&2
		return 1
	fi
	echo "$zones"
}
