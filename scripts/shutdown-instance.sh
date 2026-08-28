#!/bin/bash

set -ex

source "$(dirname "$0")/common.sh"

rc=0
ZONE=$(instance_zone) || rc=$?
if [ $rc -eq $INSTANCE_MISSING ]; then
	echo "Nothing to stop."
	exit 0
elif [ $rc -ne 0 ]; then
	echo "Could not determine instance zone; instance may still be running." >&2
	exit $rc
fi

# Stop the instance.
echo "Stopping instance..."
gcloud compute instances stop $NODE_NAME --project=$PROJECT --zone=$ZONE
