#!/bin/bash

set -e

source "$(dirname "$0")/common.sh"

# https://cloud.google.com/compute/vm-instance-pricing
# https://cloud.google.com/compute/docs/general-purpose-machines
DISK_TYPE="hyperdisk-balanced"
MACHINE_TYPE="c4a-standard-32"
IMAGE_SUFFIX="-arm64"

rc=0
EXISTING=$(instance_zone) || rc=$?
if [ $rc -eq 0 ]; then
	echo "Instance $NODE_NAME already exists in $EXISTING" >&2
	exit 1
elif [ $rc -ne $INSTANCE_MISSING ]; then
	exit $rc
fi

for ZONE in "${ZONES[@]}"; do
	echo "Attempting to create instance in $ZONE..."
	if gcloud compute instances create $NODE_NAME \
		--project=$PROJECT \
		--zone=$ZONE \
		--machine-type=$MACHINE_TYPE \
		--no-restart-on-failure \
		--network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
		--maintenance-policy=MIGRATE \
		--provisioning-model=STANDARD \
		--instance-termination-action=STOP \
		--max-run-duration=21600s \
		--no-service-account \
		--no-scopes \
		--create-disk=auto-delete=yes,boot=yes,device-name=$NODE_NAME,image=projects/debian-cloud/global/images/debian-12-bookworm$IMAGE_SUFFIX-v20250910,mode=rw,size=200,type=projects/$PROJECT/zones/$ZONE/diskTypes/$DISK_TYPE \
		--no-shielded-secure-boot \
		--shielded-vtpm \
		--shielded-integrity-monitoring \
		--tags=github-actions-ssh \
		--labels=goog-ec-src=vm_add-gcloud \
		--reservation-affinity=any; then
		echo "Created instance in $ZONE"
		exit 0
	fi
	rc=0
	EXISTING=$(instance_zone) || rc=$?
	if [ $rc -eq 0 ]; then
		echo "Create failed but instance exists in $EXISTING; refusing to create another" >&2
		exit 1
	elif [ $rc -ne $INSTANCE_MISSING ]; then
		echo "Create failed and instance lookup was inconclusive; refusing to continue" >&2
		exit $rc
	fi
	echo "Could not create instance in $ZONE, trying next zone..."
done

echo "Failed to create instance in any zone: ${ZONES[*]}" >&2
exit 1
