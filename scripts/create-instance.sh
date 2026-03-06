#!/bin/bash

set -ex

NODE_NAME="github-actions-tpuf-benchmark"
PROJECT="turbopuffer-test"
ZONE="us-central1-c"
# https://cloud.google.com/compute/vm-instance-pricing
# https://cloud.google.com/compute/docs/general-purpose-machines
DISK_TYPE="hyperdisk-balanced"
MACHINE_TYPE="c4a-standard-32-lssd"
IMAGE_SUFFIX="-arm64"

gcloud compute instances create $NODE_NAME \
	--project=$PROJECT \
	--zone=$ZONE \
	--machine-type=$MACHINE_TYPE \
    --no-restart-on-failure \
	--network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
	--maintenance-policy=MIGRATE \
	--provisioning-model=STANDARD \
	--local-ssd-recovery-timeout=1 \
	--instance-termination-action=STOP \
	--max-run-duration=10800s \
	--no-service-account \
	--no-scopes \
	"${DISK_ARGS[@]}" \
	--create-disk=auto-delete=yes,boot=yes,device-name=$NODE_NAME,image=projects/debian-cloud/global/images/debian-12-bookworm$IMAGE_SUFFIX-v20250910,mode=rw,size=200,type=projects/$PROJECT/zones/$ZONE/diskTypes/$DISK_TYPE \
	--no-shielded-secure-boot \
	--shielded-vtpm \
	--shielded-integrity-monitoring \
	--tags=github-actions-ssh \
	--labels=goog-ec-src=vm_add-gcloud \
	--reservation-affinity=any \
	--discard-local-ssds-at-termination-timestamp=true
