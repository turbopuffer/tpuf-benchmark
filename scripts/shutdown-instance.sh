#!/bin/bash

set -ex

NODE_NAME="github-actions-tpuf-benchmark"
PROJECT="turbopuffer-test"
ZONE="us-central1-c"

# Stop the instance.
echo "Stopping instance..."
gcloud compute instances stop $NODE_NAME --project=$PROJECT --zone=$ZONE --discard-local-ssd=true
