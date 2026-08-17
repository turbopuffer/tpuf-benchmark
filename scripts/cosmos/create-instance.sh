#!/bin/bash

set -ex

NODE_NAME="${NODE_NAME:-tpuf-cosmos-bench-vm}"
RESOURCE_GROUP="tpuf-cosmos-bench"
LOCATION="eastus2"
SIZE="${SIZE:-Standard_D32ds_v5}"
ADMIN_USER="tpuf"

az vm create \
	--resource-group $RESOURCE_GROUP \
	--name $NODE_NAME \
	--location $LOCATION \
	--image Ubuntu2404 \
	--size $SIZE \
	--admin-username $ADMIN_USER \
	--generate-ssh-keys \
	--os-disk-size-gb 64 \
	--public-ip-sku Standard
