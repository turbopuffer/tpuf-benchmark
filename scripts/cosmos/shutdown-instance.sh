#!/bin/bash

set -ex

NODE_NAME="tpuf-cosmos-bench-vm"
RESOURCE_GROUP="tpuf-cosmos-bench"

# Deallocate the instance.
echo "Deallocating instance..."
az vm deallocate --resource-group $RESOURCE_GROUP --name $NODE_NAME
