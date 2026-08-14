# cosmosbench

Create an Azure Cosmos DB account, configure the benchmark containers, ingest MSMarco, inspect storage, and run queries:

```sh
eval "$(RESOURCE_GROUP=bench ACCOUNT_NAME=mycosmos LOCATION=westus2 scripts/cosmos/account.sh)"
go run ./cmd/cosmosbench setup
go run ./cmd/cosmosbench ingest
RESOURCE_GROUP=bench ACCOUNT_NAME=mycosmos scripts/cosmos/stats.sh
go run ./cmd/cosmosbench query --mode ann
```

Use `--recall` with ANN queries to compare DiskANN results with brute-force vector search. Use `--mode fts` to benchmark full-text search.
