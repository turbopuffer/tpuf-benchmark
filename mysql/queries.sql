-- name: GetDatasetByLabel :one
SELECT * FROM `benchmark_dataset` WHERE `label` = ? LIMIT 1;

-- name: CreateDataset :execresult
INSERT INTO `benchmark_dataset` (`label`, `created_at`) VALUES (?, ?);

-- name: GetQueryByLabel :one
SELECT * FROM `benchmark_query` WHERE `dataset_id` = ? AND `label` = ? LIMIT 1;

-- name: CreateQuery :execresult
INSERT INTO `benchmark_query` (`dataset_id`, `label`, `created_at`, `tags`) VALUES (?, ?, ?, ?);

-- name: UpdateQueryTags :exec
UPDATE `benchmark_query` SET `tags` = ? WHERE `id` = ?;

-- name: InsertQueryResult :exec
INSERT INTO `benchmark_query_result` (`query_id`, `timestamp`, `p0_ms`, `p25_ms`, `p50_ms`, `p75_ms`, `p90_ms`, `p95_ms`, `p99_ms`, `p100_ms`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
