-- name: GetDatasetByLabel :one
SELECT * FROM `benchmark_dataset` WHERE `label` = ? LIMIT 1;

-- name: CreateDataset :execresult
INSERT INTO `benchmark_dataset` (`label`, `rows`, `created_at`) VALUES (?, ?, ?);

-- name: GetQueryByLabel :one
SELECT * FROM `benchmark_query` WHERE `dataset_id` = ? AND `label` = ? LIMIT 1;

-- name: CreateQuery :execresult
INSERT INTO `benchmark_query` (`dataset_id`, `label`, `first_seen_at`, `tags`) VALUES (?, ?, ?, ?);

-- name: UpdateQueryTags :exec
UPDATE `benchmark_query` SET `tags` = ? WHERE `id` = ?;

-- name: InsertQueryResult :exec
INSERT INTO `benchmark_query_result` (`query_id`, `timestamp`, `p0_us`, `p25_us`, `p50_us`, `p75_us`, `p90_us`, `p95_us`, `p99_us`, `p100_us`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
