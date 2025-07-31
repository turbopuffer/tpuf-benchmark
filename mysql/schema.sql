-- Required SQL schema for the MySQL database

CREATE TABLE `benchmark_dataset` (
    `id` bigint not null auto_increment,
    `label` varchar(255) not null,
    `rows` int not null,
    `created_at` datetime not null default current_timestamp,
    `updated_at` datetime not null default current_timestamp on update current_timestamp,
    primary key (`id`),
    unique key `idx_label` (`label`)
);

CREATE TABLE `benchmark_query` (
    `id` bigint not null auto_increment,
    `dataset_id` bigint not null,
    `label` varchar(255) not null,
    `first_seen_at` datetime not null default current_timestamp,
    `last_seen_at` datetime not null default current_timestamp on update current_timestamp,
    `tags` json not null,
    primary key (`id`),
    unique key `idx_dataset_label` (`dataset_id`, `label`)
);

CREATE TABLE `benchmark_query_result` (
    `id` bigint not null auto_increment,
    `query_id` bigint not null,
    `timestamp` datetime not null default current_timestamp,
    `p0_us` bigint not null,
    `p25_us` bigint not null,
    `p50_us` bigint not null,
    `p75_us` bigint not null,
    `p90_us` bigint not null,
    `p95_us` bigint not null,
    `p99_us` bigint not null,
    `p100_us` bigint not null,
    primary key (`id`),
    key `idx_query_timestamp` (`query_id`, `timestamp`)
);