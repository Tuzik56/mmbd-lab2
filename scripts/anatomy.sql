-- Часть 2.1: число групп и размер footer (в байтах).
select file_name, num_rows, num_row_groups, footer_size
from parquet_file_metadata(['data/txn_zstd.parquet', 'data/txn_rg10k.parquet', 'data/txn_rg1m.parquet']);

-- Часть 2.2: размер каждой колонки и кодировки.
select path_in_schema as col, type,
       sum(total_compressed_size) / 1e6 as mb_compressed,
       sum(total_uncompressed_size) / 1e6 as mb_raw,
       any_value(encodings) as encodings,
       max(stats_distinct_count) as max_dictionary_distinct
from parquet_metadata('data/txn_zstd.parquet')
group by 1, 2 order by 3 desc;

-- Дополнение: фактическая кардинальность и NULL, а не размер локального словаря.
select count(distinct merchant_name_raw) as unique_merchant_name_raw,
       count(distinct txn_id) as unique_txn_id,
       count(distinct amount_rub) as unique_amount_rub,
       count(distinct txn_date) as unique_dates,
       count(distinct orig_txn_id) as unique_orig_txn_id,
       count(*) filter (where orig_txn_id is null) as orig_txn_id_nulls,
       count(*) filter (where currency = 'RUB') as rub_count,
       count(*) as total_rows
from txn_wide;

-- Часть 2.3: диапазоны дат и возможность пропуска группы.
select file_name, row_group_id, stats_min, stats_max,
       not (stats_min::date <= date '2026-07-15'
            and stats_max::date >= date '2026-07-15') as can_skip
from parquet_metadata(['data/txn_zstd.parquet', 'data/txn_shuffled.parquet'])
where path_in_schema = 'txn_date'
order by file_name, row_group_id;
