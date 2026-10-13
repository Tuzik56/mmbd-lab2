-- Q1: txn.csv
explain analyze select federal_district, sum(amount_rub) from read_csv('data/txn.csv', types={'mcc': 'VARCHAR', 'txn_date': 'DATE'}) where status = 'approved' group by 1;

-- Q1: txn_zstd.parquet
explain analyze select federal_district, sum(amount_rub) from read_parquet('data/txn_zstd.parquet') where status = 'approved' group by 1;
