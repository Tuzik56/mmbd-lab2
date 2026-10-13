#!/usr/bin/env python3
"""
Часть 5. Arrow: формат данных В ПАМЯТИ и передача между движками без копий.

Parquet — как таблица лежит на диске. Arrow — как она лежит в оперативной
памяти, одинаково для DuckDB, Polars, pandas 2, Spark, DataFusion. Пока
такого стандарта не было, «передать таблицу из движка в Python» означало
сериализовать и разобрать её заново.

Запуск: python scripts/arrow_demo.py
"""
import pathlib
import time
import json
import pandas as pd

import duckdb
import polars as pl

ROOT = pathlib.Path(__file__).resolve().parent.parent
con = duckdb.connect(str(ROOT / "formats.duckdb"), read_only=True)


measurements = {}


def timed(label, fn):
    t = time.perf_counter()
    res = fn()
    elapsed = time.perf_counter() - t
    measurements[label] = elapsed
    print(f"  {label:55s} {elapsed:7.3f} с")
    return res


print("Путь 1 — через Arrow:")
tbl = timed("DuckDB → Arrow Table (.to_arrow_table())", lambda: con.sql("select * from txn_wide").to_arrow_table())
df = timed("Arrow → Polars (pl.from_arrow)", lambda: pl.from_arrow(tbl))
res = timed("Polars: group_by федеральный округ",
            lambda: df.group_by("federal_district").agg(pl.col("amount_rub").sum()))
back = timed("Polars DataFrame → снова запрос DuckDB (через Arrow)",
             lambda: con.sql("select count(*) from df where status = 'approved'").fetchall())

print("Путь 2 — через файл (так делали до Arrow):")
tmp_csv = ROOT / "data" / "_arrow_demo.csv"
timed("DuckDB → CSV на диск", lambda: con.execute(f"copy txn_wide to '{tmp_csv}' (header)"))
timed("CSV → Polars (read_csv)", lambda: pl.read_csv(tmp_csv))
tmp_csv.unlink()

# TODO 1. Добавьте путь 3: DuckDB → pandas (.df()) → Polars (pl.from_pandas).
#         pandas без pyarrow-типов хранит строки объектами Python — это копия
#         и конвертация. Сравните время с путём 1 и объясните.
print("Путь 3 — через pandas:")
pandas_df = timed("DuckDB → pandas (.df())", lambda: con.sql("select * from txn_wide").df())
pandas_polars = timed("pandas → Polars (pl.from_pandas)", lambda: pl.from_pandas(pandas_df))
print("pandas:", pd.__version__, "fio dtype:", pandas_df["fio"].dtype)
print("pandas fio array:", type(pandas_df["fio"].array).__name__)
assert pandas_polars.shape == df.shape == (2312606, 28)
assert pandas_polars.equals(df), "Arrow и pandas дали разные данные"
print("Проверка Arrow / pandas: все значения таблиц совпадают")


# TODO 2. Посмотрите на tbl.schema. Найдите, как Arrow хранит txn_ts и fio
#         (тип, единицы времени). Сравните с parquet_schema() того же набора:
#         где Arrow и Parquet совпадают, а где различаются, и почему это
#         два разных формата, а не один.
print("Arrow schema:")
print(tbl.schema)
parquet_types = con.execute("""
    select name, type, converted_type, logical_type
    from parquet_schema(?) where name in ('txn_ts', 'fio')
""", [str(ROOT / 'data' / 'txn_zstd.parquet')]).fetchall()
print("Parquet: name, physical type, converted type, logical type")
for row in parquet_types:
    print(row)
print("Arrow txn_ts:", tbl.schema.field('txn_ts').type)
print("Arrow fio:", tbl.schema.field('fio').type)
(ROOT / 'results').mkdir(exist_ok=True)
(ROOT / 'results' / 'part5_arrow.json').write_text(json.dumps({
    'seconds': measurements,
    'shape': df.shape,
    'pandas_version': pd.__version__,
    'pandas_fio_dtype': str(pandas_df['fio'].dtype),
    'pandas_fio_array': type(pandas_df['fio'].array).__name__,
    'arrow_schema': str(tbl.schema),
    'parquet_types': parquet_types,
    'arrow_pandas_equal': True,
}, ensure_ascii=False, indent=2))
con.close()

