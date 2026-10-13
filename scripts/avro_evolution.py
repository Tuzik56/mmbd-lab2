#!/usr/bin/env python3
"""
Часть 2. Avro: поток процессинга и эволюция схемы. ШАБЛОН.

Avro — строковый формат с схемой внутри файла. Его место — не аналитика,
а поток: Kafka, Schema Registry, обмен между сервисами (лекция 9).
Здесь мы (1) пишем сырые операции процессинга в Avro и сравниваем размер
с Parquet и JSON, (2) меняем схему и смотрим, что читается, а что ломается.

Запуск: python scripts/avro_evolution.py [../lab_1/data]
"""
import os
import math
from fastavro._read_common import SchemaResolutionError
import pathlib
import sys
import time

import duckdb
from fastavro import parse_schema, reader, writer

DATA_DIR = sys.argv[1] if len(sys.argv) > 1 else "../lab_1/data"
OUT = pathlib.Path(__file__).resolve().parent.parent / "data"
OUT.mkdir(exist_ok=True)

# Схема v1 — то, что процессинг отдаёт сегодня. Обратите внимание: типы
# явные, NULL разрешён только там, где объявлен union с "null".
SCHEMA_V1 = {
    "type": "record", "name": "Transaction", "namespace": "sinitsa.processing",
    "fields": [
        {"name": "txn_id",   "type": "string"},
        {"name": "txn_ts",   "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "card_id",  "type": "string"},
        {"name": "mcc",      "type": "string"},
        {"name": "channel",  "type": "string"},
        {"name": "amount",   "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "status",   "type": "string"},
        {"name": "decline_reason", "type": ["null", "string"], "default": None},
    ],
}

con = duckdb.connect()
rows = con.execute(f"""
    select txn_id, txn_ts, card_id, mcc, channel, amount, currency, status, decline_reason
    from read_parquet('{DATA_DIR}/processing/transactions_2026-07.parquet')
""").fetchall()
names = [f["name"] for f in SCHEMA_V1["fields"]]
records = [dict(zip(names, r)) for r in rows]
print(f"строк процессинга за июль: {len(records):,}")

# ── 1. Запись в Avro (deflate) и сравнение размеров ──────────────────────────
t0 = time.perf_counter()
with open(OUT / "txn_july.avro", "wb") as f:
    writer(f, parse_schema(SCHEMA_V1), records, codec="deflate")
print(f"Avro записан за {time.perf_counter() - t0:.1f} с")

# Те же 9 полей — в JSON Lines и в Parquet+zstd: сравнивать форматы честно
# можно только на одинаковом наборе колонок (в исходном Parquet их 12).
cols = ", ".join(names)
for name, opts in (("txn_july.jsonl", "format json"),
                   ("txn_july.parquet", "format parquet, compression zstd")):
    con.execute(f"""copy (select {cols}
                          from read_parquet('{DATA_DIR}/processing/transactions_2026-07.parquet'))
                    to '{OUT / name}' ({opts})""")
for name in ("txn_july.jsonl", "txn_july.avro", "txn_july.parquet"):
    print(f"  {name:22s} {os.path.getsize(OUT / name) / 1e6:7.1f} МБ")

# ── 2. Схема внутри файла: читатель узнаёт её из заголовка ───────────────────
with open(OUT / "txn_july.avro", "rb") as f:
    rd = reader(f)
    print("схема писателя (из заголовка файла):", [x["name"] for x in rd.writer_schema["fields"]])
    print("первая запись:", next(rd))

# ── 3. Эволюция: v2 добавляет поле С default — старые файлы читаются ─────────
SCHEMA_V2 = {**SCHEMA_V1, "fields": SCHEMA_V1["fields"] + [
    {"name": "terminal_id", "type": ["null", "string"], "default": None},
]}
with open(OUT / "txn_july.avro", "rb") as f:
    rec = next(reader(f, reader_schema=parse_schema(SCHEMA_V2)))
    print("v2-читатель читает v1-файл, terminal_id =", rec["terminal_id"])

# ── TODO 1. Схема v3: добавьте поле БЕЗ default (например, "terminal_id":
#           "string"). Прочитайте v1-файл с reader_schema=v3. Что произошло
#           и почему? Запишите текст ошибки в отчёт.
SCHEMA_V3 = {**SCHEMA_V1, "fields": SCHEMA_V1["fields"] + [
    {"name": "terminal_id", "type": "string"},
]}
try:
    with open(OUT / "txn_july.avro", "rb") as f:
        next(reader(f, reader_schema=parse_schema(SCHEMA_V3)))
except SchemaResolutionError as exc:
    print(f"v3: ожидаемая ошибка {type(exc).__name__}: {exc}")
else:
    raise AssertionError("v3 должна требовать отсутствующее поле terminal_id")


# ── TODO 2. Переименуйте в v4 поле amount в amount_orig (без aliases).
#           Прочитайте v1-файл. Затем добавьте "aliases": ["amount"] к полю
#           и повторите. Сформулируйте правило: какие изменения схемы
#           безопасны для старых данных, какие — нет.
SCHEMA_V4 = {**SCHEMA_V1, "fields": [
    {**field, "name": "amount_orig"} if field["name"] == "amount" else dict(field)
    for field in SCHEMA_V1["fields"]
]}
try:
    with open(OUT / "txn_july.avro", "rb") as f:
        next(reader(f, reader_schema=parse_schema(SCHEMA_V4)))
except SchemaResolutionError as exc:
    print(f"v4 без aliases: ожидаемая ошибка {type(exc).__name__}: {exc}")
else:
    raise AssertionError("Переименование без aliases должно вызвать ошибку")

SCHEMA_V4_ALIASES = {**SCHEMA_V4, "fields": [
    {**field, "aliases": ["amount"]} if field["name"] == "amount_orig" else dict(field)
    for field in SCHEMA_V4["fields"]
]}
with open(OUT / "txn_july.avro", "rb") as f:
    renamed = next(reader(f, reader_schema=parse_schema(SCHEMA_V4_ALIASES)))
assert renamed["amount_orig"] == records[0]["amount"]
print("v4 с aliases: amount_orig =", renamed["amount_orig"], "— значение сохранено")


# ── TODO 3. Самое важное. Прочитайте из Avro только одну колонку:
#           суммируйте amount по всем записям через reader(f) и замерьте время.
#           Сравните с duckdb: select sum(amount) from read_parquet('data/txn_july.parquet').
#           Объясните разницу устройством форматов, а не «Python медленный»:
#           сколько байт Avro-читатель обязан распаковать и разобрать, чтобы
#           добраться до amount?
t0 = time.perf_counter()
with open(OUT / "txn_july.avro", "rb") as f:
    avro_sum = sum(rec["amount"] for rec in reader(f))
avro_seconds = time.perf_counter() - t0

t0 = time.perf_counter()
parquet_sum = con.execute("select sum(amount) from read_parquet(?)",
                          [str(OUT / "txn_july.parquet")]).fetchone()[0]
parquet_seconds = time.perf_counter() - t0
assert math.isclose(avro_sum, parquet_sum, rel_tol=1e-12, abs_tol=0.01)
print(f"Сумма amount Avro: {avro_sum:.2f}; время {avro_seconds:.6f} с")
print(f"Сумма amount Parquet: {parquet_sum:.2f}; время {parquet_seconds:.6f} с")
print(f"Отношение времени Avro / Parquet: {avro_seconds / parquet_seconds:.1f}")
print("Суммы совпадают с допуском для double")
amount_bytes = con.execute("""
    select sum(total_compressed_size) from parquet_metadata(?)
    where path_in_schema = 'amount'
""", [str(OUT / "txn_july.parquet")]).fetchone()[0]
print(f"Parquet: сжатые column chunks amount = {amount_bytes} байт (без footer)")
con.close()

