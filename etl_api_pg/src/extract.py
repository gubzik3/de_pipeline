# extract.py
# Назначение: загрузка данных из API в data/raw/YYYY-MM-DD.json
# TODO:
# - CLI-интерфейс/функция extract(date)
# - retries + backoff
# - atomic write в data/_tmp, затем move в data/raw
# - логирование метрик (raw_bytes, raw_records, elapsed_ms)
