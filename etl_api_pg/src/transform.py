# transform.py
# Назначение: валидация/очистка, dedup, запись Parquet в data/curated
# TODO:
# - чтение raw
# - валидация по schema.yml
# - нормализация типов, дедуп по бизнес-ключу
# - запись Parquet (snappy), row groups
# - отчёт DQ в data/_dq/YYYY-MM-DD.json
