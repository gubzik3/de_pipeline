import re
import pathlib
from extract import extract
from transform import transform
from load import batch_load


global_path = pathlib.Path("C:/Users/Влад/PycharmProjects/de_pipeline/etl_api_pg/src/data/kaggle")
dataset_path = 'fivethirtyeight/uber-pickups-in-new-york-city'
pattern = re.compile(r"uber-raw-data.*14.*\.csv$")
data_directory = global_path/dataset_path

main_table = 'uber_rides'
name_stagind_table = "uber_rides_staging"
engine = "C:/Users/Влад/PycharmProjects/de_pipeline/data/curated/db.sqlite3"
batch_size = 10000

columns = {
    "id": "INTEGER",
    "datetime_": "TIMESTAMP",
    "lat": "FLOAT",
    "lon": "FLOAT",
    "base": "TEXT"
}
def main():
    extract(dataset_path)
    batch_load(transform(data_directory, pattern),          # DataFrame
                engine,                                     # Путь к базе
                main_table,                                  # Основная таблица
                name_stagind_table,
                columns,                                    # Словарь имя:тип (структура таблицы)
                batch_size
               )

if __name__ == "__main__":
    main()



