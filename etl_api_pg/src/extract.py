from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import pathlib
import subprocess
import logger

log2 = logger.setup_logger(file_name='test.log')


def extract(endpoint):
    log = logger.get_logger(__name__)
    log.debug("Начинаем загрузку Датасета")

    data_directory = pathlib.Path(f"data/kaggle/{endpoint}")
    data_directory.mkdir(parents=True, exist_ok=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(f"{endpoint}", path=data_directory, unzip=True)

    for file_ in data_directory.iterdir():
        try:
            df = pd.read_csv(file_, encoding='utf-8', sep=',')
        except Exception as e:
            print(f"Ошибка {e}")
    log.debug("Загрузка завершена")
    return df

extract('jockeroika/life-style-data')

# endpoint = 'jockeroika/life-style-data'
# csv_path = pathlib.Path(__file__).parent
# df = extract(endpoint, csv_path)
