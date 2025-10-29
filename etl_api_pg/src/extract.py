from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import pathlib
import subprocess
import logger

log = logger.setup_logger(file_name='extrct.log')
log = logger.get_logger(__name__)


def extract(endpoint):
    log.debug("Начинаем загрузку Датасета")

    data_directory = pathlib.Path(f"data/kaggle/{endpoint}")
    data_directory.mkdir(parents=True, exist_ok=True)
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(f"{endpoint}", path=data_directory, unzip=True)

    log.debug("Загрузка завершена")
    #return df

