import datetime
import chardet
import csv
import re
import yaml

import logger
import pandas as pd
from pathlib import Path

log = logger.setup_logger(file_name="trnsfrm.log")
log = logger.get_logger(__name__)

#Функция detect_encoding принимает на вход файл, результатом работы будет кадировка исходного файла
def detect_encoding(file_):
    encodings = ['utf-8', 'cp1252', 'latin1', 'utf-16', 'cp1251']
    with open(file_, 'rb') as f:
        result = chardet.detect(f.read(10000))
    encoding = result['encoding']

    for enc in ([encoding] + encodings):
        if enc is None or enc == 'ascii' or enc == 'johab':
            continue
        try:
            with open(file_, 'r', encoding=enc) as f:
                f.read()
            return enc
        except Exception:
            continue
    return 'latin1'

#Функция check_columns принимает Датафрейм и проверяет соответствует ли его содержимое требованиям
def check_columns(df):
    with open('schema.yaml', 'r', encoding='utf-8') as f:
        schema = yaml.safe_load(f)
    actual_columns = list(df.columns)
    expected_columns = [col['name'] for col in schema['columns']]
    return actual_columns == expected_columns

#Функция определяет разделитель
def detect_delimeter(file_, encoding):
    with open(file_, 'r', encoding=encoding) as f:
        sample = f.read(5000)
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(sample).delimiter
    return delimiter

#Создаёт Датафрейм из всех файлов, которые соответствуют проверке из check_columns
def create_df(data_directory, pattern):
    dfs = []
    report = {"success": 0, "bad-columns": 0, "error": 0, "pattern-mismatch": 0}
    for file_ in data_directory.iterdir():
        if pattern.search(file_.name):
            try:
                enc = detect_encoding(file_)
                delim = detect_delimeter(file_, enc)
                df = pd.read_csv(file_, encoding=enc, sep=delim)
                if check_columns(df):
                    dfs.append(df)
                    report["success"] += 1
                else:
                    report["bad-columns"] += 1
                    log.warning(f"В файле {file_.name} имеются несовпадающие столбцы")
            except Exception as e:
                report["error"] += 1
                log.error(f"При считывании файла {file_.name} Ошибка {e}")
        else:
            report["pattern-mismatch"] += 1
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
    else:
        df_all = pd.DataFrame()
    log.debug(f"Summary: {report}")
    return df_all

#Приводим все поля к одному виду
def column_cast(df, column_types):
    for typle_ in column_types:
        for col, col_type in typle_.items():
            if col_type == 'datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif col_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif col_type == 'str':
                df[col] = df[col].astype('str')
    df.rename(columns={'Date/Time': 'datetime_',
                       'Lon': 'lon',
                       'Lat': 'lat',
                       'Base': 'base'}, inplace=True)
    return df

#Собираем все функции в кучу и на выходи получаем готовый DF
def transform(data_directory, pattern):
    df_all = create_df(data_directory, pattern)
    n_before = len(df_all)
    df_all.drop_duplicates(inplace=True)
    n_after = len(df_all)
    log.debug(f"Удалено дублей:{n_before-n_after}")

    df_all = df_all.map(lambda x: x.strip() if type(x)==str else x)

    with open('schema.yaml', 'r', encoding='utf-8') as f:
        schema = yaml.safe_load(f)
        df_all = column_cast(df_all, schema['type_columns'])
        df_all['id'] = range(1, len(df_all) + 1)
    return df_all
