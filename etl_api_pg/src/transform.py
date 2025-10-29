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

global_path = Path("C:/Users/Влад/PycharmProjects/de_pipeline/etl_api_pg/src/data/kaggle")
dataset_path = 'fivethirtyeight/uber-pickups-in-new-york-city'
pattern = re.compile(r"uber-raw-data.*14.*\.csv$")
data_directory = global_path/dataset_path

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

def check_columns(df):
    with open('schema.yaml', 'r', encoding='utf-8') as f:
        schema = yaml.safe_load(f)
    actual_columns = list(df.columns)
    expected_columns = [col['name'] for col in schema['columns']]
    return actual_columns == expected_columns

def detect_delimeter(file_, encoding):
    with open(file_, 'r', encoding=encoding) as f:
        sample = f.read(5000)
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(sample).delimiter
    return delimiter
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

def column_cast(df, column_types):
    for typle_ in column_types:
        for col, col_type in typle_.items():
            if col_type == 'datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif col_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif col_type == 'str':
                df[col] = df[col].astype('str')
    df.rename(columns={'Date/Time': 'datetime',
                       'Lon': 'lon',
                       'Lat': 'lat',
                       'Base': 'base'}, inplace=True)
    return df

def transform():
    df_all = create_df(data_directory, pattern)
    n_before = len(df_all)
    df_all.drop_duplicates(inplace=True)
    n_after = len(df_all)
    log.debug(f"Удалено дублей:{n_before-n_after}")

    df_all = df_all.map(lambda x: x.strip() if type(x)==str else x)

    with open('schema.yaml', 'r', encoding='utf-8') as f:
        schema = yaml.safe_load(f)
        df_all = column_cast(df_all, schema['type_columns'])
    return df_all
