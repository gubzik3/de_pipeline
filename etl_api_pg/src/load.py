import sqlite3
import pandas as pd
from transform import transform


#Создаём таблицу с промежуточными данными
def create_table(conn, staging_table, main_table, columns):

    columns_sql = ', '.join([f"{col} {ctype}" for col, ctype in columns.items()])
    conn.executescript(f"""
        CREATE TABLE IF NOT EXISTS {main_table} (
            {columns_sql}
        );
    
        CREATE TABLE IF NOT EXISTS {staging_table} (
            {columns_sql}
        )
    """)
    conn.commit()

#Чистим таблицу для загрузки данных батчами
def clear_staging_table(conn, staging_table):
    conn.execute(f"DELETE FROM {staging_table}")
    conn.commit()

#Наполняем главную таблицу с данными
def transfer_batch(conn, staging_table, main_table, columns):
    col_list = ', '.join(columns.keys())
    conn.execute(f"""
        INSERT INTO {main_table} ({col_list})
        SELECT {col_list} FROM {staging_table}
    """)
    conn.commit()

#Загрузка батчами
def batch_load(df, db_path, main_table, staging_table, columns, batch_size=10000):
    conn = sqlite3.connect(db_path)
    create_table(conn, staging_table, main_table, columns)

    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]
        try:
            clear_staging_table(conn, staging_table)

            batch.to_sql(staging_table, conn, if_exists='append', index=False)

            transfer_batch(conn, staging_table, main_table, columns)
            print(f"Batch {start}-{start + len(batch)} loaded and transferred.")
        except Exception as e:
            print(f"Error in batch {start}-{start + len(batch)}: {e}")

    conn.close()

