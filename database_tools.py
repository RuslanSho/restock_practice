import sqlite3
import pandas as pd
import os
from utils_restock import check_folders, read_files


if not check_folders():
    print('Please create the reports_data folder and its subfolders (fba_inventory, sales) before running the script.')
    raise BaseException('Required folders are missing.')
else:
    print('All required folders exist. Proceeding with the script...')


def update_fba_inventory(df: pd.DataFrame):
    dates_list = df['snapshot-date'].unique()

    dates_list_str ='","'.join(dates_list)
    try: 
        delete_query = f"""
                DELETE FROM fba_inventory
                WHERE DATE("snapshot-date") IN ("{dates_list_str}")"""

        with sqlite3.connect('restock_canada.db') as conn_delete:
            cursor = conn_delete.cursor()
            result = cursor.execute(delete_query)

        print(f'Deleted: {result.rowcount} rows from fba_inventory')
    except Exception as error:
        print(f"This error while deleting data from fba inventory:\n{error}")

    with sqlite3.connect('restock_canada.db') as connector:
        df.to_sql('fba_inventory', connector, if_exists='append', index=False)


def update_sales(df: pd.DataFrame):
    dates_list = [str(pd.to_datetime(x).date()) for x in df['date'].unique().tolist()]
    dates_list_str ='","'.join(dates_list)

    try:
        delete_query = f"""
                DELETE FROM sales
                WHERE DATE(date) IN ("{dates_list_str}")"""

        with sqlite3.connect('restock_canada.db') as conn_delete:
            cursor = conn_delete.cursor()
            result = cursor.execute(delete_query)

        print(f'Deleted: {result.rowcount} rows from sales')
    except Exception as error:
        print(f"This error while deleting data from sales:\n{error}")

    with sqlite3.connect('restock_canada.db') as connector:
        df.to_sql('sales', connector, if_exists='append', index=False)

    


def read_database(table_name, start_date, end_date):
    date_column = 'date' if table_name == 'sales' else 'snapshot-date'

    with sqlite3.connect('restock_canada.db') as connector:
        query = f'select * from {table_name} WHERE DATE("{date_column}") BETWEEN DATE("{start_date}") AND DATE("{end_date}")'
        result = pd.read_sql(query, connector)
    return result

if __name__ == "__main__":

    inventory = read_files('fba_inventory')
    sales = read_files('sales')


    update_fba_inventory(inventory)
    update_sales(sales)