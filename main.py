import database_tools
import pandas as pd
import sqlite3
from common import user_folder
import os
import numpy as np
from utils import mellanni_modules as mm

start_date = pd.to_datetime('today').date() - pd.Timedelta(days=181)
start_date = "2025-05-30" if start_date < pd.to_datetime("2025-05-30").date() else start_date

end_date = pd.to_datetime('today').date() - pd.Timedelta(days=1)

inv = database_tools.read_database('fba_inventory', start_date, pd.to_datetime('today').date())
sales = database_tools.read_database('sales', start_date, end_date)

dictionary_path = r'G:\Shared drives\30 Sales\30.1 MELLANNI\30.11 AMAZON\30.111 US\Sales\Dictionary.xlsx'
dictionary = pd.read_excel(dictionary_path, usecols=['ASIN','Collection','Size','Color'])
dictionary = dictionary.rename(columns={'ASIN':'asin', 'Collection':'collection', 'Size':'size', 'Color':'color'})
dictionary = dictionary.drop_duplicates('asin')

sets_in_a_box_path = r'G:\My Drive\Все файлы\AMAZON\ASIN dimensions_2.xlsx'
sets_in_a_box = pd.read_excel(sets_in_a_box_path, usecols=['asin','sets_in_a_box'])

days_of_sale = 49

def calculate_inventory_isr(inventory):
    filter = inventory['sku'].str.endswith('-CA') # Create a filter for Canadian SKUs ONLY

    inventory = inventory[filter][['snapshot-date', 'asin', 'inventory_supply_at_fba']] # apply the filter and select relevant columns
    inventory_grouped = inventory.groupby(['snapshot-date','asin']).agg('sum').reset_index()
    total_days = inventory_grouped['snapshot-date'].nunique()

    inventory_grouped['in_stock?'] = inventory_grouped['inventory_supply_at_fba'] > 0
    asin_isr = (inventory_grouped.groupby('asin')[['in_stock?']].agg('mean').reset_index()).round(2)
    return asin_isr, total_days


def fill_dates(df:pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date','(child)_asin','units_ordered','sessions_-_total']]
    start_date = df['date'].min()
    end_date = df['date'].max()
    date_range = pd.date_range(start=start_date, end=end_date)
    full_dates = pd.DataFrame(date_range, columns=['date'])
    asin_list = df['(child)_asin'].unique()
    
    # result = pd.DataFrame()
    all_files = []
    for asin in asin_list:
        temp_df = df[df['(child)_asin']==asin]
        full_asin = pd.merge(full_dates, temp_df, how = 'left', on = 'date')
        full_asin['(child)_asin']=asin
        all_files.append(full_asin)
    result = pd.concat(all_files)
    result = result.fillna(0)
    # result.to_clipboard(index=False)

def last_2_weeks_sales(df_sales:pd.DataFrame, df_inventory:pd.DataFrame):
    df_sales['date'] = pd.to_datetime(df_sales['date'])
    df_inventory = df_inventory.rename(columns={'snapshot-date': 'date', 'asin':'(child)_asin'})
    df_inventory['date'] = pd.to_datetime(df_inventory['date'])
    last_date = df_sales['date'].max()
    cut_off_date = last_date - pd.Timedelta(days=13)
    latest_sales = df_sales[df_sales['date'] >= cut_off_date]
    latest_inventory = df_inventory[df_inventory['date'] >= cut_off_date]
    latest_inventory = latest_inventory.groupby(['date','(child)_asin'])[['inventory_supply_at_fba']].agg('sum').reset_index()
    latest_inventory['in_stock?'] = latest_inventory['inventory_supply_at_fba'] > 0
    latest_isr = latest_inventory.groupby('(child)_asin')[['in_stock?']].agg('mean').reset_index()
    latest_sales = latest_sales.groupby('(child)_asin')[['units_ordered','sessions_-_total']].agg('sum').reset_index()
    latest_sales = pd.merge(latest_sales, latest_isr, on='(child)_asin', how='outer')
    # latest_sales = latest_sales.rename(columns={'(child)_asin': 'asin'})
    latest_sales['average_2_weeks_units'] = latest_sales['units_ordered'] / 14
    latest_sales['average_2_weeks_sessions'] = (latest_sales['sessions_-_total'] / 14).round(3)
    latest_sales['average_2_weeks_units_corrected'] = (latest_sales['average_2_weeks_units'] / latest_sales['in_stock?']).round(3)
    return latest_sales[['(child)_asin','average_2_weeks_units_corrected', 'average_2_weeks_sessions']]



def get_asin_sales(sales, total_day):
    sales_filtered = sales[sales['sku'].str.endswith('-CA')][['date','(child)_asin','units_ordered','sessions_-_total']].copy()
    sales_daily = sales_filtered.groupby(['date','(child)_asin']).agg('sum').reset_index()
    #remove later
    # sales_daily.to_clipboard(index=False)
    latest_sales = last_2_weeks_sales(sales, inv)


    total_sales = sales_daily.groupby('(child)_asin')[['units_ordered','sessions_-_total']].agg('sum').reset_index()
    total_sales['average_daily_units'] = (total_sales['units_ordered'] / total_day).round(3)
    total_sales['average_daily_sessions'] = (total_sales['sessions_-_total'] / total_day).round(3)
    total_sales = pd.merge(total_sales, latest_sales, on='(child)_asin', how='outer')
    total_sales = total_sales.rename(columns={'(child)_asin': 'asin'})
    return total_sales


def main():
    asin_isr, total_day = calculate_inventory_isr(inv[['snapshot-date', 'sku', 'asin', 'inventory_supply_at_fba']].copy())
    total_sales = get_asin_sales(sales, total_day)
    result = pd.merge(asin_isr, total_sales, on='asin', how='outer')
    result['in_stock?'] = result['in_stock?'].fillna(0)
    result['units_ordered'] = result['units_ordered'].fillna(0)
    result['sessions_-_total'] = result['sessions_-_total'].fillna(0)
    result['average_daily_units'] = result['average_daily_units'].fillna(0)
    result['average_daily_sessions'] = result['average_daily_sessions'].fillna(0)
    result['average_corrected_long'] = (result['average_daily_units'] / result['in_stock?']).round(3)
    result['average_corrected_long'] = result['average_corrected_long'].fillna(0)
    result['average_2_weeks_units_corrected'] = result['average_2_weeks_units_corrected'].fillna(0)
    # result['average_corrected'] = result[['average_corrected_long','average_2_weeks_units_corrected']].mean(axis=1) #use `axis = ` to specify the direction of the mean calculation (rows)
    result['average_corrected'] = (result['average_corrected_long']*0.4 + result['average_2_weeks_units_corrected'] *0.6).round(3)
    inv_copy = inv.copy()
    inv_copy['snapshot-date'] = pd.to_datetime(inv_copy['snapshot-date'])
    latest_date = inv_copy['snapshot-date'].max()
    latest_inventory = inv_copy[inv_copy['snapshot-date'] == latest_date]
    quantity_map = latest_inventory.groupby('asin')['inventory_supply_at_fba'].sum().reset_index()
    result = pd.merge(result, quantity_map, on='asin', how='outer')
    result['inventory_supply_at_fba'] = result['inventory_supply_at_fba'].fillna(0)
    result['days_of_sale_remaining'] = (result['inventory_supply_at_fba'] / result['average_corrected']).round(0)
    result['days_of_sale_remaining'] = result['days_of_sale_remaining'].fillna(0)
    result['to_ship_units'] = ((result['average_corrected'] * days_of_sale - result['inventory_supply_at_fba']).round(0)).apply(lambda x: x if x > 0 else 0)
    result = pd.merge(result, sets_in_a_box, on='asin', how='inner')
    result['to_ship_boxes'] = (np.ceil(result['to_ship_units'] / result['sets_in_a_box'])).apply(lambda x: x if x > 0 else 0)
    result['dos_with_to_ship_boxes'] = (((result['sets_in_a_box'] * result['to_ship_boxes'] + result['inventory_supply_at_fba']) / result['average_corrected']).round(0)).apply(lambda x: x if x > 0 else 0)
    result = pd.merge(result, dictionary, on='asin', how='inner')
    mm.export_to_excel([result],['restock'], 'inventory_restock.xlsx', user_folder)
    os.startfile(os.path.join(user_folder))

if __name__ == "__main__":
    main()