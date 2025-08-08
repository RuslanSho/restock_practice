import os
import pandas as pd
from column_names import sales_columns, fba_inventory_columns


def check_folders():
    reports_dir = 'reports_data'
    subfolders = ['fba_inventory','sales']
    # print(f'Root dir (reports_data) exists? : {os.path.exists(reports_dir)}')
    if not {os.path.exists(reports_dir)}:
        return False
    for subfolder in subfolders:
        subfolder_path = os.path.join(reports_dir, subfolder)
        # print(f'Subfolder {subfolder} exists? : {os.path.exists(subfolder_path)}')
        if not os.path.exists(subfolder_path):
            # print(f'Creating subfolder: {subfolder_path}')
            # os.makedirs(subfolder_path, exist_ok=True)
            return False
    return True


def read_files(subfolder):
    if subfolder == 'fba_inventory':
        target_columns = fba_inventory_columns
    else:
        target_columns = sales_columns
    result = pd.DataFrame()
    file_list = os.listdir(os.path.join('reports_data', subfolder))
    for file in file_list:
        if not file.endswith('.csv'):
            print(f'Skipping non-CSV file: {file}')
            continue
        temp_file = pd.read_csv(os.path.join('reports_data', subfolder, file))
        columns_old = temp_file.columns.tolist()
        columns_new = [x.strip().replace('–','-').replace(' ','_').lower() for x in columns_old]
        temp_file.columns = columns_new
        if subfolder == 'sales':
            date_str = file.replace('.csv','')
            date = pd.to_datetime(date_str)
            temp_file['date'] = date
            temp_file = temp_file.rename(
                columns={
                    "unit_session_percentage_-_b2b":"units_session_percentage_-_b2b",
                    "unit_session_percentage":"units_session_percentage"
                    }
                    )
        check_column_names(temp_file, file, target_columns)
        result = pd.concat([result, temp_file], axis=0, ignore_index=True)
    return result


def check_column_names(df, file_name, target_columns):
    df_columns = df.columns.tolist()
    # print("Checking file columns")
    for column in df_columns:
        if column not in target_columns:
            raise BaseException(f"Database column missing: {column}\nFile name: {file_name}")

    # print("Checking db columns")
    for column in target_columns:
        if column not in df_columns:
            raise BaseException(f"File is missing {column} column\nFile name: {file_name}")