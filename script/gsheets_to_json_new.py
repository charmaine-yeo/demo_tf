import pandas as pd
import json
import os
import re

def no_dup_read_sheets(url):
    df = pd.read_csv(url)
    select_col = [ col for col in df.columns if not col.startswith("Unnamed") ]
    df = df[select_col].copy()

    return df
    
def read_gsheet(sheet_id, dataset_sheet_name, table_sheet_name):
   
    #gsheet url 
    dataset_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={dataset_sheet_name}' 
    table_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={table_sheet_name}' 

    dataset_df = no_dup_read_sheets(dataset_url)
    table_df = no_dup_read_sheets(table_url)

    return dataset_df, table_df

def remove_whitespaces(text):
    if isinstance(text, str):
        return re.sub(r'\s+', '', text)
    return text

def clean_description(record):
    if pd.isna(record.get('description')) or record.get('description') == '':
        del record['description']
    return record

new_sheet_id = '1qt0ojzO4bmB32dCobnnEiROB2Hhns2EVIwfpmsat3Hk'

# google sheet name 
new_dataset_sheet_name = 'dataset'
new_table_sheet_name = 'table'

new_dataset_df,new_table_df = read_gsheet(new_sheet_id,new_dataset_sheet_name,new_table_sheet_name)

dataset_grouped = {k: v.to_dict(orient='records') 
                   for k, v in new_dataset_df.groupby('datasetId')}

# Create the JSON structure
dataset_json_output = []

for dataset_id, records in dataset_grouped.items():
    # Extract dataset-level information (excluding NaNs)
    dataset_info = {k: remove_whitespaces(v) for k, v in records[0].items() if pd.notna(v) and k not in ['tableId']}
    dataset_info['tables'] = []
    rem_list = ['partitionField', 'partitionType', 'clustering']
 
# Remove multiple keys from dictionary
    for key in rem_list:
        if pd.notna(key) and key in dataset_info:
            del dataset_info[key]
        else:
            print(f"Key '{key}' is either NaN or not in the dictionary.")

    for record in records:
        if 'tableId' in record:
            table_info = {k: remove_whitespaces(v) for k, v in record.items() if pd.notna(v) and k in ['tableId', "partitionType", "partitionField", 'clustering']}
            tableSchemaPath = f"resource/schema/{dataset_info["datasetId"]}/{table_info["tableId"]}.json"
            table_info["tableSchemaPath"] = tableSchemaPath
            if 'clustering' in table_info:
                table_info['clustering'] = [remove_whitespaces(c) for c in table_info['clustering'].split(',')]
            dataset_info['tables'].append(table_info)
    
    dataset_json_output.append(dataset_info)
    
file_path = 'datasets_with_tables.json'

with open(file_path, 'w') as file:
    json.dump(dataset_json_output, file, indent=4)

# Tables 

table_grouped = new_table_df.groupby(['dataset', 'tablename'])

for (dataset, table), group in table_grouped:
    rem_list = ['description']
        # display(group["description"])
    dir_path = os.path.join(dataset)
    os.makedirs(dir_path, exist_ok=True)

    group = group.drop(columns=['dataset','tablename'])

    table_json_output = group.rename(columns={
        'column_name': 'name',
        'column_type': 'type',
        'column_mode': 'mode',
        'description': 'description'
    }).to_dict(orient='records')
    
    table_json_output = [clean_description(record) for record in group.to_dict(orient='records')]

    file_path = os.path.join(dir_path, f'{table}.json')

    with open(file_path, 'w') as file:
        json.dump(table_json_output, file, indent=4)