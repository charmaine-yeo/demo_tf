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

mig_sheet_id = '1w4z3se0TbTo8Peo0sRoeyb-HLtpQglCItmuws7a1Ig0'

# google sheet name 
mig_dataset_sheet_name = 'table_list'
mig_table_sheet_name = 'schema_list'

mig_dataset_df,mig_table_df = read_gsheet(mig_sheet_id,mig_dataset_sheet_name,mig_table_sheet_name)

#Get relevant columns. 
columns_with_destination = [col for col in mig_dataset_df.columns if 'destination' in col.lower()]
destination_cols = mig_dataset_df[columns_with_destination]
destination_cols = destination_cols[2:].reset_index(drop=True)
rename_cols ={
    'destination_dataset': 'datasetId', 
    'destination_dataset_region': 'datasetRegion',
    'destination_table': 'tableId',
    'destination_table_description': 'datasetDescription',
    'destination_partition_field': 'partitionField',
    'destination_partition_type': 'partitionType',
    'destination_table_clustering': 'clustering',
    'destination_dataset_labels': 'datasetLabels',
    'destination_table_labels': 'tableLabels'
}

destination_cols.rename(columns=rename_cols, inplace=True)
mig_final_df = destination_cols.copy()
mig_drop_cols = ["destination_project",
                 'destination_dataset_tags']
mig_final_df.drop(mig_drop_cols,axis='columns', inplace=True)

mig_dataset_grouped = {k: v.to_dict(orient='records') 
                   for k, v in mig_final_df.groupby('datasetId')}


# Create the JSON structure
mig_dataset_json_output = []
count = 0

for dataset_id, records in mig_dataset_grouped.items():
    mig_dataset_info = {k: remove_whitespaces(v) for k, v in records[0].items() if pd.notna(v) and k not in ['tableId']}
    mig_dataset_info['tables'] = []
    mig_rem_list = ['partitionField', 'partitionType', 'clustering','tableLabels']
    try:
        dataset_labels_dict = {}
        if mig_dataset_info['datasetLabels']:
            dataset_labels_str = mig_dataset_info.get('datasetLabels')
            if dataset_labels_str:
                label_pairs = dataset_labels_str.split(',')
                for pair in label_pairs:
                    key, value = pair.split(':', 1) 
                    dataset_labels_dict[key] = value
                mig_dataset_info['datasetLabels'] = dataset_labels_dict
    except: 
        print("No dataset labels")
        # mig_dataset_info.get('datasetLabels')
    
# Remove multiple keys from dictionary
    for key in mig_rem_list:
        if pd.notna(key) and key in mig_dataset_info:
            del mig_dataset_info[key]
        else:
            print(f"Key '{key}' is either NaN or not in the dictionary.")

    for record in records:
        if 'tableId' in record:
            mig_table_info = {k: remove_whitespaces(v) for k, v in record.items() if pd.notna(v) and k in ['tableId', "partitionType", "partitionField", 'clustering','tableLabels']}
            mig_tableSchemaPath = f"resource/schema/{mig_dataset_info["datasetId"]}/{mig_table_info["tableId"]}.json"
            mig_table_info["tableSchemaPath"] = mig_tableSchemaPath
            optional_fields = ['clustering','datasetLabels','tableLabels']
            for x in optional_fields: 
                try:
                    mig_table_info[x] = [remove_whitespaces(c) for c in mig_table_info[x].split(',')]
                except:
                    print(f"No {x}")
            try:        
                table_labels_dict = {}
                if mig_table_info['tableLabels']:
                    table_labels_str = mig_table_info.get('tableLabels')
                    print(table_labels_str)
                    if table_labels_str:
                        # for kvp in table_labels_str:
                        #     print("WEEHOO")
                        for pair in table_labels_str:
                            key, value = pair.split(':', 1) 
                            table_labels_dict[key] = value
                            print(table_labels_dict)
                        mig_table_info['tableLabels'] = table_labels_dict   
                        print(mig_table_info['tableLabels'])
            except: 
                print("No table labels")
                
            mig_dataset_info['tables'].append(mig_table_info)
            
    mig_dataset_json_output.append(mig_dataset_info)
    count +=1
    
mig_file_path = 'infra/resource/datasets_with_tables.json'


with open(mig_file_path, 'w') as file:
    json.dump(mig_dataset_json_output, file, indent=4)

#Tables.

# Clean table df
rename_tbl_cols ={
    'target_dataset 搬遷目標的資料庫 schema 名稱 *optional (only required for PostgreSQL)': 'dataset', 
    'target_table 搬遷目標的資料表名稱 *required': 'tablename',
    'en_column 搬遷目標的資料表欄位名稱 *required': 'name',
    'data_type 搬遷目標的資料表欄位的資料型別 *required': 'type',
    'null_acceptance 該欄位是否可接受空值 *required': 'mode',
    'ColumnDescription ': 'description'
}

mig_final_tbl_df = mig_table_df.copy()
mig_final_tbl_df.rename(columns=rename_tbl_cols, inplace=True)
mig_final_tbl_df
mig_tbl_drop_cols = ["target_column_key *auto-filled",
                     'source_system 搬遷目標的資料庫 instance 名稱 *required',
                     'source_db 搬遷目標的資料庫 databases 名稱 *required',
                     'cn_column 搬遷目標的資料表欄位中文名稱 *optional *optional',
                     'primary_key 該欄位是否為primary key *required',
                     'index 該欄位是否為index *required',
                     '個資 (PII) 該欄位是否為個資資訊 *required',
                     'Sensitive Data 該欄位是否為機敏資訊 *required',
                     'default_value 該欄位是否有預設值，若有則直接填入預設值，若無則免填 *optional',
                     '驗證 (Validation) 資料表是否存在於table_list *auto-filled'
                    ]
mig_final_tbl_df.drop(mig_tbl_drop_cols,axis='columns', inplace=True)

mig_table_grouped = mig_final_tbl_df.groupby(['dataset', 'tablename'])

for (dataset, table), group in mig_table_grouped:
    rem_list = ['description']
        # display(group["description"])
    prefix_path = "infra/tables/resource/schema"
    dir_path = os.path.join(prefix_path,dataset)
    print(dir_path)
    os.makedirs(dir_path, exist_ok=True)

    group = group.drop(columns=['dataset','tablename'])
    mig_table_json_output = group.to_dict(orient='records')
    mig_table_json_output = [clean_description(record) for record in group.to_dict(orient='records')]

    file_path = os.path.join(dir_path, f'{table}.json')
    print(file_path)

    with open(file_path, 'w') as file:
        json.dump(mig_table_json_output, file, indent=4)