import json
import os
import re
from pprint import pprint
from simple_ddl_parser import parse_from_file

base_directory = "../tmp/output"
raw_input_path = "../tmp/input/tbl.sql"
processed_input_path = "../tmp/output/processed.sql"


def cleansing():
    with open(raw_input_path, 'r', encoding="utf-16") as file:
        sql_script = file.read()

    with open(processed_input_path, "w") as output_file:
        create_table_statements = re.findall(r'CREATE TABLE \[.*?\]\n\((?:\n|.)*?\)\n', sql_script, flags=re.DOTALL)

        # Print the filtered CREATE TABLE statements
        for statement in create_table_statements:
            output_data = f'{statement}\n'
            output_file.write(output_data)


def convert():
    result = parse_from_file(processed_input_path)
    pprint(result)

    for element in result:
        dataset_name = element.get("schema").strip("[]")
        table_name = element.get("table_name").strip("[]")

        output = list()
        for column in element.get("columns"):
            tmp_dict = dict()

            schema_type = column.get("type").strip("[]")

            match schema_type:
                case "nvarchar":
                    convert_type = "STRING"
                case "char":
                    convert_type = "STRING"
                case "nchar":
                    convert_type = "STRING"
                case "ntext":
                    convert_type = "STRING"
                case "text":
                    convert_type = "STRING"  
                case "uniqueidentifier":
                    convert_type = "STRING"
                case "varchar":
                    convert_type = "STRING"
                case "xml":
                    convert_type = "STRING"      
                case "int":
                    convert_type = "INTEGER"
                case "bit":
                    convert_type = "INTEGER"
                case "bigint":
                    convert_type = "INTEGER"
                case "smallint":
                    convert_type = "INTEGER"
                case "tinyint":
                    convert_type = "INTEGER"
                case "money":
                    convert_type = "NUMERIC"
                case "numeric":
                    convert_type = "NUMERIC"
                case "smallmoney":
                    convert_type = "NUMERIC"
                case "decimal":
                    convert_type = "NUMERIC"
                case "float":
                    convert_type = "FLOAT"
                case "real":
                    convert_type = "FLOAT"
                case "date":
                    convert_type = "DATE"
                case "time":
                    convert_type = "TIME"
                case "datetime":
                    convert_type = "TIMESTAMP"
                case "datetime2":
                    convert_type = "TIMESTAMP"
                case "datetimeoffset":
                    convert_type = "DATETIME"
                case "smalldatetime":
                    convert_type = "DATETIME"
                case "timestamp":
                    convert_type = "TIMESTAMP"
                case "binary":
                    convert_type = "BYTES"
                case "image":
                    convert_type = "BYTES"
                case "varbinary":
                    convert_type = "BYTES"
                case "bit":
                    convert_type = "BOOL"
                case _:
                    convert_type = "STRING"

            tmp_dict["mode"] = "NULLABLE" if column.get("nullable") is True else "Required"
            tmp_dict["name"] = column.get("name").strip("[]")
            tmp_dict["type"] = convert_type
            if convert_type == "NUMERIC":
                tmp_dict["precision"] = str(column.get("size")[0])
                tmp_dict["scale"] = str(column.get("size")[1])

            output.append(tmp_dict)

        file_path = f"../tmp/output/{dataset_name}/{table_name}.json"
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w") as output_file:
            # output_file.write(json.dumps(output))
            json.dump(output, output_file, indent=4)

        print(f'dataset name: {dataset_name}')
        print(f'table name: {table_name}')
        print("------------")


def generate_map():
    subdirectories = [subdir for subdir in os.listdir(base_directory) if
                      os.path.isdir(os.path.join(base_directory, subdir))]

    for subdir in subdirectories:
        subdir_path = os.path.join(base_directory, subdir)
        print(f"Files in directory {subdir_path}:")

        files = [file for file in os.listdir(subdir_path) if os.path.isfile(os.path.join(subdir_path, file))]

        out_data = []
        for file_name in files:
            table_name = file_name.split(".json")[0]
            tmp_dict = dict()
            tmp_dict["table_id"] = table_name
            out_data.append(tmp_dict)

        file_path = f"{subdir_path}/map/map.json"
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w") as output_file:
            json.dump(out_data, output_file, indent=4)

        print("----")


if __name__ == '__main__':
    cleansing()
    convert()
    generate_map()