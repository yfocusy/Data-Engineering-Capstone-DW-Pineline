import os
import json

def get_lookup_dict(filepath, keyType, valueType, keyIndex1, keyIndex2, valueIndex1,valueIndex2):
    output_dict = {}
    with open(filepath,"r") as file:
        for line in file:
            line = line.strip()
            output_dict[keyType(line[keyIndex1:keyIndex2])] = valueType(line[valueIndex1:valueIndex2]).strip()
    return output_dict


def get_formatted_data_list(input_dict, label1,label2):
    output_list = []
    for key in input_dict:
        tmp_dict = {}
        tmp_dict[label1]=key
        tmp_dict[label2]=input_dict[key]
        output_list.append(tmp_dict)
    return output_list

def save_as_formatted_JSON_file(filepath, write, data):
    with open(filepath,write) as file:
        for d in data:
            j = json.dumps(d)
            file.write(j+'\n')


if __name__ == '__main__':
    mode_file_path = '.././data/lookups/1.mode.json'
    if not os.path.exists(mode_file_path):
        mode_dict = get_lookup_dict(".././data/lookups/mode.txt", int, str, 0, 1, 5, -1)
        mode_data = get_formatted_data_list(mode_dict, "mode_id", "mode_name")
        save_as_formatted_JSON_file(".././data/lookups/1.mode.json", "w", mode_data)


