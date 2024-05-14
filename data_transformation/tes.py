import os
import json

json_path = '../data/'
json_files = [f for f in os.listdir(json_path)]

for json_file_name in json_files:
    with open(os.path.join(json_path, json_file_name)) as json_file:
        raw = json.load(json_file)
        print(raw)
