import csv
import sys
import yaml

x = sys.version_info

with open('ac.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    i = 0
    result = {}
    for row in spamreader:
        if i > 0 and len(row)>0:
            result[row[1]] = {"enterprise_id": int(row[0]), "access_code": row[3]}
        i = i + 1
    with open('data.yml', 'w') as outfile:
        yaml.dump(result, outfile, default_flow_style=False)
