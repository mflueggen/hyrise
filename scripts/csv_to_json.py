import csv
import sys
import json

if __name__ == '__main__':
    # argv[1] := type
    # argv[2] := csv file
    # argv[3] := json file
    with open(sys.argv[2]) as csvfile:
        json_data = {}
        json_data["type"] = sys.argv[1]
        json_data["memory_segments"] = []
        csvreader = csv.reader(csvfile, delimiter=";")
        next(csvreader)
        for row in csvreader:
            table_name = row[0]
            column_name = row[1]
            chunk_id = int(row[2])
            json_data["memory_segments"].append(
                {"table_name": table_name, "column_name": column_name, "chunk_id": chunk_id})

    with open(sys.argv[3], 'w') as outfile:
        json.dump(json_data, outfile, indent=2)
