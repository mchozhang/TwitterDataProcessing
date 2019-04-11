import geo_util
import json
from collections import Counter

result = Counter()

with open("melbGrid.json") as grid_file:
    json_object = json.load(grid_file)

    cell_list = []
    for cell in json_object.get('features'):
        properties = cell.get('properties')
        cell = dict()
        cell["name"] = properties.get('id')
        cell["xmin"] = properties.get('xmin')
        cell["xmax"] = properties.get('xmax')
        cell["ymin"] = properties.get('ymin')
        cell["ymax"] = properties.get('ymax')
        print(cell["name"])
        result[cell["name"]] = 0
        cell_list.append(cell)

with open("tinyTwitter.json") as twitter_file:
    twitter_file.readline()
    for line in twitter_file:
        line = line.strip()
        if line.endswith(","):
            line = line[:-1]

        try:
            tweet = json.loads(line)
            if tweet.get('doc').get('coordinates') is None:
                continue

            x = tweet.get('doc').get('coordinates').get('coordinates')[0]
            y = tweet.get('doc').get('coordinates').get('coordinates')[1]

            for cell in cell_list:
                if cell["xmin"] <= x <= cell["xmax"] and cell["ymin"] <= y <= cell["ymax"]:
                    result[cell["name"]] += 1

        except Exception as e:
            print("exception line:" + line)
            print(e)

    print(str(result))
