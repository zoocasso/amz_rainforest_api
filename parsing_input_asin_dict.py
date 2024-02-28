import os
import json

for search_term in os.listdir('./result/search_json'):
    result_dict = dict()
    with open (f'./result/search_json/{search_term}','r') as f:
        json_data = json.load(f)
    search_keyword = search_term.split(".")[0]

    asin_list = list()
    for search_results in json_data['search_results']:
        asin_list.append(search_results['asin'])

    result_dict[search_keyword] = asin_list[:80]


    with open(f"./input/{search_term}",'a') as f:
        json.dump(result_dict, f)