import os
from dotenv import load_dotenv
load_dotenv()

from tqdm import tqdm
import requests
import json

url = "https://api.rainforestapi.com/request"

with open("input.txt",'r', encoding='UTF8') as f:
    search_term_list = f.read().splitlines()

for search_term in tqdm(search_term_list):
    # set up the request parameters
    params = {
        'api_key':os.getenv("RAINFOREST_KEY"),
        'type':'search',
        'amazon_domain':'amazon.com',
        'search_term': search_term,
        'customer_zipcode': '98101',
        'page': '1',
        'max_page': '5',
        'sort_by': 'average_review',
        'output': 'json'
    }

    # make the http GET request to Rainforest API
    api_result = requests.get(url, params)

    # print the JSON response from Rainforest API
    api_result_json = api_result.json()
    with open(f"./result/search_json/{search_term}.json",'w') as f:
        json.dump(api_result_json, f)