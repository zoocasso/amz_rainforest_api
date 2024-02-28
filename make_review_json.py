import os
from dotenv import load_dotenv
load_dotenv()

from tqdm import tqdm
import requests
import json

url = "https://api.rainforestapi.com/request"

dir_list = os.listdir("input")
for i in dir_list:
    os.mkdir(f"result/review_json/{i.split('.')[0]}")
    with open(f'./input/{i}','r') as f:
        input_dict = json.loads(f.read())
    for search_keyword, asin_code_list in input_dict.items():
        for asin_code in tqdm(asin_code_list):
            # set up the request parameters
            params = {
                'api_key': os.getenv("RAINFOREST_KEY"),
                'type': 'reviews',
                'amazon_domain': 'amazon.com',
                'asin': asin_code,
                'customer_zipcode': '98101',
                'output': 'json',
                'include_html': 'false',
                'page':'1',
                'max_page':'5'
            }

            # make the http GET request to Rainforest API
            api_result = requests.get(url, params)

            # print the JSON response from Rainforest API
            api_result_json = api_result.json()

            with open(f"./result/review_json/{search_keyword}/{asin_code}.json",'w') as f:
                json.dump(api_result_json, f)