import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine
import pandas as pd
import requests
import json
import re
from datetime import datetime
from tqdm import tqdm


DATABASE_CONFIG = json.loads(os.getenv("DATABASE_CONFIG"))
db_connection_str = f"mysql+pymysql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}/{DATABASE_CONFIG['dbname']}"
db_connection = create_engine(db_connection_str)

def make_search_json(url,search_term,output_folder):
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
    with open(f"./{output_folder}/{search_term}.json",'w',encoding="UTF8") as f:
        json.dump(api_result_json, f)

def parsing_input_asin_dict(input_folder_name,search_term,output_folder_name):
    result_dict = dict()
    with open (f'./{input_folder_name}/{search_term}','r',encoding="UTF8") as f:
        json_data = json.load(f)
    search_keyword = search_term.split(".")[0]

    asin_list = list()
    for search_results in json_data['search_results']:
        asin_list.append(search_results['asin'])

    result_dict[search_keyword] = asin_list[:80]


    with open(f"./{output_folder_name}/{search_term}",'a',encoding="UTF8") as f:
        json.dump(result_dict, f)

def make_product_json(input_folder_name,url,output_folder_name):
    dir_list = os.listdir(input_folder_name)
    for i in dir_list:
        # os.mkdir(f"./{output_folder_name}/{i.split('.')[0]}")
        with open(f'./{input_folder_name}/{i}','r',encoding="UTF8") as f:
            input_dict = json.loads(f.read())

        for search_keyword, asin_code_list in input_dict.items():
            for asin_code in tqdm(asin_code_list):
                # set up the request parameters
                params = {
                    'api_key': os.getenv("RAINFOREST_KEY"),
                    'amazon_domain': 'amazon.com',
                    'asin': asin_code,
                    'type': 'product',
                    'customer_zipcode': '98101',
                    'output': 'json'
                }

                # make the http GET request to Rainforest API
                api_result = requests.get(url, params)

                # print the JSON response from Rainforest API
                api_result_json = api_result.json()

                with open(f"./{output_folder_name}/{search_keyword}/{asin_code}.json",'w',encoding="UTF8") as f:
                    json.dump(api_result_json, f)

def make_review_json(input_folder_name,url,output_folder_name):
    dir_list = os.listdir(input_folder_name)
    for i in dir_list:
        os.mkdir(f"./{output_folder_name}/{i.split('.')[0]}")
        with open(f'./{input_folder_name}/{i}','r',encoding="UTF8") as f:
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

                with open(f"./{output_folder_name}/{search_keyword}/{asin_code}.json",'w',encoding="UTF8") as f:
                    json.dump(api_result_json, f)

def product_json_db_insert(product_json_folder):
    now = datetime.now()
    directory_list = os.listdir(f'./{product_json_folder}')
    for directory in directory_list:
        product_list = os.listdir(f'./{product_json_folder}/{directory}')
        product_dict_list = list()
        keyword_dict_list = list()
        for product in product_list:
            attributes_list = list()
            specifications_list = list()
            with open(f'./{product_json_folder}/{directory}/{product}') as f:
                product_json = json.loads(f.read())
            product_json_product = product_json["product"]

            keyword_dict = dict()
            keyword_dict['keyword'] = directory
            keyword_dict['asin'] = product_json_product['asin']
            keyword_dict['insert_time'] = now.strftime('%Y-%m-%d %H:%M:%S')
            keyword_dict_list.append(keyword_dict)

            product_dict = dict()
            product_dict['keyword'] = directory
            product_dict['asin'] = product_json_product['asin']
            product_dict['title'] = product_json_product['title']
            category_list = list()
            for i in product_json_product['categories']:
                category_list.append(i['name'])
            product_dict['categories'] = str(category_list)
            try:
                product_dict['brand'] = product_json_product['brand']
            except:
                pass
            try:
                product_dict['rating'] = product_json_product['rating']
            except:
                pass
            try:
                product_dict['ratings_total'] = product_json_product['ratings_total']
            except:
                pass
            try:
                product_dict['five_star'] = product_json_product['rating_breakdown']['five_star']['count']
            except:
                pass
            try:
                product_dict['four_star'] = product_json_product['rating_breakdown']['four_star']['count']
            except:
                pass
            try:
                product_dict['three_star'] = product_json_product['rating_breakdown']['three_star']['count']
            except:
                pass
            try:
                product_dict['two_star'] = product_json_product['rating_breakdown']['two_star']['count']
            except:
                pass
            try:
                product_dict['one_star'] = product_json_product['rating_breakdown']['one_star']['count']
            except:
                pass
            try:
                product_dict['feature_bullets'] = ' '.join(re.sub(r"[^a-zA-Z\s]","",str(product_json_product['feature_bullets'])).split())
            except:
                pass
            try:
                product_dict['description'] = product_json_product['description']
            except:
                pass
            try:
                for index in product_json_product['attributes']:
                    attributes_dict = dict()
                    attributes_dict['keyword'] = directory
                    attributes_dict['asin'] = product_json_product['asin']
                    attributes_dict['name'] = index['name']
                    attributes_dict['value'] = index['value']
                    attributes_list.append(attributes_dict)
                attributes_df = pd.DataFrame(attributes_list)
                attributes_df.to_sql(name='attributes', con=db_connection, if_exists='append', index=False)
            except:
                pass
            
            try:
                for index in product_json_product['specifications']:
                    specifications_dict = dict()
                    specifications_dict['keyword'] = directory
                    specifications_dict['asin'] = product_json_product['asin']
                    specifications_dict['name'] = index['name']
                    specifications_dict['value'] = index['value']
                    specifications_list.append(specifications_dict)
                specifications_df = pd.DataFrame(specifications_list)
                specifications_df.to_sql(name='specifications', con=db_connection, if_exists='append', index=False)
            except:
                pass
            product_dict_list.append(product_dict)

        keyword_df = pd.DataFrame(keyword_dict_list)
        keyword_df.to_sql(name='keyword', con=db_connection, if_exists='append', index=False)

        product_df = pd.DataFrame(product_dict_list)
        product_df.to_sql(name='product', con=db_connection, if_exists='append', index=False)

def review_json_db_insert(review_json_folder):
    directory_list = os.listdir(f'./{review_json_folder}')
    for directory in directory_list:
        review_list = os.listdir(f'./{review_json_folder}/{directory}')
        directory_review_list = list()
        for product in review_list:
            with open(f'./{review_json_folder}/{directory}/{product}') as f:
                review_json = json.loads(f.read())
            try:
                review_summary = review_json["summary"]
            except:
                pass

            reviews = review_json["reviews"]
            
            for review in reviews:
                review_dict = dict()
                try:
                    review_dict['keyword'] = directory
                except:
                    pass
                try:
                    review_dict['asin'] = product.split('.')[0]
                except:
                    pass
                try:
                    review_dict['rating'] = review_summary['rating']
                except:
                    pass
                try:
                    review_dict['ratings_total'] = review_summary['ratings_total']
                except:
                    pass
                try:
                    review_dict['reviews_total'] = review_summary['reviews_total']
                except:
                    pass
                try:
                    review_dict['id'] = review['id']
                except:
                    pass
                try:
                    review_dict['title'] = review['title']
                except:
                    pass
                try:
                    review_dict['body'] = review['body']
                except:
                    pass
                try:
                    review_dict['link'] = review['link']
                except:
                    pass
                try:
                    review_dict['rating'] = review['rating']
                except:
                    pass
                try:    
                    review_dict['date'] = review['date']['utc']
                except:
                    pass
                try:
                    review_dict['helpful_votes'] = review['helpful_votes']
                except:
                    pass
                try:
                    review_dict['review_country'] = review['review_country']
                except:
                    pass
                try:
                    review_dict['position'] = review['position']
                except:
                    pass
                try:
                    directory_review_list.append(review_dict)
                except:
                    pass

        review_df = pd.DataFrame(directory_review_list)
        # review_df.to_csv(f"./result/review_csv/{directory}.csv")
        review_df.to_sql(name='review', con=db_connection, if_exists='append', index=False)

if __name__ == "__main__":
    url = "https://api.rainforestapi.com/request"
    input_folder_name = 'keyword'
    search_json_folder = 'result/search_json'
    product_json_folder = 'result/product_json'
    review_json_folder = 'result/review_json'

    with open("input.txt",'r',encoding="UTF8") as f:
        search_term_list = f.read().splitlines()
        for search_term in tqdm(search_term_list):
            make_search_json(url,search_term,search_json_folder)

    for search_term in os.listdir(f'./{search_json_folder}'):
        parsing_input_asin_dict(search_json_folder,search_term,input_folder_name)

    make_product_json(input_folder_name,url,product_json_folder)
    product_json_db_insert(product_json_folder)

    make_review_json(input_folder_name,url,review_json_folder)
    review_json_db_insert(review_json_folder)