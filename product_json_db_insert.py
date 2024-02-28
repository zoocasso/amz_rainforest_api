import os
from dotenv import load_dotenv
load_dotenv()

import re
import json
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

now = datetime.now()

DATABASE_CONFIG = json.loads(os.getenv("DATABASE_CONFIG"))
db_connection_str = f"mysql+pymysql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}/{DATABASE_CONFIG['dbname']}"
db_connection = create_engine(db_connection_str)

directory_list = os.listdir('./result/product_json')
for directory in directory_list:
    product_list = os.listdir(f'./result/product_json/{directory}')
    product_dict_list = list()
    keyword_dict_list = list()
    for product in product_list:
        attributes_list = list()
        specifications_list = list()
        with open(f'./result/product_json/{directory}/{product}') as f:
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