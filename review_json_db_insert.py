import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine
import pymysql
import pandas as pd
import json
import re

DATABASE_CONFIG = json.loads(os.getenv("DATABASE_CONFIG"))
db_connection_str = f"mysql+pymysql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}/{DATABASE_CONFIG['dbname']}"
db_connection = create_engine(db_connection_str)

directory_list = os.listdir('./result/review_json')
for directory in directory_list:
    review_list = os.listdir(f'./result/review_json/{directory}')
    directory_review_list = list()
    for product in review_list:
        with open(f'./result/review_json/{directory}/{product}') as f:
            review_json = json.loads(f.read())
        try:
            review_summary = review_json["summary"]
        except:
            pass
        try:
            reviews = review_json["reviews"]
        except:
            pass
        for review in reviews:
            review_dict = dict()
            review_dict['keyword'] = directory
            review_dict['asin_code'] = product.split('.')[0]
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
            review_dict['id'] = review['id']
            review_dict['title'] = review['title']
            try:
                review_dict['body'] = review['body']
            except:
                pass
            # review_dict['body_html'] = review['body_html']
            try:
                review_dict['link'] = review['link']
            except:
                pass
            review_dict['rating'] = review['rating']
            review_dict['date'] = review['date']['utc']
            try:
                review_dict['helpful_votes'] = review['helpful_votes']
            except:
                pass
            review_dict['review_country'] = review['review_country']
            review_dict['position'] = review['position']
            directory_review_list.append(review_dict)

    review_df = pd.DataFrame(directory_review_list)
    # review_df.to_csv(f"./result/review_csv/{directory}.csv")
    review_df.to_sql(name='review', con=db_connection, if_exists='append', index=False)