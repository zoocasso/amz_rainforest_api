1. KISTI 제공 파일 아마존level4 기준 keyword로 search_api를 호출하여 json으로 저장
make_search_json.py

2. 1번에서 저장한 json을 파싱하여 json으로 저장
parsing_input_asin_dict.py

3. 2번에서 저장한 json을 input으로 하여 product_api와 review_api를 호출하여 json으로 저장
make_product_json.py
make_review_json.py

4. 3번에서 저장한 json을 파싱하여 저장 (main.py에서는 csv > DB insert로 변경됨)
product_json_db_insert.py
review_json_db_insert.py