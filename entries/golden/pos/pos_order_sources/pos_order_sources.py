import sys
import os
import json
import pandas as pd
from dotenv import load_dotenv
from numpy import nan

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

db_golden_name = os.getenv("DB_GOLDEN_NAME")
golden_table_name = "pos_order_sources"
db_raw_name = os.getenv("DB_RAW_NAME")
raw_table_name = "raw_" + golden_table_name

new_rows = pd.DataFrame({
    "shop_id": ['*', '*', '*'],
    "id": ['-403', '-404', '-405'],
    "name": [
        'Đơn hàng không có nguồn đơn',
        'Đã có dòng trên bảng UTM nhưng chưa chọn nguồn đơn',
        'Page chạy quảng cáo chưa được link vào POS'
    ]
})

numeric_cols = []
string_cols = ['shop_id', 'name', 'custom_id', 'id', 'link_source_id', 'parent_id', 'project_id']
datetime_cols = ['inserted_at', 'updated_at']
golden_cols = numeric_cols + string_cols + datetime_cols

def load_config_shops():
    config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
    with open(config_file, 'r') as file:
        return json.load(file).get('shops', [])

def get_order_sources_from_api():
    shops = load_config_shops()

    for shop in shops:
        shop_id = shop['shop_id']
        api_key = shop['api_key']
        result = []
        if not (shop_id and api_key):
            return result
        pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
        order_sources_from_api = pos_handler.get_all(endpoint='order_source')
        result.extend(order_sources_from_api)
    
    return result

def prepare_raw_df(order_sources_list):
    raw_df = pd.DataFrame(order_sources_list)
    raw_df = raw_df[golden_cols]
    return raw_df

def prepare_golden_df(raw_df):
    df = raw_df.copy()
    df = pd.concat([df, new_rows], ignore_index=True)
    id_cols = ['parent_id', 'id', 'link_source_id']
    df.replace(nan, None, inplace=True)
    df[id_cols] = df[id_cols].apply(lambda col: col.map(lambda x: str(x).split('.')[0]))
    df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, errors='coerce')
    df[string_cols] = df[string_cols].astype(str)
    df.replace('None', None, inplace=True)
    return df

def get_order_sources():
    order_sources = get_order_sources_from_api()
    raw_df = prepare_raw_df(order_sources)
    golden_df = prepare_golden_df(raw_df)
    return golden_df

if __name__ == "__main__":
    setup_logger(__file__)
    
    order_sources = get_order_sources_from_api()
    raw_df = prepare_raw_df(order_sources)
    golden_df = prepare_golden_df(raw_df)

    # if not raw_df.empty:
    #     raw_df = raw_df.astype(str)
    #     MariaDBHandler().insert_and_update_from_df(db_raw_name, raw_table_name, raw_df, unique_columns=["id", "shop_id"])
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(db_golden_name, golden_table_name, golden_df, unique_columns=["id", "shop_id"], log=True, updated_flag=True)
