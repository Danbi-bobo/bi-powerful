import sys
import os
import pandas as pd
from dotenv import load_dotenv
from numpy import nan

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.adapters.http.http_client import HttpClient
from cdp.domain.utils.udfs import lark_transform_mapping

mapping_dict = {
    'page_id': {'path': 'id', 'type': 'str'},
    'page_name': {'path': 'name', 'type': 'str'},
    'platform': {'path': 'platform', 'type': 'str'},
    'shop_id': {'path': 'shop_id', 'type': 'str'},
    'timezone': {'path': 'timezone', 'type': 'int'},
    'role_in_page': {'path': 'role_in_page', 'type': 'str'},
    'is_activated': {'path': 'is_activated', 'type': 'bool_int'},
    'tag_sync_group_id': {'path': 'tag_sync_group_id', 'type': 'str'},
    'department': {'path': 'department', 'type': 'str'}
}

def get_pancake_pages(department, token):
    dfs = []
    categorized = ['activated', 'inactivated']

    url = 'https://pages.fm/api/v1/pages'
    params = {
        'access_token': token
    }

    res = HttpClient().get(url, params=params)

    data = res.json().get('categorized', {})
    for category in categorized:
        category_data = data.get(category, [])
        df = pd.DataFrame(category_data)
        df['department'] = department
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def get_all_pancake_pages(token_dict):
    dfs = []
    for department, token in token_dict.items():
        df = get_pancake_pages(department=department, token=token)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

if __name__ == "__main__":
    setup_logger(__file__)

    lark_client = LarkApiHandle()
    pancake_tokens = lark_client.get_pancake_tokens_in_lark()

    df = get_all_pancake_pages(pancake_tokens)
    final_df = lark_transform_mapping(df, mapping_dict, sourced_from_lark=False)
    final_df.replace([nan, "None"], None, inplace=True)
    final_df.drop_duplicates(subset='page_id', keep='first', inplace=True)

    if not final_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_skyward_data',
            table='pancake_pages',
            df=final_df,
            unique_columns=['page_id']
        )