import sys
import os
import pandas as pd
from dotenv import load_dotenv
from numpy import nan

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.adapters.http.http_client import HttpClient
from queries import list_page
import time

def get_page_tags(df: pd.DataFrame):
    dfs = []

    for _, row in df.iterrows():
        page_id = row['page_id']
        page_access_token = row['page_access_token']
        res = HttpClient().get(
            url=f'https://pages.fm/api/public_api/v1/pages/{page_id}/tags',
            params={
                'page_access_token': page_access_token
            }
        )
        data = res.json().get('tags', [])
        df = pd.DataFrame(data)
        df['page_id'] = page_id
        dfs.append(df)
        time.sleep(0.5)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def transform(df:pd.DataFrame):
    df = df.copy()
    columns = ['page_id', 'tag_id', 'tag_name']
    df.rename(
        columns={
            'id': 'tag_id',
            'text': 'tag_name'
        },
        inplace=True
    )
    df.replace([nan, "None"], None, inplace=True)
    return df[columns]

if __name__ == "__main__":
    setup_logger(__file__)

    pages = MariaDBHandler().read_from_db(query=list_page, output_type='dataframe')
    tags = get_page_tags(pages)
    final_df = transform(tags)

    if not final_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_skyward_data',
            table='pancake_page_tags',
            df=final_df,
            unique_columns=['page_id', 'tag_id']
        )