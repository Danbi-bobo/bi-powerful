import sys
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.adapters.http.http_client import HttpClient
from queries import list_page
from datetime import datetime, timezone
import logging
import time

def get_page_statistics(df: pd.DataFrame):
    dfs = []
    start = int(datetime.now(timezone.utc).timestamp()) // 86400 * 86400
    end = int(datetime.now(timezone.utc).timestamp())

    for _, row in df.iterrows():
        page_id = row['page_id']
        page_access_token = row['page_access_token']

        params = {
            'page_access_token': page_access_token,
            'since': start,
            'until': end
        }

        try:
            res = HttpClient().get(
                url=f'https://pages.fm/api/public_api/v1/pages/{page_id}/statistics/pages',
                params=params
            )
            data = res.json().get('data', [])
            time.sleep(0.5)
            if not data:
                continue

            df_page = pd.DataFrame(data)
            df_page['page_id'] = page_id
            dfs.append(df_page)
        except Exception as e:
            logging.info(f'ERROR while get conversation of {page_id}: {e}')
            continue

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    return final_df

def transform(df:pd.DataFrame):
    df = df.copy()
    columns = ['page_id', 'date', 'hour', 'new_customer_count', 'customer_inbox_count', 'customer_comment_count', 'page_inbox_count', 'page_comment_count', 'phone_number_count', 'inbox_interactive_count', 'new_inbox_count', 'today_uniq_website_referral', 'today_website_guest_referral', 'uniq_phone_number_count']

    df['hour'] = pd.to_datetime(df['hour'], errors='coerce')
    df['date'] = df['hour'].dt.date
    df['hour'] = df['hour'].dt.hour
    
    return df[columns]

if __name__ == "__main__":
    setup_logger(__file__)
    
    pages = MariaDBHandler().read_from_db(query=list_page, output_type='dataframe')
    df = get_page_statistics(pages)
    final_df = transform(df)

    if not final_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_skyward_data',
            table='pancake_page_statistics',
            df=final_df,
            unique_columns=['page_id', 'date', 'hour']
        )