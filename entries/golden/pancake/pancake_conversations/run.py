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
from datetime import datetime, timezone
import json
import logging
import time

def get_page_conversations(df: pd.DataFrame):
    dfs = []
    start = load_last_run()
    end = int(datetime.now(timezone.utc).timestamp())

    for _, row in df.iterrows():
        page_id = row['page_id']
        page_access_token = row['page_access_token']

        params = {
            'page_access_token': page_access_token,
            'since': start,
            'until': end
        }

        while True:
            try:
                res = HttpClient().get(
                    url=f'https://pages.fm/api/public_api/v2/pages/{page_id}/conversations',
                    params=params
                )
                data = res.json().get('conversations', [])
                time.sleep(0.5)
                if not data:
                    break

                df_page = pd.DataFrame(data)
                dfs.append(df_page)

                last_conversation = data[-1]
                params['last_conversation_id'] = last_conversation['id']
            except Exception as e:
                logging.info(f'ERROR while get conversation of {page_id}: {e}')
                break

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    save_last_run(end)
    return final_df

def load_last_run(path='lastrun.json'):
    now_ts = int(datetime.now(timezone.utc).timestamp())

    if not os.path.exists(path):
        last_run = now_ts
        with open(path, 'w') as f:
            json.dump({'last_run': last_run}, f)
        return now_ts - 15 * 60

    try:
        with open(path, 'r') as f:
            data = json.load(f)
            last_run = data.get('last_run')
            if last_run is None:
                return now_ts - 15 * 60
            return last_run - 15 * 60
    except Exception:
        return now_ts - 15 * 60
    
def save_last_run(timestamp, path='lastrun.json'):
    with open(path, 'w') as f:
        json.dump({'last_run': int(timestamp)}, f)

def transform(df:pd.DataFrame):
    df = df.copy()
    final_df = pd.DataFrame()
    columns = {
        'string': ['id', 'type', 'page_id', 'post_id', 'recent_phone_numbers'],
        'numeric': ['message_count'],
        'datetime': ['inserted_at', 'updated_at'],
        'bool': ['has_phone', 'seen', ]
    }
    final_df[columns['datetime']] = df[columns['datetime']].apply(pd.to_datetime, errors='coerce')
    final_df[columns['string']] = df[columns['string']].astype(str)
    final_df[columns['numeric']] = df[columns['numeric']].apply(pd.to_numeric, errors='coerce')
    final_df[columns['bool']] = df[columns['bool']].astype(bool)

    final_df['tags_id'] = df['tags'].apply(
        lambda x: json.dumps(
            [tag.get('id') for tag in x if isinstance(tag, dict)]
        ) if isinstance(x, list) else None
    )
    final_df['customer'] = df['page_customer'].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
    )
    final_df['current_assign_users'] = df['current_assign_users'].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
    )

    final_df.replace([nan, "None"], None, inplace=True)

    return final_df

if __name__ == "__main__":
    setup_logger(__file__)
    
    pages = MariaDBHandler().read_from_db(query=list_page, output_type='dataframe')
    conversations = get_page_conversations(pages)
    final_df = transform(conversations)
    
    if not final_df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_skyward_data',
            table='pancake_conversations',
            df=final_df,
            unique_columns=['id']
        )
