import sys
import os
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
from cdp.adapters.http.http_client import HttpClient
from queries import query

def get_page_access_token(df):
    df = df.copy()
    tokens = []

    for _, row in df.iterrows():
        page_id = row['page_id']
        access_token = row['user_access_token']

        try:
            res = HttpClient().post(
                url=f'https://pages.fm/api/v1/pages/{page_id}/generate_page_access_token',
                params={
                    'page_id': page_id,
                    'access_token': access_token
                },
                data=None
            )
            page_access_token = res.json().get('page_access_token')
        except Exception:
            page_access_token = None

        tokens.append(page_access_token)

    df["page_access_token"] = tokens
    return df[['page_id', 'page_access_token']]


if __name__ == "__main__":
    setup_logger(__file__)

    lark_client = LarkApiHandle()
    pancake_tokens = lark_client.get_pancake_tokens_in_lark()
    df = MariaDBHandler().read_from_db(query=query, output_type='dataframe')
    df["user_access_token"] = df["department"].map(pancake_tokens)
    df = get_page_access_token(df)
    
    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_skyward_data',
            table='pancake_pages',
            df=df,
            unique_columns=['page_id']
        )