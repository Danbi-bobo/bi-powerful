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
import pandas as pd
import requests
from io import StringIO
from mapping import mapping_dict, mapping_lark_fields
import numpy as np


def extract_csv_from_lark(file_url, access_token, skiprows=0):
    url = file_url

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Giải mã nội dung CSV từ byte -> string
        csv_content = response.content.decode("utf-8")

        # Đọc nội dung CSV vào DataFrame
        df = pd.read_csv(StringIO(csv_content), skiprows=skiprows)

        return df
    else:
        raise Exception(f"Failed to download file: {response.status_code} - {response.text}")

def handle_df(file_url, token, mapping_dict_key, skiprows, market_key):
    df = extract_csv_from_lark(file_url=file_url, access_token=token, skiprows=skiprows)
    data = mapping_dict[mapping_dict_key]
    df.rename(columns=data['rename_dict'], inplace=True)

    string_cols = data['columns']['string']
    numeric_cols = data['columns']['numeric']
    datetime_cols = data['columns']['datetime']

    df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, errors='coerce').apply(lambda x: x.dt.tz_localize(None))
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df[string_cols] = df[string_cols].astype(str).apply(lambda x: x.str.strip())

    return_cols = string_cols + numeric_cols + datetime_cols
    df = df[return_cols]

    if mapping_dict_key == 'card_withdrawal':
        handle_card_withdrawal(df)
    elif mapping_dict_key == 'card_transaction':
        handle_card_transaction(df)
    elif mapping_dict_key == 'account_transaction':
        handle_account_transaction(df)
    elif mapping_dict_key == 'global_account_transaction':
        handle_global_account_transaction(df, market_key)

    if 'card_number' in df.columns and 'last4' not in df.columns:
        df['last4'] = df['card_number'].str[-4:]

    df.replace([np.nan, 'None', 'nan'], None, inplace = True)

    return df

def handle_account_transaction(df):
    df['transaction_time'] = df['transaction_time'] - pd.to_timedelta(1, unit='h')

def handle_card_transaction(df):
    df['transaction_type'] = 'Payment'
    df['decline_fee'] = df['transaction_fee'].apply(
        lambda x: float(x.split(':')[1]) if x.startswith('Decline Fee') else None
    )

def handle_card_withdrawal(df):
    df['currency'] = 'USD'
    df['transaction_type'] = 'Deposit/Withdrawal'

def extract_numeric(df, columns):
    for col in columns:
        df[col.lower()] = (
            df[col]
            .fillna('')
            .astype(str)
            .str.extract(r'([-\d\.]+)')[0]
            .astype(float)
        )
    return df

def handle_global_account_transaction(df, market_key):
    df = extract_numeric(df, ['fee', 'net'])
    key = market_key.split()[-1]
    if key.lower() == 'topas':
        df['market'] = 'TOPAS'
    elif key.lower() == 'phil':
        df['market'] = 'SETO PHIL'

if __name__ == '__main__':
    setup_logger(__file__)

    lark_client = LarkApiHandle()
    data = lark_client.list_records(
        base_id= 'JkBKbSSPQapddtshUdjle7ylg2g',
        table_id='tblnuizBZk49spZH',
        params={
            'view_id': 'vewrAYvSw0'
        }
    )

    row = data[0]['fields']

    for key, value in mapping_lark_fields.items():
        if row.get(key):
            df = handle_df(
                file_url=row[key][0]['url'],
                token=lark_client.tenant_token,
                mapping_dict_key=value, 
                skiprows = 0 if value != 'global_account_transaction' else 10,
                market_key=key
            )

            table_name = mapping_dict[value]['table_name']
            if table_name == 'pingpong_global_account_transactions':
                unique_columns = ['transaction_id', 'market']
            else:
                unique_columns=['transaction_id']
            
            MariaDBHandler().insert_and_update_from_df(
                database='alomix_skyward_data',
                table=table_name,
                df=df,
                unique_columns=unique_columns
            )
