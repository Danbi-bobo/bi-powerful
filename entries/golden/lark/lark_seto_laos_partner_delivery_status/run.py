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
from mapping import mapping_dict
import logging
from cdp.domain.utils.udfs import lark_transform_mapping
from io import BytesIO, StringIO
import time
import re

def extract_data_from_file(file_url, access_token, file_type):
    url = file_url

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logging.info(f'successfully downloaded file with url {url}')
        if file_type == 'html':
            content = StringIO(response.content.decode('utf-8'))
            tables = pd.read_html(content)
            df = tables[0]
        elif file_type == 'xlsx':
            df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
        else:
            df = pd.DataFrame()
        return df

    else:
        raise Exception(f"Failed to download file: {response.status_code} - {response.text}")

def extract_and_transform_lark_data(data, mapping_dict, client:LarkApiHandle):
    dfs = []

    for row in data:
        fields = row['fields']

        for key, key_detail in mapping_dict.items():
            if key in fields:
                files = fields[key]
                if files:
                    for file in files:
                        file_url = file['url']
                        file_type = key_detail['file_type']
                        raw_df = extract_data_from_file(
                            file_url=file_url,
                            access_token=client.tenant_token,
                            file_type=file_type
                        )

                        if key == 'Anousith':
                            file_name = file['name']
                            match = re.search(r"\d{8}", file_name)
                            date_str = match.group()
                            raw_df['date_from_file_name'] = date_str

                        golden_df = lark_transform_mapping(
                            df=raw_df,
                            mapping_dict=key_detail['mapping_dict'],
                            sourced_from_lark=False
                        )
                        golden_df['partner_name'] = key.upper()
                        golden_df = golden_df[golden_df['bill_code'] != 'ລວມທັງໝົດ']

                        dfs.append(golden_df)
                        time.sleep(0.5)

        client.batch_edit(
            base_id='UNqJbEfmMaMI2EszppClfPfYgJf',
            table_id='tblEUIMFIi1dzNZX',
            batch_type='update',
            data=[
                {
                    'record_id': row['record_id'],
                    'fields': {
                        'handled': True
                    }
                }
            ]
        )
    
    if not dfs:
        return pd.DataFrame()
    
    return pd.concat(dfs, ignore_index=True)


if __name__ == '__main__':
    setup_logger(__file__)

    lark_client = LarkApiHandle()
    data = lark_client.list_records(
        base_id='UNqJbEfmMaMI2EszppClfPfYgJf',
        table_id='tblEUIMFIi1dzNZX',
        params={'filter': 'CurrentValue.[handled] = 0'}
    )

    df = extract_and_transform_lark_data(
        data=data,
        mapping_dict=mapping_dict,
        client=lark_client
    )

    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database='alomix_seto_data',
            table='lark_seto_laos_partner_delivery_status',
            df=df,
            unique_columns=['bill_code']
        )