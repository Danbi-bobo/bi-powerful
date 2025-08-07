import sys
import os
import time
import pandas as pd
import numpy as np
import logging
import re
import json
from typing import Literal, List, Dict, Optional
from httpx import URL
from cdp.domain.utils.udfs import lark_transform_mapping
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.http.http_client import HttpClient
from cdp.domain.utils.list import split_list

class LarkApiHandle():
    def __init__(self, app_id = None, app_secret = None):
        self.app_id = app_id or os.getenv("LARK_APP_ID")
        self.app_secret = app_secret or os.getenv("LARK_APP_SECRET")
        self.fb_token_base_id = os.getenv("LARK_FB_TOKEN_BASE")
        self.fb_token_table_id = os.getenv("LARK_FB_TOKEN_TABLE")
        self.tenant_token = None
        self.token_expiry = 0

    def get_tenant_token(self, app_id=None, app_secret=None, max_retries=3) -> str:
        app_id = app_id or self.app_id
        app_secret = app_secret or self.app_secret

        if not app_id or not app_secret:
            raise Exception("APP_ID and APP_SECRET are required")
        
        if self.tenant_token and time.time() < self.token_expiry:
            return self.tenant_token
        
        attempt = 0
        while attempt < max_retries:
            try:
                lark_token_res = HttpClient().post(
                    url="https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
                    data={"app_id": app_id, "app_secret": app_secret},
                )
                print({"app_id": app_id, "app_secret": app_secret})
                if lark_token_res.status_code != 200:
                    raise Exception(f"Error while getting lark token {lark_token_res.text}")
                token_data = lark_token_res.json()
                self.tenant_token = token_data["tenant_access_token"]
                self.token_expiry = time.time() + token_data.get("expire", 3600) - 60  # Trừ 60s tránh hết hạn đột ngột
                return self.tenant_token
            
            except Exception as e:
                attempt += 1
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def list_records(self, base_id, table_id, return_type: Literal["full", "record_id_only"] = "full", params = None, log = False):
        all_items = []
        current_params = params.copy() if isinstance(params, dict) else {}
        current_params['user_id_type'] = 'user_id'
        api_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records"
        current_url = URL(api_url)
        token = self.get_tenant_token()
        while True:
            res = HttpClient().get(current_url, headers={"Authorization": f"Bearer {token}"}, params= current_params)
            res.raise_for_status()

            data = res.json().get('data', {})       

            if log == True:
                logging.info(data)

            if "items" in data and data.get("items"):
                if return_type == "full":
                    all_items.extend([record for record in data.get("items")])
                elif return_type == "record_id_only":
                    all_items.extend([record['record_id'] for record in data.get("items")])

            has_more = data.get("has_more", False)
            if not has_more:
                break
            else:
                page_token = data.get("page_token", None)
                if page_token:
                    current_params['page_token'] = page_token

            if not page_token:
                break
        
            time.sleep(1)
        return all_items
    
    def batch_edit(self, base_id, table_id, data, batch_type: Literal["create", "update", "delete"] = "create"):
        sublists = split_list(data, max_items_per_sublist=500)
        for sublist in sublists:
            res = HttpClient().post(
                url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{base_id}/tables/{table_id}/records/batch_{batch_type}",
                headers = {
                    "Authorization": f"Bearer {self.get_tenant_token()}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                data = {"records": sublist}
            )
            time.sleep(1)
            if res.status_code != 200:
                raise Exception(f"Error when batch_{batch_type} lark: {res.text}")
            else:
                if res.json()["code"] != 0:
                    raise Exception(f"Error when batch_{batch_type} lark: {res.text}")
                else:
                    logging.info(res.text)
        
        logging.info(f'Successfully {batch_type} {len(data)} records from {base_id}.{table_id}')
    
    def batch_create_from_df(self, base_id, table_id, df: pd.DataFrame):
        data = df.to_dict(orient='records')
        data_to_insert = [{"fields": item} for item in data]
        if data_to_insert:
            self.batch_edit(base_id=base_id, table_id=table_id, data=data_to_insert, batch_type="create")

    def truncate_table(self, base_id, table_id):
        data_to_delete = self.list_records(base_id=base_id, table_id=table_id, return_type='record_id_only')
        if data_to_delete:
            self.batch_edit(base_id=base_id, table_id=table_id, data=data_to_delete, batch_type="delete")

    def overwrite_table(self, base_id, table_id, input_type: Literal["dataframe", "list_of_dict"] = "dataframe", df: Optional[pd.DataFrame] = None, data: Optional[List[Dict]] = None):
        df = df if df is not None else pd.DataFrame()
        data = data if data is not None else []
        
        if input_type == "dataframe":
            if not isinstance(df, pd.DataFrame):
                raise ValueError("Expected a pandas DataFrame when input_type='dataframe'")
        elif input_type == "list_of_dict":
            if not isinstance(data, list) or not all(isinstance(i, dict) for i in data):
                raise ValueError("Expected a list of dictionaries when input_type='list_of_dict'")
        
        self.truncate_table(base_id=base_id, table_id=table_id)
        
        if input_type == "dataframe":
            self.batch_create_from_df(base_id=base_id, table_id=table_id, df=df)
        elif input_type == "list_of_dict":
            self.batch_edit(base_id=base_id, table_id=table_id, data=data, batch_type="create")

    def get_fb_tokens_in_lark(self, via_type = "Analytics"):
        all_token = {}

        params = {
            'filter': f'CurrentValue.[Loại Via]="{via_type}"'
        }

        items = self.list_records(base_id= self.fb_token_base_id, table_id= self.fb_token_table_id, params= params)
        for item in items:
            team = item["fields"]["Team"]
            token = item["fields"]["Token"]
            if token:
                all_token[team] = token
        
        return all_token
    
    def error_noti(self, title = None, msg = None, path = None):
        url = os.getenv('LARK_ERROR_NOTI_WEBHOOK_URL')

        title = f"Lỗi job {os.path.splitext(os.path.basename(path))[0]}" if path else "Lỗi job Lollibooks"
        msg = f"{msg}<hr />path: {path}" if path and msg else (f"path: {path}" if path else msg)
        
        card_design = {
            'msg_type': 'interactive',
            'card': {
                "config": {
                    "wide_screen_mode": True
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": msg
                    }
                ],
                "header": {
                    "template": "red",
                    "title": {
                        "content": title,
                        "tag": "plain_text"
                    }
                }
            }
        }

        HttpClient().post(
            url=url,
            headers={'Content-Type': 'application/json'},
            data=card_design
        )

    def extract_table_to_df(self, base_id, table_id, mapping_dict, params = None, has_record_id=True, fields_return:Literal['all', 'mapping_only']='mapping_only'):
        if not isinstance(mapping_dict, dict):
            raise TypeError("mapping_dict must be a dictionary")
        
        params = params if isinstance(params, dict) else {}

        if fields_return == 'mapping_only':
            field_names = [
                re.split(r'[\[\.]', v['path'])[0]
                for v in mapping_dict.values()
            ]

            params['field_names'] = json.dumps(field_names)
        
        data = self.list_records(base_id=base_id, table_id=table_id, return_type='full', params=params)
        if not data:
            return pd.DataFrame()
        df = lark_transform_mapping(df=pd.DataFrame(data), mapping_dict=mapping_dict, has_record_id=has_record_id)
        df.replace(np.nan, None, inplace=True)
        return df

    def batch_update_from_df(self, base_id, table_id, df: pd.DataFrame):
        data_to_update = df.apply(
            lambda row: {
                "record_id": row["record_id"], 
                "fields": row.drop("record_id").to_dict()
            }, 
            axis=1
        ).tolist()

        if data_to_update:
            self.batch_edit(base_id=base_id, table_id=table_id, data=data_to_update, batch_type="update")

    def get_google_credentials(self):
        result = []
        base_id = os.getenv("LARK_GOOGLE_TOKEN_BASE")
        table_id = os.getenv("LARK_GOOGLE_TOKEN_TABLE")

        params = {
            'view_id': 'vewqIP2Xvo'
        }

        items = self.list_records(base_id= base_id, table_id= table_id, params= params)
        for item in items:
            result.append(
                {
                    'team': item['fields']['Team'].strip(),
                    'client_id': item['fields']['Client ID'].strip(),
                    'client_secret': item['fields']['Client Secret'].strip(),
                    'refresh_token': item['fields']['Refresh Token'].strip(),
                    'developer_token': item['fields']['Developer Token'].strip(),
                    'mcc_id': str(item['fields']['MCC ID']).replace("-", "").strip(),
                }
            )
            
        return result

    def get_pancake_tokens_in_lark(self):
        all_token = {}
        base_id = os.getenv("LARK_PANCAKE_TOKEN_BASE_ID")
        table_id = os.getenv("LARK_PANCAKE_TOKEN_TABLE_ID")
    
        items = self.list_records(base_id= base_id, table_id= table_id)
        for item in items:
            department = item["fields"]["Bộ phận phụ trách"]
            token = item["fields"]["Token"]
            if token:
                all_token[department] = token
        
        return all_token
