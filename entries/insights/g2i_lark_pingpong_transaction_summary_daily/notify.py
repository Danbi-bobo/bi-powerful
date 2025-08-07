import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.domain.utils.log_helper import setup_logger
from datetime import date
from cdp.adapters.http.http_client import HttpClient
import logging

def check_notified(lark_client):
    data = lark_client.list_records(
        base_id= 'JkBKbSSPQapddtshUdjle7ylg2g',
        table_id='tblnuizBZk49spZH',
        params={
            'view_id': 'vewrAYvSw0',
            'field_names': '["is_notified", "STT"]'
        }
    )

    record = data[0]
    record_id = record['record_id']
    is_notified = record['fields'].get('is_notified', True)

    return record_id, is_notified

def get_card_balance(lark_client):
    result = {}
    mapping_dict = {
        'date': {'path': 'Ngày', 'type': 'ms_timestamp'},
        'card': {'path': 'Thẻ', 'type': 'str'},
        'balance': {'path': 'Số dư', 'type': 'double'},
        'updated_at': {'path': 'Dữ liệu cập nhật lúc', 'type': 'lark_date'},
        'market': {'path': 'Thị trường.[0].text', 'type': 'str'}
    }

    df = lark_client.extract_table_to_df(
        base_id= 'JkBKbSSPQapddtshUdjle7ylg2g',
        table_id='tbluWWL8rIxDifmF',
        params={
            'view_id': 'vewFnlihYS',
            'field_names': '["Ngày", "Số dư", "Thẻ", "Dữ liệu cập nhật lúc", "Thị trường"]'
        },
        mapping_dict = mapping_dict
    )
    
    updated_at = df['updated_at'].unique()[0]
    updated_at_str = updated_at.strftime('%d/%m/%Y %H:%M')
    data = df.to_dict(orient='records')

    for card in data:
        market = card['market']
        last4 = card['card']

        if market not in result:
            result[market] = {}
        
        result[market][last4] = card['balance']
    
    return result, updated_at_str

def prepare_msg(market, market_balance_data, updated_at):
    today_str = date.today().strftime('%d/%m/%Y')
    
    msg_data = []
    for last4, balance in market_balance_data.items():
        msg_data.append(f'- Thẻ {last4}: **{round(float(balance), 2)} USD**')
    
    msg = '\n'.join(msg_data)
    msg += f'\n\n *Dữ liệu cập nhật lúc {updated_at}*'
    title = f"Số dư thẻ PingPong {market} ngày {today_str}"
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

    return card_design

def get_urls(lark_client):
    result = {}
    data = lark_client.list_records(
        base_id= 'JkBKbSSPQapddtshUdjle7ylg2g',
        table_id='tbl6sD3WEMZffpS2',
        params={
            'field_names': '["Thị trường", "Webhook URL"]',
            'view_id': 'vew5YFRRXk'
        }
    )

    for record in data:
        fields = record['fields']
        market = fields['Thị trường']
        url = fields['Webhook URL']['link']
        if market and url:
            result[market] = url
    
    return result

if __name__ == "__main__":
    setup_logger(__file__)

    lark_client = LarkApiHandle()

    record_id, is_notified = check_notified(lark_client)
    if not is_notified:
        url_dict = get_urls(lark_client)
        balace_data, updated_at = get_card_balance(lark_client)

        for market, market_balance_data in balace_data.items():
            card = prepare_msg(market, market_balance_data, updated_at)
            url = url_dict.get(market, None)
            if url:
                HttpClient().post(
                    url=url,
                    headers={'Content-Type': 'application/json'},
                    data=card
                )

        lark_client.batch_edit(
            base_id='JkBKbSSPQapddtshUdjle7ylg2g',
            table_id='tblnuizBZk49spZH',
            batch_type='update',
            data=[
                {
                    'record_id': record_id,
                    'fields': {
                        'is_notified': True
                    }
                }
            ]
        )
    else:
        logging.info('Latest upload log has already been notified. Skipping notification.')