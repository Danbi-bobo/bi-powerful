import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from queries import list_address
from cdp.domain.utils.log_helper2 import setup_logger
from datetime import datetime
import pandas as pd
from typing import List
import logging
from fuzzy_search import match_address
from maps_search import maps_handle_address

def get_orders(pos_client: PosAPIHandler):
    page_size = 100
    page_number = 1
    end = int(datetime.now().timestamp())
    start = end - 5 * 60
    params={
        "page_number": page_number, 
        "page_size": page_size, 
        "startDateTime": start,
        "endDateTime":end, 
        "updateStatus":"updated_at",
        "status": 0
    }
    orders = pos_client.get_all(
        endpoint = 'orders',
        params=params
    )
    return orders

def get_tag_ids(tags):
  return [tag['id'] for tag in tags]

def handle_shipping_address(shipping_address):
    cleaned_address = {k: v for k, v in shipping_address.items() if v is not None}
    return cleaned_address

def handle_orders(orders: List, address_df: pd.DataFrame, fuzzy_tag_id: int) -> List:
    pos_result = []
    map_result = []
    for order in orders:
        order_id = str(order['id'])
        tags = list(order['tags'] or [])
        tag_ids = get_tag_ids(tags)
        is_samenumber = 38 in tag_ids

        if fuzzy_tag_id not in tag_ids and not is_samenumber:
            shipping_address = order.get('shipping_address', {})
            shipping_address = handle_shipping_address(shipping_address)

            province_name = shipping_address.get('province_name', None)
            district_name = shipping_address.get('district_name', None)
            commune_name = shipping_address.get('commune_name', None)
            address = shipping_address.get('address')
            address = address.replace("brgy", "barangay")

            if province_name and address and not commune_name:
                fuzzy_result, status = match_address(
                    province_input=province_name,
                    district_input=district_name,
                    address_detail=address,
                    admin_df=address_df
                )
                
                if status == 'OK_EXACT':
                    shipping_address['district_id'] = fuzzy_result['district_id']
                    shipping_address['commune_id'] = fuzzy_result['commune_id']
                    tag_ids.append(fuzzy_tag_id)
                    pos_result.append(
                        {
                            'order_id': order_id,
                            'data': {
                                'shipping_address': shipping_address,
                                'tags': tag_ids
                            }
                        }
                    )
                else:
                    map_result.append(
                        {
                            'order_id': order_id,
                            'data': {
                                'shipping_address': shipping_address,
                                'tags': tag_ids
                            }
                        }
                    )
    return {
        'fuzzy': pos_result,
        'maps': map_result
    }

if __name__ == "__main__":
    setup_logger(__file__)
    
    fuzzy_search_tag_id = 426
    maps_search_tag_id = 427

    shop_id = '120276509'
    pos_api_key = os.getenv(f"POS_API_KEY_{shop_id}")

    google_maps_api_key = os.getenv(f"GOOGLE_MAPS_API_KEY1")

    address_df = MariaDBHandler().read_from_db(query=list_address, output_type='dataframe')

    pos_client = PosAPIHandler(shop_id=shop_id, api_key=pos_api_key)
    orders = get_orders(pos_client)
    orders_data = handle_orders(orders, address_df, fuzzy_tag_id=fuzzy_search_tag_id)

    fuzzy_data = orders_data['fuzzy'] or []
    for order in fuzzy_data:
        res = pos_client.update_order(
            order_id = order['order_id'],
            data = order['data']
        )
        logging.info(order)
        logging.info(res)

    maps_data = orders_data['maps']
    for order in maps_data:
        new_data = maps_handle_address(google_maps_api_key, order, address_df, maps_search_tag_id)
        if new_data:
            res = pos_client.update_order(
                order_id = new_data['order_id'],
                data = new_data['data']
            )
            logging.info(new_data)
            logging.info(res)
