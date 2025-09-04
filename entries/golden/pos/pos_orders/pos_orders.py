import datetime
import sys
import os
import json
import logging
from dotenv import load_dotenv
import time

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

setup_logger(__file__)

def load_config_shops():
    config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
    with open(config_file, 'r') as file:
        return json.load(file)

def load_config():
    config_file = rf"{CDP_PATH}/entries/golden/pos/pos_orders/config.json"
    with open(config_file, 'r') as file:
        return json.load(file)

config_shops = load_config_shops()
config = load_config()

DB_GOLDEN_NAME = os.getenv("DB_GOLDEN_NAME")
GOLDEN_TABLE_NAME = "pos_orders"
CURRENT_TIME = int(datetime.datetime.now().timestamp())
LAST_RUN_TIME = config.get("last_run", CURRENT_TIME - 86400)  # Default: last 24h
SHOPS = config_shops.get("shops", [])

def update_last_run_time(end_time):
    config_file = rf"{CDP_PATH}/entries/golden/pos/pos_orders/config.json"
    
    with open(config_file, 'r+') as file:
        config = json.load(file)
        config["last_run"] = end_time
        file.seek(0)
        json.dump(config, file, indent=4)
        file.truncate()
    logging.info(f"Updated last_run to {end_time}")

def clean_unicode_text(text):
    if not isinstance(text, str):
        return text
    return text.encode('utf-8', errors='ignore').decode('utf-8')

def extract_product_names(items):
    if isinstance(items, str):
        try:
            items = json.loads(items)
        except json.JSONDecodeError:
            return ""
    if not isinstance(items, list):
        return ""
    
    names = []
    for item in items:
        if isinstance(item, dict) and not item.get("is_bonus_product", False):
            variation_info = item.get("variation_info", {})
            if isinstance(variation_info, dict):
                name = variation_info.get("name", "")
                if name:
                    names.append(name)
    
    return ", ".join(names)

def extract_ids(tags):
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except json.JSONDecodeError:
            return ''
    
    if not isinstance(tags, list):
        return ''
    
    ids = []
    for tag in tags:
        if isinstance(tag, dict) and "id" in tag:
            ids.append(str(tag["id"]))
    
    return ', '.join(ids)

def get_updated_at(history_list, old_status=None, new_status=None):
    if isinstance(history_list, str):
        try:
            history_list = json.loads(history_list)
        except json.JSONDecodeError:
            return None

    if isinstance(history_list, list):
        for item in history_list:
            if isinstance(item, dict) and "status" in item and isinstance(item["status"], dict):
                old = item["status"].get("old")
                new = item["status"].get("new")
                if (old_status is None or old == old_status) and (new_status is None or new == new_status):
                    return item.get("updated_at")
    return None

def parse_datetime_safe(dt_str):
    if not dt_str:
        return None
    try:
        if isinstance(dt_str, str):
            return datetime.datetime.fromisoformat(dt_str.replace('Z', ''))
        return dt_str
    except:
        return None

def process_single_order(order, shop_id):
    """Process single order without pandas"""
    try:
        # Parse JSON fields
        partner = order.get("partner")
        if isinstance(partner, str):
            try:
                partner = json.loads(partner)
            except:
                partner = {}
        elif not isinstance(partner, dict):
            partner = {}
        
        shipping_address = order.get("shipping_address")
        if isinstance(shipping_address, str):
            try:
                shipping_address = json.loads(shipping_address)
            except:
                shipping_address = {}
        elif not isinstance(shipping_address, dict):
            shipping_address = {}
        
        # Extract transformed fields
        record = {
            'order_id': str(order.get('id', '')),
            'ad_id': order.get('p_utm_content') if order.get('p_utm_content') else order.get('ad_id') if order.get('ad_id') else None,
            'shop_id': str(shop_id),
            'status': order.get('status'),
            'partner_name': partner.get('partner_name'),
            'extend_code': partner.get('extend_code'),
            'first_delivery_at': parse_datetime_safe(partner.get('first_delivery_at')),
            'product': json.dumps(order.get('items', []), ensure_ascii=False) if order.get('items') else None,
            'transfer_money': float(order.get('transfer_money', 0)) if order.get('transfer_money') else None,
            'total_price': float(order.get('total_price', 0)) if order.get('total_price') else None,
            'cod': float(order.get('cod', 0)) if order.get('cod') else None,
            'partner_fee': float(order.get('partner_fee', 0)) if order.get('partner_fee') else None,
            'order_sources_name': order.get('order_sources_name'),
            'total_discount': float(order.get('total_discount', 0)) if order.get('total_discount') else None,
            'bill_phone_number': order.get('bill_phone_number'),
            'account': order.get('account'),
            'time_assign_seller': parse_datetime_safe(order.get('time_assign_seller')),
            'time_assign_care': parse_datetime_safe(order.get('time_assign_care')),
            'shipping_address': shipping_address.get('full_address') if shipping_address else None,
            'bill_full_name': clean_unicode_text(order.get('bill_full_name', '')),
            'order_sources': order.get('order_sources'),
            'ads_sources': order.get('ads_source'),
            'sub_status': order.get('sub_status'),
            'warehouse_id': order.get('warehouse_id'),
            'assigning_seller_id': order.get('assigning_seller_id'),
            'marketer': order.get('marketer').get('id') if order.get('marketer') else None,
            'assigning_care_id': order.get('assigning_care_id'),
            'note': clean_unicode_text(order.get('note', '')) if order.get('note') else None,
            'tags': json.dumps(order.get('tags', []), ensure_ascii=False) if order.get('tags') else None,
            'products_name': extract_product_names(order.get('items', [])),
            'inserted_at': parse_datetime_safe(order.get('inserted_at')),
            'updated_at': parse_datetime_safe(order.get('updated_at')),
            'province_id': shipping_address.get('province_id') if shipping_address else None,
            'province_name': shipping_address.get('province_name') if shipping_address else None,
            'utm_source': order.get('p_utm_source'),
            'returned_reason': order.get('returned_reason_name', '').split('/')[0] if order.get('returned_reason_name') and '/' in order.get('returned_reason_name', '') else order.get('returned_reason_name'),
            'returned_reason_detail': order.get('returned_reason_name', '').split('/')[1] if order.get('returned_reason_name') and '/' in order.get('returned_reason_name', '') else None,
            'sent_time': parse_datetime_safe(get_updated_at(order.get('histories'), None, 2)),
            'confirmed_time': parse_datetime_safe(get_updated_at(order.get('histories'), None, 1)),
            'returned_time': parse_datetime_safe(get_updated_at(order.get('histories'), 4, 5)),
            'receive_time': parse_datetime_safe(get_updated_at(order.get('histories'), 2, 3)),
            'tags_id': extract_ids(order.get('tags', [])),
            'money_to_collect': float(order.get('money_to_collect', 0)) if order.get('money_to_collect') else None,
            'partial_return_time': parse_datetime_safe(get_updated_at(order.get('histories'), 4, 15)),
            'total_price_after_sub_discount': float(order.get('total_price_after_sub_discount', 0)) if order.get('total_price_after_sub_discount') else None,
            'fee_marketplace': float(order.get('fee_marketplace', 0)) if order.get('fee_marketplace') else None
        }
        
        # Clean empty strings and 'None'
        for key, value in record.items():
            if value == '' or value == 'None' or value == 'nan':
                record[key] = None
        
        return record
        
    except Exception as e:
        logging.error(f"Error processing order {order.get('id', 'unknown')}: {e}")
        return None

def insert_with_adaptive_batch(data, table_name, unique_columns, initial_batch_size=15):
    """Insert data with adaptive batch size - reduce batch size if errors occur"""
    batch_size = initial_batch_size
    total_inserted = 0
    start_idx = 0
    consecutive_errors = 0
    
    logging.info(f"Starting adaptive batch insert: {len(data)} records (initial batch_size: {batch_size})")
    
    while start_idx < len(data):
        batch = data[start_idx:start_idx + batch_size]
        
        try:
            MariaDBHandler().insert_and_update_from_dict(
                database=DB_GOLDEN_NAME,
                table=table_name,
                data=batch,
                unique_columns=unique_columns
            )
            
            total_inserted += len(batch)
            start_idx += batch_size
            consecutive_errors = 0
            
            logging.info(f"Batch inserted: {len(batch)} records (batch_size: {batch_size})")
            
            # Success - can try increasing batch size slightly
            if consecutive_errors == 0 and batch_size < 30:
                batch_size = min(batch_size + 3, 30)
            
        except Exception as e:
            consecutive_errors += 1
            logging.error(f"Batch failed (size: {batch_size}): {e}")
            
            # Reduce batch size on error
            if batch_size > 5:
                batch_size = max(batch_size // 2, 5)
                logging.info(f"Reducing batch size to: {batch_size}")
            else:
                # If even size 5 fails, skip this batch
                logging.error(f"Skipping problematic batch: {len(batch)} records")
                start_idx += batch_size
                
            # Too many consecutive errors - abort
            if consecutive_errors >= 5:
                logging.error("Too many consecutive batch errors, aborting")
                break
        
        time.sleep(0.1)  # Small delay
    
    return total_inserted

def fetch_orders_for_shop(shop_id, api_key):
    """Fetch and process orders for single shop"""
    logging.info(f"Fetching orders for shop {shop_id}")
    
    try:
        with PosAPIHandler(shop_id, api_key) as pos_handler:
            orders = pos_handler.get_all("orders", params={
                "page_number": 1, 
                "page_size": 200, 
                "startDateTime": LAST_RUN_TIME,
                "endDateTime": CURRENT_TIME, 
                "updateStatus": "inserted_at"
            })
            
            if not orders:
                logging.info(f"No orders retrieved for shop {shop_id}")
                return 0
            
            logging.info(f"Shop {shop_id}: {len(orders)} orders retrieved")
            
            # Process each order
            processed_orders = []
            for order in orders:
                processed_order = process_single_order(order, shop_id)
                if processed_order:
                    processed_orders.append(processed_order)
            
            logging.info(f"Shop {shop_id}: {len(processed_orders)} orders processed")
            
            # Use adaptive batch insert
            total_inserted = insert_with_adaptive_batch(
                data=processed_orders,
                table_name=GOLDEN_TABLE_NAME,
                unique_columns=["shop_id", "order_id"],
                initial_batch_size=15  # Start with 15 for orders (more complex)
            )
            
            logging.info(f"Shop {shop_id} completed: {total_inserted}/{len(processed_orders)} orders inserted")
            return total_inserted
            
    except Exception as e:
        logging.error(f"Error fetching orders for shop {shop_id}: {e}")
        return 0

def fetch_orders_from_db():
    """Process all shops"""
    logging.info(f"Processing orders for {len(SHOPS)} shops")
    logging.info(f"Time range: {LAST_RUN_TIME} to {CURRENT_TIME}")
    
    total_inserted = 0
    successful_shops = 0
    
    for shop in SHOPS:
        shop_id = shop.get("shop_id")
        api_key = shop.get("api_key")
        
        if shop_id and api_key:
            orders_inserted = fetch_orders_for_shop(shop_id, api_key)
            if orders_inserted > 0:
                total_inserted += orders_inserted
                successful_shops += 1
            
            # Delay between shops
            time.sleep(2)
        else:
            logging.warning(f"Skipping shop {shop_id}: missing credentials")
    
    logging.info(f"Processing completed: {successful_shops}/{len(SHOPS)} shops successful")
    logging.info(f"Total orders inserted: {total_inserted}")

if __name__ == "__main__":
    logging.info("Starting POS Orders extraction (Safe Mode)")
    
    try:
        fetch_orders_from_db()
        update_last_run_time(end_time=CURRENT_TIME)
        logging.info("Orders processing completed")
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise