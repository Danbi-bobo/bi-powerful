import sys
import os
import json
import logging
from dotenv import load_dotenv
from datetime import datetime

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

def safe_convert_to_json(value):
    """Safely convert value to JSON string"""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value

def safe_convert_to_float(value):
    """Safely convert value to float"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_convert_to_int(value):
    """Safely convert value to int"""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_convert_to_bool(value):
    """Safely convert value to boolean"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return None

def process_order_items(order, shop_id):
    """Extract items from a single order"""
    order_id = str(order.get('id', ''))
    items = order.get('items', [])
    
    item_records = []
    
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        
        # Extract variation_info
        variation_info = item.get('variation_info', {})
        
        # Extract measure_info
        measure_info = variation_info.get('measure_info', {}) if variation_info else {}
        
        record = {
            'order_id': order_id,
            'shop_id': str(shop_id),
            'item_index': index,
            
            # Product identification
            'variation_id': item.get('variation_id'),
            'product_id': item.get('product_id'),
            'variation_name': variation_info.get('name') if variation_info else None,
            'variation_detail': variation_info.get('detail') if variation_info else None,
            'display_id': variation_info.get('display_id') if variation_info else None,
            'product_display_id': variation_info.get('product_display_id') if variation_info else None,
            
            # Quantity and pricing
            'quantity': safe_convert_to_int(item.get('quantity')),
            'retail_price': safe_convert_to_int(variation_info.get('retail_price')) if variation_info else None,
            'weight': safe_convert_to_int(variation_info.get('weight')) if variation_info else None,
            
            # Product flags
            'is_bonus_product': safe_convert_to_bool(item.get('is_bonus_product')),
            'is_composite': safe_convert_to_bool(item.get('is_composite')),
            'is_wholesale': safe_convert_to_bool(item.get('is_wholesale')),
            'one_time_product': safe_convert_to_bool(item.get('one_time_product')),
            
            # Discount info
            'discount_each_product': safe_convert_to_float(item.get('discount_each_product')),
            'is_discount_percent': safe_convert_to_bool(item.get('is_discount_percent')),
            
            # Notes
            'note': item.get('note'),
            'note_product': item.get('note_product'),
            
            # Measure info
            'measure_group_id': safe_convert_to_int(item.get('measure_group_id')),
            'measure_exchange_value': safe_convert_to_int(measure_info.get('exchange_value')) if measure_info else None,
            'measure_id': safe_convert_to_int(measure_info.get('measure_id')) if measure_info else None,
            
            # JSON fields
            'variation_info': safe_convert_to_json(variation_info),
            'images': safe_convert_to_json(variation_info.get('images')) if variation_info else None,
            'fields': safe_convert_to_json(variation_info.get('fields')) if variation_info else None,
            'components': safe_convert_to_json(item.get('components'))
        }
        
        # Clean empty values
        for key, value in record.items():
            if value == '' or value == 'None':
                record[key] = None
        
        # Only add if has valid order_id
        if record['order_id']:
            item_records.append(record)
    
    return item_records

def fetch_and_save_order_items():
    shops = load_config_shops().get("shops", [])
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    total_items = 0
    
    for shop in shops:
        shop_id = shop.get("shop_id")
        api_key = shop.get("api_key")
        
        if not (shop_id and api_key):
            continue
            
        try:
            logging.info(f"Processing order items for shop {shop_id}")
            
            # Fetch orders from API
            pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
            orders = pos_handler.get_all("orders")
            
            if not orders:
                logging.info(f"No orders for shop {shop_id}")
                continue
            
            logging.info(f"Shop {shop_id}: {len(orders)} orders retrieved")
            
            # Process orders and extract items
            all_item_records = []
            
            for order in orders:
                item_records = process_order_items(order, shop_id)
                all_item_records.extend(item_records)
            
            logging.info(f"Shop {shop_id}: {len(orders)} orders → {len(all_item_records)} item records")
            
            # Save to database in batches
            if all_item_records:
                batch_size = 100
                saved_count = 0
                
                for i in range(0, len(all_item_records), batch_size):
                    batch = all_item_records[i:i+batch_size]
                    
                    try:
                        MariaDBHandler().insert_and_update_from_dict(
                            database=db_golden,
                            table="pos_order_items",
                            data=batch,
                            unique_columns=["order_id", "shop_id", "item_index"]
                        )
                        
                        saved_count += len(batch)
                        logging.info(f"Shop {shop_id}: Batch {i//batch_size + 1} - {len(batch)} item records saved")
                        
                    except Exception as batch_error:
                        logging.error(f"Shop {shop_id}: Batch {i//batch_size + 1} failed: {batch_error}")
                        continue
                
                total_items += saved_count
                logging.info(f"Shop {shop_id}: ✓ Total {saved_count} item records saved")
            
        except Exception as e:
            logging.error(f"Shop {shop_id} failed: {e}")
            continue
    
    logging.info(f"Total order items processed: {total_items}")

def get_order_items_summary(order_id, shop_id):
    """Query function to get item summary for a specific order"""
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    query = f"""
        SELECT 
            order_id,
            COUNT(*) as total_items,
            SUM(quantity) as total_quantity,
            SUM(retail_price * quantity) as total_retail_value,
            GROUP_CONCAT(variation_name SEPARATOR ', ') as product_names
        FROM {db_golden}.pos_order_items
        WHERE order_id = '{order_id}' AND shop_id = '{shop_id}'
        GROUP BY order_id, shop_id
    """
    
    return MariaDBHandler().read_from_db(query=query, database=db_golden)

def get_order_items_detail(order_id, shop_id):
    """Query function to get detailed items for a specific order"""
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    query = f"""
        SELECT 
            item_index,
            variation_id,
            product_id,
            variation_name,
            variation_detail,
            quantity,
            retail_price,
            discount_each_product,
            is_bonus_product,
            is_composite,
            note,
            note_product
        FROM {db_golden}.pos_order_items
        WHERE order_id = '{order_id}' AND shop_id = '{shop_id}'
        ORDER BY item_index
    """
    
    return MariaDBHandler().read_from_db(query=query, database=db_golden)

if __name__ == "__main__":
    logging.info("Starting POS Order Items extraction")
    fetch_and_save_order_items()
    logging.info("Completed")
    
    # Example usage:
    # summary = get_order_items_summary("1418", "4")
    # details = get_order_items_detail("1418", "4")
    # print("Summary:", summary)
    # print("Details:", details)