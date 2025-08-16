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

def parse_datetime_safe(dt_str):
    """Safely parse datetime string"""
    if not dt_str:
        return None
    try:
        if isinstance(dt_str, str):
            # Handle format: "2024-01-10T14:34:44.000000"
            if '.' in dt_str:
                dt_str = dt_str.split('.')[0]  # Remove microseconds
            return datetime.fromisoformat(dt_str)
        return dt_str
    except:
        return None

def extract_warehouse_data(variations_warehouses):
    """Extract warehouse data from variations_warehouses array"""
    if not variations_warehouses or not isinstance(variations_warehouses, list):
        return {}
    
    # Take first warehouse data (most products have 1 warehouse)
    warehouse = variations_warehouses[0]
    return {
        'warehouse_id': warehouse.get('warehouse_id'),
        'warehouse_actual_remain_quantity': safe_convert_to_float(warehouse.get('actual_remain_quantity')),
        'warehouse_pending_quantity': safe_convert_to_int(warehouse.get('pending_quantity')),
        'warehouse_remain_quantity': safe_convert_to_int(warehouse.get('remain_quantity')),
        'warehouse_returning_quantity': safe_convert_to_int(warehouse.get('returning_quantity')),
        'warehouse_selling_avg': safe_convert_to_float(warehouse.get('selling_avg')),
        'warehouse_total_quantity': safe_convert_to_int(warehouse.get('total_quantity')),
        'warehouse_waiting_quantity': None,  # Not in API response
        'warehouse_batch_position': None,    # Not in API response
        'warehouse_shelf_position': None     # Not in API response
    }

def extract_product_categories(product_data):
    """Extract categories from product data as comma-separated strings"""
    if not product_data or not isinstance(product_data, dict):
        return None, None
    
    categories = product_data.get('categories', [])
    if not categories:
        return None, None
    
    # Extract IDs and names as comma-separated strings
    category_ids = []
    category_names = []
    
    for cat in categories:
        if isinstance(cat, dict):
            cat_id = cat.get('id')
            cat_name = cat.get('name', '')
            
            if cat_id:
                category_ids.append(str(cat_id))
            if cat_name:
                category_names.append(cat_name.strip())
    
    # Join with comma separator
    return (
        ', '.join(category_ids) if category_ids else None,
        ', '.join(category_names) if category_names else None
    )

def process_product_variation(variation, shop_id):
    """Convert product variation data to database records - one record per warehouse"""
    
    # Extract product data
    product_data = variation.get('product', {})
    categories_id, categories_name = extract_product_categories(product_data)
    
    # Get product name and display_id from nested product object
    product_name = product_data.get('name', '') if product_data else ''
    product_display_id = product_data.get('display_id', '') if product_data else ''
    
    # Base record data (common for all warehouses)
    base_record = {
        # Basic info
        'id': variation.get('id', ''),
        'shop_id': int(shop_id),
        'variation_id': variation.get('id'),
        'product_id': variation.get('product_id'),
        'name': product_name,
        'barcode': variation.get('barcode'),
        'display_id': product_display_id,
        
        # Boolean flags
        'is_composite': None,
        'is_hidden': safe_convert_to_bool(variation.get('is_hidden')),
        'is_locked': safe_convert_to_bool(variation.get('is_locked')),
        'is_removed': None,
        'is_sell_negative_variation': safe_convert_to_bool(variation.get('is_sell_negative_variation')),
        
        # Pricing (same for all warehouses)
        'retail_price': safe_convert_to_int(variation.get('retail_price')),
        'retail_price_after_discount': None,
        'wholesale_price': safe_convert_to_json(variation.get('wholesale_price')),
        'price_at_counter': safe_convert_to_float(variation.get('price_at_counter')),
        'last_imported_price': safe_convert_to_float(variation.get('last_imported_price')),
        'total_purchase_price': safe_convert_to_float(variation.get('total_purchase_price')),
        
        # Total inventory (across all warehouses)
        'remain_quantity': safe_convert_to_int(variation.get('remain_quantity')),
        'weight': None,
        
        # Categories
        'categories_id': categories_id,
        'categories_name': categories_name,
        
        # JSON fields
        'product': safe_convert_to_json(variation.get('product')),
        'bonus_variations': None,
        'composite_products': None,
        'fields': safe_convert_to_json(variation.get('fields')),
        'images': safe_convert_to_json(variation.get('images')),
        'videos': None,
        
        # Timestamps
        'inserted_at': parse_datetime_safe(variation.get('inserted_at')),
        'updated_at': None
    }
    
    # Get warehouse data
    variations_warehouses = variation.get('variations_warehouses', [])
    records = []
    
    if variations_warehouses and isinstance(variations_warehouses, list):
        # Create one record per warehouse
        for warehouse in variations_warehouses:
            if isinstance(warehouse, dict):
                record = base_record.copy()
                
                # Add warehouse-specific data
                record.update({
                    'warehouse_id': warehouse.get('warehouse_id'),
                    'warehouse_actual_remain_quantity': safe_convert_to_float(warehouse.get('actual_remain_quantity')),
                    'warehouse_pending_quantity': safe_convert_to_int(warehouse.get('pending_quantity')),
                    'warehouse_remain_quantity': safe_convert_to_int(warehouse.get('remain_quantity')),
                    'warehouse_returning_quantity': safe_convert_to_int(warehouse.get('returning_quantity')),
                    'warehouse_selling_avg': safe_convert_to_float(warehouse.get('selling_avg')),
                    'warehouse_total_quantity': safe_convert_to_int(warehouse.get('total_quantity')),
                    'warehouse_waiting_quantity': None,
                    'warehouse_batch_position': warehouse.get('batch_position'),
                    'warehouse_shelf_position': warehouse.get('shelf_position')
                })
                
                # Clean empty values
                for key, value in record.items():
                    if value == '' or value == 'None':
                        record[key] = None
                
                records.append(record)
    else:
        # No warehouse data - create single record with NULL warehouse info
        record = base_record.copy()
        record.update({
            'warehouse_id': None,
            'warehouse_actual_remain_quantity': None,
            'warehouse_pending_quantity': None,
            'warehouse_remain_quantity': None,
            'warehouse_returning_quantity': None,
            'warehouse_selling_avg': None,
            'warehouse_total_quantity': None,
            'warehouse_waiting_quantity': None,
            'warehouse_batch_position': None,
            'warehouse_shelf_position': None
        })
        
        # Clean empty values
        for key, value in record.items():
            if value == '' or value == 'None':
                record[key] = None
        
        records.append(record)
    
    return records

def fetch_and_save_products():
    shops = load_config_shops().get("shops", [])
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    total_products = 0
    
    for shop in shops:
        shop_id = shop.get("shop_id")
        api_key = shop.get("api_key")
        
        if not (shop_id and api_key):
            continue
            
        try:
            logging.info(f"Processing product variations for shop {shop_id}")
            
            # Fetch product variations from API
            pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
            variations = pos_handler.get_all("products/variations")
            
            if not variations:
                logging.info(f"No product variations for shop {shop_id}")
                continue
            
            logging.info(f"Shop {shop_id}: {len(variations)} variations retrieved")
            
            # Process variations in batches
            batch_size = 50
            processed_count = 0
            
            for i in range(0, len(variations), batch_size):
                batch = variations[i:i+batch_size]
                
                # Process batch - now returns multiple records per variation
                batch_records = []
                for variation in batch:
                    variation_records = process_product_variation(variation, shop_id)  # Returns list
                    if variation_records:  # List of records
                        batch_records.extend(variation_records)
                
                # Save batch to database
                if batch_records:
                    try:
                        MariaDBHandler().insert_and_update_from_dict(
                            database=db_golden,
                            table="pos_products",
                            data=batch_records,
                            unique_columns=["id", "shop_id", "warehouse_id"]  # Include warehouse_id
                        )
                        
                        processed_count += len(batch_records)
                        logging.info(f"Shop {shop_id}: Batch {i//batch_size + 1} - {len(batch_records)} product records saved")
                        
                    except Exception as batch_error:
                        logging.error(f"Shop {shop_id}: Batch {i//batch_size + 1} failed: {batch_error}")
                        continue
            
            total_products += processed_count
            logging.info(f"Shop {shop_id}: ✓ Total {processed_count} variations saved")
            
        except Exception as e:
            logging.error(f"Shop {shop_id} failed: {e}")
            continue
    
    logging.info(f"Total product variations processed: {total_products}")

if __name__ == "__main__":
    logging.info("Starting POS Product Variations extraction")
    fetch_and_save_products()
    logging.info("Completed")