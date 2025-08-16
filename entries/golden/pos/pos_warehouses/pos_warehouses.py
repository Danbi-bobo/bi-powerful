import sys
import os
import json
import logging
from dotenv import load_dotenv

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

def process_warehouse(warehouse, shop_id):
    """Convert warehouse data to database record"""
    record = {
        'id': warehouse.get('id', ''),
        'shop_id': int(shop_id),  # API returns shop_id as int in response
        'name': warehouse.get('name', ''),
        'address': warehouse.get('address', ''),
        'allow_create_order': bool(warehouse.get('allow_create_order', False)),
        'commune_id': warehouse.get('commune_id', ''),
        'country_code': warehouse.get('country_code', ''),
        'custom_id': warehouse.get('custom_id'),
        'district_id': warehouse.get('district_id', ''),
        'full_address': warehouse.get('full_address', ''),
        'phone_number': warehouse.get('phone_number', ''),
        'province_id': warehouse.get('province_id', '')
    }
    
    # Clean empty strings to NULL
    for key, value in record.items():
        if value == '' or value == 'None':
            if key == 'allow_create_order':
                record[key] = False  # Default boolean value
            else:
                record[key] = None
    
    return record

def fetch_and_save_warehouses():
    shops = load_config_shops().get("shops", [])
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    total_warehouses = 0
    
    for shop in shops:
        shop_id = shop.get("shop_id")
        api_key = shop.get("api_key")
        
        if not (shop_id and api_key):
            continue
            
        try:
            logging.info(f"Processing warehouses for shop {shop_id}")
            
            # Fetch warehouses from API
            pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
            warehouses = pos_handler.get_all("warehouses")
            
            if not warehouses:
                logging.info(f"No warehouses for shop {shop_id}")
                continue
            
            # Process all warehouses
            all_records = []
            for warehouse in warehouses:
                record = process_warehouse(warehouse, shop_id)
                if record['id']:  # Only add if has valid ID
                    all_records.append(record)
            
            logging.info(f"Shop {shop_id}: {len(warehouses)} warehouses → {len(all_records)} records")
            
            # Save to database
            if all_records:
                MariaDBHandler().insert_and_update_from_dict(
                    database=db_golden,
                    table="pos_warehouse",
                    data=all_records,
                    unique_columns=["id", "shop_id"]
                )
                
                total_warehouses += len(all_records)
                logging.info(f"Shop {shop_id}: ✓ Saved {len(all_records)} warehouses")
            
        except Exception as e:
            logging.error(f"Shop {shop_id} failed: {e}")
            continue
    
    logging.info(f"Total warehouses processed: {total_warehouses}")

if __name__ == "__main__":
    logging.info("Starting POS Warehouses extraction")
    fetch_and_save_warehouses()
    logging.info("Completed")