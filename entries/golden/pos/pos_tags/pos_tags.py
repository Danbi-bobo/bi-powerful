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

def process_tag(tag, shop_id):
    """Convert one tag into database records"""
    base_record = {
        'id': str(tag.get('id', '')),
        'shop_id': str(shop_id),
        'name': tag.get('name', ''),
        'color': tag.get('color', ''),
        'is_system_tag': bool(tag.get('is_system_tag', False)),
        'updated_at': None
    }
    
    groups = tag.get('groups', [])
    records = []
    
    if groups:
        # Create one record per group
        for group in groups:
            record = base_record.copy()
            record['group_id'] = group.get('id')
            record['group_name'] = group.get('name', '')
            records.append(record)
    else:
        # No groups - single record with null group info
        record = base_record.copy()
        record['group_id'] = None
        record['group_name'] = None
        records.append(record)
    
    return records

def fetch_and_save_tags():
    shops = load_config_shops().get("shops", [])
    db_golden = os.getenv("DB_GOLDEN_NAME")
    
    for shop in shops:
        shop_id = shop.get("shop_id")
        api_key = shop.get("api_key")
        
        if not (shop_id and api_key):
            continue
            
        try:
            logging.info(f"Processing shop {shop_id}")
            
            # Fetch tags from API
            pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
            tags = pos_handler.get_all("orders/tags")
            
            if not tags:
                logging.info(f"No tags for shop {shop_id}")
                continue
            
            # Process all tags into records
            all_records = []
            for tag in tags:
                records = process_tag(tag, shop_id)
                all_records.extend(records)
            
            logging.info(f"Shop {shop_id}: {len(tags)} tags → {len(all_records)} records")
            
            # Save to database
            MariaDBHandler().insert_and_update_from_dict(
                database=db_golden,
                table="pos_tags",
                data=all_records,
                unique_columns=["id", "shop_id", "group_id"]
            )
            
            logging.info(f"Shop {shop_id}: ✓ Saved {len(all_records)} records")
            
        except Exception as e:
            logging.error(f"Shop {shop_id} failed: {e}")
            continue

if __name__ == "__main__":
    logging.info("Starting POS Tags extraction")
    fetch_and_save_tags()
    logging.info("Completed")