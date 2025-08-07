import sys
import os
import json
import logging
from dotenv import load_dotenv
from datetime import datetime
import time

sys.stdout.reconfigure(encoding='utf-8')
load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger

db_golden_name = os.getenv("DB_GOLDEN_NAME")
golden_table_name = "pos_order_sources"

# Default order sources (no pandas)
default_sources = [
    {
        'shop_id': '*',
        'id': '-403',
        'name': 'Đơn hàng không có nguồn đơn',
        'custom_id': None,
        'link_source_id': None,
        'parent_id': None,
        'project_id': None,
        'inserted_at': datetime.now(),
        'updated_at': datetime.now()
    },
    {
        'shop_id': '*',
        'id': '-404',
        'name': 'Đã có dòng trên bảng UTM nhưng chưa chọn nguồn đơn',
        'custom_id': None,
        'link_source_id': None,
        'parent_id': None,
        'project_id': None,
        'inserted_at': datetime.now(),
        'updated_at': datetime.now()
    },
    {
        'shop_id': '*',
        'id': '-405',
        'name': 'Page chạy quảng cáo chưa được link vào POS',
        'custom_id': None,
        'link_source_id': None,
        'parent_id': None,
        'project_id': None,
        'inserted_at': datetime.now(),
        'updated_at': datetime.now()
    }
]

def load_config_shops():
    config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
    with open(config_file, 'r', encoding='utf-8') as file:
        return json.load(file).get('shops', [])

def clean_unicode_text(text):
    if not isinstance(text, str):
        return text
    return text.encode('utf-8', errors='ignore').decode('utf-8')

def clean_id_field(value):
    """Clean ID fields - remove decimal points"""
    if value is None or value == '':
        return None
    return str(value).split('.')[0]

def process_single_shop(shop_id, api_key):
    """Process one shop and insert immediately"""
    logging.info(f"Processing shop: {shop_id}")
    
    try:
        pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
        order_sources = pos_handler.get_all(endpoint='order_source')
        
        if not order_sources:
            logging.warning(f"No sources found for shop {shop_id}")
            return 0
        
        logging.info(f"Shop {shop_id}: {len(order_sources)} sources retrieved")
        
        # Process data without pandas
        processed_data = []
        
        for source in order_sources:
            # Clean and prepare record
            record = {
                'shop_id': str(shop_id),
                'id': clean_id_field(source.get('id')),
                'name': clean_unicode_text(str(source.get('name', ''))) if source.get('name') else None,
                'custom_id': clean_unicode_text(str(source.get('custom_id', ''))) if source.get('custom_id') else None,
                'link_source_id': clean_id_field(source.get('link_source_id')),
                'parent_id': clean_id_field(source.get('parent_id')),
                'project_id': clean_id_field(source.get('project_id')),
                'inserted_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Convert datetime fields if they exist in source
            if source.get('inserted_at'):
                try:
                    record['inserted_at'] = datetime.fromisoformat(str(source['inserted_at']).replace('Z', ''))
                except:
                    record['inserted_at'] = datetime.now()
            
            if source.get('updated_at'):
                try:
                    record['updated_at'] = datetime.fromisoformat(str(source['updated_at']).replace('Z', ''))
                except:
                    record['updated_at'] = datetime.now()
            
            # Clean empty strings
            for key, value in record.items():
                if value == '' or value == 'nan' or value == 'None':
                    record[key] = None
            
            processed_data.append(record)
        
        # Insert in small batches immediately
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(processed_data), batch_size):
            batch = processed_data[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            try:
                MariaDBHandler().insert_and_update_from_dict(
                    database=db_golden_name,
                    table=golden_table_name,
                    data=batch,
                    unique_columns=['shop_id', 'id']
                )
                total_inserted += len(batch)
                logging.info(f"Shop {shop_id} - Batch {batch_num}: {len(batch)} records inserted")
                time.sleep(0.2)
                
            except Exception as e:
                logging.error(f"Shop {shop_id} - Batch {batch_num} failed: {e}")
                continue
        
        logging.info(f"Shop {shop_id} completed: {total_inserted}/{len(processed_data)} records inserted")
        return total_inserted
        
    except Exception as e:
        logging.error(f"Shop {shop_id} processing failed: {e}")
        return 0

def insert_default_sources():
    """Insert default order sources"""
    try:
        MariaDBHandler().insert_and_update_from_dict(
            database=db_golden_name,
            table=golden_table_name,
            data=default_sources,
            unique_columns=['shop_id', 'id']
        )
        logging.info("Default sources inserted")
    except Exception as e:
        logging.warning(f"Default sources insert failed: {e}")

def get_order_sources():
    """Main function - process each shop individually"""
    shops = load_config_shops()
    logging.info(f"Processing {len(shops)} shops")
    
    # Insert default sources first
    insert_default_sources()
    
    total_inserted = 0
    successful_shops = 0
    
    for shop in shops:
        shop_id = shop.get('shop_id')
        api_key = shop.get('api_key')
        
        if not (shop_id and api_key):
            logging.warning(f"Skipping shop {shop_id}: missing credentials")
            continue
        
        records_inserted = process_single_shop(shop_id, api_key)
        
        if records_inserted > 0:
            total_inserted += records_inserted
            successful_shops += 1
        
        # Delay between shops
        time.sleep(2)
    
    logging.info(f"Processing completed: {successful_shops}/{len(shops)} shops successful")
    logging.info(f"Total records inserted: {total_inserted}")
    
    # Final verification
    try:
        db = MariaDBHandler()
        result = db.read_from_db(
            query="SELECT COUNT(*) as total FROM pos_order_sources",
            database=db_golden_name
        )
        total_in_db = result.iloc[0, 0] if result is not None else 0
        logging.info(f"Total records in database: {total_in_db}")
        
    except Exception as e:
        logging.warning(f"Could not verify final count: {e}")

if __name__ == "__main__":
    setup_logger(__file__)
    logging.info("Starting POS Order Sources extraction (No Pandas)")
    
    try:
        get_order_sources()
        logging.info("POS Order Sources extraction completed")
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise