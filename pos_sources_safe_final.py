import sys
import os
import json
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

def load_config_shops():
    config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
    with open(config_file, 'r') as file:
        return json.load(file).get('shops', [])

def clean_unicode_text(text):
    """Clean problematic Unicode characters"""
    if not isinstance(text, str):
        return text
    
    # Remove or replace problematic characters
    text = text.encode('utf-8', errors='ignore').decode('utf-8')
    # Remove null bytes and control characters
    text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
    return text

def process_shop_data_safe(shop_id, api_key):
    """Process shop data safely without pandas DataFrame"""
    print(f"🏪 Processing shop: {shop_id}")
    
    try:
        # Get API data
        pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
        order_sources = pos_handler.get_all(endpoint='order_source')
        
        if not order_sources:
            print(f"⚠️ No data for shop {shop_id}")
            return 0
        
        print(f"📊 Retrieved {len(order_sources)} records")
        
        # Process data manually (avoid pandas)
        processed_data = []
        
        for source in order_sources:
            # Clean and prepare each record
            record = {
                'shop_id': str(shop_id),
                'id': str(source.get('id', '')),
                'name': clean_unicode_text(str(source.get('name', ''))),
                'custom_id': clean_unicode_text(str(source.get('custom_id', ''))) if source.get('custom_id') else None,
                'link_source_id': str(source.get('link_source_id', '')) if source.get('link_source_id') else None,
                'parent_id': str(source.get('parent_id', '')) if source.get('parent_id') else None,
                'project_id': str(source.get('project_id', '')) if source.get('project_id') else None,
                'inserted_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Convert empty strings to None
            for key, value in record.items():
                if value == '' or value == 'nan':
                    record[key] = None
            
            processed_data.append(record)
        
        print(f"🧹 Processed {len(processed_data)} records")
        
        # Insert in small batches to avoid segfault
        batch_size = 5  # Very small batch
        total_inserted = 0
        
        db = MariaDBHandler()
        db_name = os.getenv("DB_GOLDEN_NAME")
        
        for i in range(0, len(processed_data), batch_size):
            batch = processed_data[i:i+batch_size]
            
            print(f"💾 Inserting batch {i//batch_size + 1}: {len(batch)} records")
            
            try:
                db.insert_and_update_from_dict(
                    database=db_name,
                    table='pos_order_sources',
                    data=batch,
                    unique_columns=['shop_id', 'id']
                )
                total_inserted += len(batch)
                print(f"✅ Batch inserted successfully")
                
                # Small delay between batches
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Batch insert failed: {e}")
                # Continue with next batch
                continue
        
        print(f"🎉 Shop {shop_id} completed: {total_inserted}/{len(processed_data)} records inserted")
        return total_inserted
        
    except Exception as e:
        print(f"❌ Error processing shop {shop_id}: {e}")
        return 0

def insert_default_sources():
    """Insert default order sources"""
    try:
        print("📝 Inserting default order sources...")
        
        default_data = [
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
        
        db = MariaDBHandler()
        db_name = os.getenv("DB_GOLDEN_NAME")
        
        db.insert_and_update_from_dict(
            database=db_name,
            table='pos_order_sources',
            data=default_data,
            unique_columns=['shop_id', 'id']
        )
        
        print("✅ Default sources inserted")
        
    except Exception as e:
        print(f"⚠️ Default sources insert failed: {e}")

if __name__ == "__main__":
    print("🚀 POS Order Sources - Safe Mode (No Pandas)")
    
    try:
        # Insert defaults
        insert_default_sources()
        
        # Process shops
        shops = load_config_shops()
        print(f"📊 Processing {len(shops)} shop(s)")
        
        total_success = 0
        
        for shop in shops:
            shop_id = shop.get('shop_id')
            api_key = shop.get('api_key')
            
            if not (shop_id and api_key):
                print(f"⚠️ Skipping shop {shop_id}: missing credentials")
                continue
            
            records_inserted = process_shop_data_safe(shop_id, api_key)
            if records_inserted > 0:
                total_success += records_inserted
            
            # Delay between shops
            time.sleep(2)
        
        print(f"\n🎉 Extraction completed!")
        print(f"📊 Total records inserted: {total_success}")
        
        # Final verification
        db = MariaDBHandler()
        result = db.read_from_db(
            query="SELECT COUNT(*) as total FROM pos_order_sources",
            database=os.getenv("DB_GOLDEN_NAME")
        )
        total_in_db = result.iloc[0, 0] if result is not None else 0
        print(f"🔍 Total in database: {total_in_db}")
        
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()