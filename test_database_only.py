import sys
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler

if __name__ == "__main__":
    print("🧪 Testing database insert only")
    
    try:
        db_name = os.getenv("DB_GOLDEN_NAME")
        table_name = "pos_order_sources"
        
        # Create minimal test data (no pandas)
        test_data = [
            {
                'shop_id': 'TEST_SHOP',
                'id': 'TEST_001',
                'name': 'Test Source 1',
                'custom_id': None,
                'link_source_id': None,
                'parent_id': None,
                'project_id': None,
                'inserted_at': datetime.now(),
                'updated_at': datetime.now()
            },
            {
                'shop_id': 'TEST_SHOP',
                'id': 'TEST_002', 
                'name': 'Test Source 2',
                'custom_id': None,
                'link_source_id': None,
                'parent_id': None,
                'project_id': None,
                'inserted_at': datetime.now(),
                'updated_at': datetime.now()
            }
        ]
        
        print(f"💾 Testing insert to {db_name}.{table_name}")
        print(f"📊 Test data: {len(test_data)} records")
        
        # Test database insert
        db = MariaDBHandler()
        db.insert_and_update_from_dict(
            database=db_name,
            table=table_name,
            data=test_data,
            unique_columns=['shop_id', 'id']
        )
        
        print("✅ Database insert successful!")
        
        # Verify data
        result = db.read_from_db(
            query="SELECT * FROM pos_order_sources WHERE shop_id = 'TEST_SHOP'",
            database=db_name
        )
        
        if result is not None and len(result) > 0:
            print(f"🔍 Verification: {len(result)} records found in database")
            for _, row in result.iterrows():
                print(f"  - {row['id']}: {row['name']}")
        
        # Clean up test data
        db.read_from_db(
            query="DELETE FROM pos_order_sources WHERE shop_id = 'TEST_SHOP'",
            database=db_name
        )
        print("🧹 Test data cleaned up")
        
        print("🎉 Database test completed - no segfault!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()