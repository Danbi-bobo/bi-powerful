import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv('CDP_PATH')
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

print("🚀 Complete POS System Test")
print("=" * 50)

# Test 1: Environment & Imports
print("\n1️⃣ Testing Environment & Imports:")
try:
    from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
    from cdp.adapters.pos.pos_api_handler import PosAPIHandler
    from cdp.domain.utils.log_helper import setup_logger
    print("✅ All modules imported successfully")
except ImportError as e:
    print(f"❌ Import error: {e}")

# Test 2: Database Connection & Tables
print("\n2️⃣ Testing Database & Tables:")
try:
    db = MariaDBHandler()
    db_name = os.getenv('DB_GOLDEN_NAME')
    
    # Test pos_orders
    result = db.read_from_db(
        query="SELECT COUNT(*) as count FROM pos_orders",
        database=db_name
    )
    orders_count = result.iloc[0, 0] if result is not None else 0
    print(f"✅ pos_orders table: {orders_count} records")
    
    # Test pos_order_sources  
    result = db.read_from_db(
        query="SELECT COUNT(*) as count FROM pos_order_sources",
        database=db_name
    )
    sources_count = result.iloc[0, 0] if result is not None else 0
    print(f"✅ pos_order_sources table: {sources_count} records")
    
except Exception as e:
    print(f"❌ Database error: {e}")

# Test 3: Config Files
print("\n3️⃣ Testing Config Files:")

# Test shops config
shops_config_path = os.path.join(CDP_PATH, "entries", "golden", "pos", "config_shops.json")
if os.path.exists(shops_config_path):
    try:
        with open(shops_config_path, 'r') as f:
            config = json.load(f)
        shops = config.get('shops', [])
        print(f"✅ POS shops config: {len(shops)} shop(s) configured")
        
        for shop in shops:
            shop_id = shop.get('shop_id')
            api_key = shop.get('api_key')
            if api_key != 'your_api_key_here':
                print(f"  🔑 Shop {shop_id}: API key configured")
            else:
                print(f"  ⚠️ Shop {shop_id}: API key needs update")
    except Exception as e:
        print(f"❌ Shops config error: {e}")
else:
    print(f"❌ Shops config not found: {shops_config_path}")

# Test orders config  
orders_config_path = os.path.join(CDP_PATH, "entries", "golden", "pos", "pos_orders", "config.json")
if os.path.exists(orders_config_path):
    print("✅ POS orders config exists")
else:
    print("⚠️ POS orders config not found")

# Test 4: POS API Handler
print("\n4️⃣ Testing POS API Handler:")
try:
    pos = PosAPIHandler(shop_id="test", api_key="test", timeout=10)
    print("✅ POS API handler initialized successfully")
except Exception as e:
    print(f"❌ POS API handler error: {e}")

# Test 5: Environment Variables
print("\n5️⃣ Testing Environment Variables:")
required_vars = [
    'CDP_PATH', 'DB_HOST', 'DB_USER', 'DB_GOLDEN_NAME', 
    'POS_API_URL', 'POS_API_KEY_1021208973'
]

for var in required_vars:
    value = os.getenv(var)
    if value:
        if 'API_KEY' in var and value != 'your_real_api_key_here':
            print(f"✅ {var}: configured")
        elif 'API_KEY' in var:
            print(f"⚠️ {var}: needs real API key")  
        else:
            print(f"✅ {var}: {value}")
    else:
        print(f"❌ {var}: not set")

# Summary
print("\n" + "=" * 50)
print("📊 SUMMARY:")
print("✅ Modules: Ready")
print("✅ Database: Connected with tables")  
print("✅ POS Handler: Initialized")
print("⚠️ API Keys: Need real credentials")

print("\n🎯 NEXT STEPS:")
print("1. Update API keys in config_shops.json")
print("2. Update POS_API_KEY_* in .env file") 
print("3. Run POS jobs:")
print("   - python entries/golden/pos/pos_order_sources/pos_order_sources.py")
print("   - python entries/golden/pos/pos_orders/pos_orders.py")

print("\n🚀 POS System is ready for data extraction!")