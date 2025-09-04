import sys
import os
import json
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')
load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.pos.pos_api_handler import PosAPIHandler

def load_config_shops():
    config_file = rf"{CDP_PATH}/entries/golden/pos/config_shops.json"
    with open(config_file, 'r') as file:
        return json.load(file).get('shops', [])

if __name__ == "__main__":
    print("🧪 Testing API only (no pandas, no database)")
    
    try:
        shops = load_config_shops()
        shop = shops[0]  # First shop only
        
        shop_id = shop['shop_id']
        api_key = shop['api_key']
        
        print(f"🏪 Testing shop: {shop_id}")
        
        # Test API call only
        pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
        order_sources = pos_handler.get_all(endpoint='order_source')
        
        print(f"✅ API call successful!")
        print(f"📊 Retrieved {len(order_sources)} records")
        
        # Show first few records (no pandas)
        print("\n📋 Sample data (first 3):")
        for i, source in enumerate(order_sources[:3]):
            print(f"  {i+1}. Name: {source.get('name', 'No name')}")
            print(f"     ID: {source.get('id')}")
            print(f"     Shop: {source.get('shop_id', shop_id)}")
            print()
        
        print("🎉 API test completed - no segfault!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()