import sys
import os
import json
import pandas as pd
from dotenv import load_dotenv
import numpy as np

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
    print("🧪 Testing pandas processing only")
    
    try:
        shops = load_config_shops()
        shop = shops[0]  # First shop only
        
        shop_id = shop['shop_id']
        api_key = shop['api_key']
        
        print(f"🏪 Testing shop: {shop_id}")
        
        # Get data from API
        pos_handler = PosAPIHandler(shop_id=shop_id, api_key=api_key)
        order_sources = pos_handler.get_all(endpoint='order_source')
        
        print(f"✅ API call successful: {len(order_sources)} records")
        
        # Add shop_id to data
        for source in order_sources:
            source['shop_id'] = shop_id
        
        print("📊 Creating DataFrame...")
        
        # Create DataFrame step by step
        df = pd.DataFrame(order_sources)
        print(f"✅ DataFrame created: {len(df)} rows, {len(df.columns)} columns")
        
        # Show basic info
        print(f"📋 Columns: {list(df.columns)}")
        print(f"📊 Data types:\n{df.dtypes}")
        
        # Try basic operations
        print("\n🔄 Testing basic pandas operations...")
        
        # Test 1: String operations
        if 'name' in df.columns:
            df['name_clean'] = df['name'].astype(str)
            print("✅ String operations OK")
        
        # Test 2: Replace NaN
        df_clean = df.replace([np.nan], None)
        print("✅ Replace NaN OK")
        
        # Test 3: Select columns
        test_cols = ['shop_id', 'id', 'name']
        available_cols = [col for col in test_cols if col in df.columns]
        df_selected = df[available_cols]
        print(f"✅ Column selection OK: {available_cols}")
        
        # Test 4: to_dict
        print("🔄 Converting to dict...")
        data_dict = df_selected.head(5).to_dict(orient='records')  # Only first 5 records
        print(f"✅ to_dict OK: {len(data_dict)} records")
        
        print("🎉 Pandas processing completed - no segfault!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()