import os
import sys
from dotenv import load_dotenv

print("🔍 Testing CDP_PATH configuration...")

# Load environment
load_dotenv()
CDP_PATH = os.getenv('CDP_PATH')

print(f"CDP_PATH from .env: {CDP_PATH}")

# Add to Python path
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)
    print(f"✅ Added {CDP_PATH} to Python path")

# Test imports
print("\n📦 Testing imports...")

try:
    from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
    print('✅ MariaDBHandler imported successfully')
except ImportError as e:
    print(f'❌ MariaDBHandler import failed: {e}')

try:
    from cdp.adapters.pos.pos_api_handler import PosAPIHandler
    print('✅ PosAPIHandler imported successfully')
except ImportError as e:
    print(f'❌ PosAPIHandler import failed: {e}')

try:
    from cdp.domain.utils.log_helper import setup_logger
    print('✅ setup_logger imported successfully')
except ImportError as e:
    print(f'❌ setup_logger import failed: {e}')

try:
    from cdp.adapters.http.http_client import HttpClient
    print('✅ HttpClient imported successfully')
except ImportError as e:
    print(f'❌ HttpClient import failed: {e}')

print("\n🎯 Import test completed!")