import os
import sys
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv('CDP_PATH')
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

print("🔍 Testing Database Connection...")

# Test database connection
try:
    from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
    
    db = MariaDBHandler()
    
    # Test basic connection
    db.connect()
    print("✅ Basic database connection successful")
    db.close()
    
    # Test connection to specific database
    try:
        db.connect(database='alomix_golden_data')
        print("✅ Connected to alomix_golden_data database")
        db.close()
    except Exception as e:
        print(f"⚠️ alomix_golden_data database not found: {e}")
        print("💡 You may need to create this database first")
    
    print("\n📊 Database Configuration:")
    print(f"Host: {os.getenv('DB_HOST')}")
    print(f"User: {os.getenv('DB_USER')}")
    print(f"Golden DB: {os.getenv('DB_GOLDEN_NAME')}")
    
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    print("\n🔧 Please check:")
    print("1. MySQL/MariaDB is running")
    print("2. Database credentials in .env are correct")
    print("3. Database user has proper permissions")

print("\n🎯 Database test completed!")