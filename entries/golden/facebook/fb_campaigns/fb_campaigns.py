import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from datetime import datetime
import time
import logging

# Minimal logging - chỉ ERROR
logging.basicConfig(level=logging.ERROR)

# Configuration
TARGET_DATABASE = os.getenv("DB_GOLDEN_NAME")
TARGET_TABLE = "fb_ad_campaigns"
START_DATE = "2024-01-01"

def get_all_accounts():
    """Lấy tất cả ad accounts"""
    query = """
        SELECT id, name, token, market
        FROM ad_account
        WHERE token IS NOT NULL AND token != ''
        ORDER BY market, name
    """
    
    try:
        mdb = MariaDBHandler()
        accounts = mdb.read_from_db(
            database=TARGET_DATABASE,
            query=query,
            output_type='list_of_dicts'
        )
        
        if accounts:
            # Group by market
            markets = {}
            for acc in accounts:
                market = acc['market']
                markets[market] = markets.get(market, 0) + 1
            
            print(f"✅ Found {len(accounts)} accounts:")
            for market, count in markets.items():
                print(f"   {market}: {count}")
            
            return accounts
        else:
            print("No accounts found")
            return []
            
    except Exception as e:
        print(f"Error getting accounts: {e}")
        return []

def extract_campaigns_for_account(account):
    """Extract campaigns cho 1 account"""
    account_id = account['id']
    token = account['token']
    market = account['market']
    
    # ✅ Extract clean account ID từ account_id (bỏ "act_" prefix nếu có)
    clean_account_id = account_id.replace('act_', '') if account_id.startswith('act_') else account_id
    
    try:
        fb_handler = FacebookAPIHandler(
            access_token=token,
            timeout=180,
            max_retries=2
        )
        
        campaigns_endpoint = f"{account_id}/campaigns"
        params = {
            'fields': 'id,name,status,created_time,daily_budget,objective,spend_cap,start_time,updated_time,account_id',
            'limit': 1000,
            'filtering': f'''[{{"field": "campaign.updated_time", "operator": "GREATER_THAN", "value": "{START_DATE}"}}]'''
        }
        
        campaigns = fb_handler.get_all(endpoint=campaigns_endpoint, params=params)
        
        if campaigns:
            # Add metadata
            for campaign in campaigns:
                campaign['market'] = market
                campaign['account_name'] = account['name']
                campaign['access_token'] = token
                campaign['clean_account_id'] = clean_account_id  # ✅ Pass clean account ID
            
            return campaigns
        else:
            return []
            
    except Exception as e:
        print(f"{account_id}: {str(e)[:50]}...")
        return []
    finally:
        if 'fb_handler' in locals():
            fb_handler.close()

def save_campaigns_batch(campaigns_list):
    """Save campaigns to database"""
    if not campaigns_list:
        return 0
    
    try:
        mdb = MariaDBHandler()
        batch_records = []
        
        for campaign in campaigns_list:
            try:
                # ✅ Lấy account_id từ API response, fallback về clean_account_id từ URL
                api_account_id = campaign.get('account_id')
                clean_account_id = campaign.get('clean_account_id')  # From URL without "act_"
                
                # Priority: API response -> Clean URL account ID
                final_account_id = None
                if api_account_id:
                    # API trả về account_id, bỏ "act_" nếu có
                    final_account_id = str(api_account_id).replace('act_', '') if str(api_account_id).startswith('act_') else str(api_account_id)
                elif clean_account_id:
                    # Fallback to clean account ID from URL
                    final_account_id = str(clean_account_id)
                
                clean_record = {
                    'campaign_id': str(campaign.get('id', '')),
                    'account_id': final_account_id,  # ✅ Sử dụng account ID đã xử lý
                    'campaign_name': str(campaign.get('name', ''))[:255] if campaign.get('name') else None,
                    'campaign_status': str(campaign.get('status', '')) if campaign.get('status') else None,
                    'created_time': campaign.get('created_time'),
                    'updated_time': campaign.get('updated_time'),
                    'start_time': campaign.get('start_time'),
                    'daily_budget': int(campaign.get('daily_budget')) if campaign.get('daily_budget') else None,
                    'spend_cap': int(campaign.get('spend_cap')) if campaign.get('spend_cap') else None,
                    'objective': str(campaign.get('objective', ''))[:100] if campaign.get('objective') else None,
                    'access_token': campaign.get('access_token'),
                    'market': str(campaign.get('market', ''))[:100] if campaign.get('market') else None
                }
                
                # ✅ Chỉ loại bỏ các field bắt buộc nếu rỗng, giữ lại NULL values cho optional fields
                if clean_record['campaign_id']:  # Chỉ cần campaign_id không rỗng
                    # Convert empty strings to None for optional fields
                    for key, value in clean_record.items():
                        if value == '' and key != 'campaign_id':  # campaign_id không được phép rỗng
                            clean_record[key] = None
                    
                    batch_records.append(clean_record)
                    
                    # ✅ Debug log để xem account_id được xử lý như thế nào
                    if not api_account_id and clean_account_id:
                        print(f"  🔄 Fallback account_id: {clean_account_id} for campaign {clean_record['campaign_id']}")
                
            except Exception as e:
                print(f"Error processing campaign: {e}")
                continue
        
        # Save all at once
        if batch_records:
            mdb.insert_and_update_from_dict(
                database=TARGET_DATABASE,
                table=TARGET_TABLE,
                data=batch_records,
                unique_columns=["campaign_id"]  # ✅ Sửa từ "id" thành "campaign_id"
            )
            return len(batch_records)
        
        return 0
        
    except Exception as e:
        print(f"Save error: {str(e)[:50]}...")
        return 0

def main():
    """Main function"""
    print(f"Database: {TARGET_DATABASE}.{TARGET_TABLE}")
    print(f"Date: {START_DATE} → now")
    
    start_time = datetime.now()
    
    # Get accounts
    accounts = get_all_accounts()
    if not accounts:
        return
    
    # Process accounts
    print(f"\nProcessing {len(accounts)} accounts...")
    
    total_campaigns = 0
    total_saved = 0
    success_accounts = 0
    
    for i, account in enumerate(accounts, 1):
        account_id = account['id']
        market = account['market']
        
        # Show progress
        print(f"[{i:2d}/{len(accounts)}] {account_id} ({market})...", end=' ')
        
        # Extract campaigns
        campaigns = extract_campaigns_for_account(account)
        
        if campaigns:
            total_campaigns += len(campaigns)
            
            # Save campaigns
            saved = save_campaigns_batch(campaigns)
            total_saved += saved
            success_accounts += 1
            
            print(f"✅ {len(campaigns)} campaigns → {saved} saved")
        else:
            print("📭 No campaigns")
        
        # Rate limiting
        if i < len(accounts):
            time.sleep(3)
    
    # Final summary
    duration = datetime.now() - start_time
    
    if total_campaigns > 0:
        success_rate = total_saved/total_campaigns*100
        print(f"   Success rate: {success_rate:.1f}% - Duration: {duration}")

if __name__ == "__main__":
    main()