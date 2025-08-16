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
        WHERE account_status IN (1, 3)
        AND token IS NOT NULL AND token != ''
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
                clean_record = {
                    'campaign_id': str(campaign.get('id', '')),
                    'campaign_name': str(campaign.get('name', ''))[:255],
                    'campaign_status': str(campaign.get('status', '')),
                    'created_time': campaign.get('created_time'),
                    'updated_time': campaign.get('updated_time'),
                    'start_time': campaign.get('start_time'),
                    'daily_budget': int(campaign.get('daily_budget')) if campaign.get('daily_budget') else None,
                    'spend_cap': int(campaign.get('spend_cap')) if campaign.get('spend_cap') else None,
                    'objective': str(campaign.get('objective', ''))[:100],
                    'access_token': campaign.get('access_token'),
                    'market': str(campaign.get('market', ''))[:100]
                }
                
                # Keep non-null values
                clean_record = {k: v for k, v in clean_record.items() if v is not None and v != ''}
                batch_records.append(clean_record)
                
            except Exception:
                continue
        
        # Save all at once
        if batch_records:
            mdb.insert_and_update_from_dict(
                database=TARGET_DATABASE,
                table=TARGET_TABLE,
                data=batch_records,
                unique_columns=["id"]
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
        print(f"   Success rate: {success_rate:.1f}%")

if __name__ == "__main__":
    main()