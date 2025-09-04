import sys
import os
from dotenv import load_dotenv
from datetime import datetime
import time
import logging
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random

load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler

# Minimal logging
logging.basicConfig(level=logging.ERROR)

# Configuration
TARGET_DATABASE = os.getenv("DB_GOLDEN_NAME")
TARGET_TABLE = "fb_ad_ads_v2"
START_DATE = "2024-01-01"

# Rate limiting configuration
MAX_WORKERS = 3  # Giảm từ 6 xuống 3 để tránh rate limit
MIN_DELAY = 2    # Minimum delay between requests (seconds)
MAX_DELAY = 5    # Maximum delay between requests (seconds)
BATCH_SIZE = 50  # Smaller batches for faster processing
API_TIMEOUT = 90 # Shorter timeout
MAX_RETRIES = 2  # Fewer retries

# Rate limiter class
class RateLimiter:
    def __init__(self, calls_per_minute=100):  # Facebook limit ~200/min, we use 100 to be safe
        self.calls_per_minute = calls_per_minute
        self.calls = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # Remove calls older than 1 minute
            self.calls = [call_time for call_time in self.calls if now - call_time < 60]
            
            if len(self.calls) >= self.calls_per_minute:
                # Wait until oldest call is more than 1 minute old
                wait_time = 60 - (now - self.calls[0]) + 1
                if wait_time > 0:
                    print(f"⏳ Rate limit reached, waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
            
            # Add current call
            self.calls.append(now)

# Global rate limiter
rate_limiter = RateLimiter()

# Thread-safe counter
class ThreadSafeCounter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def increment(self, amount=1):
        with self._lock:
            self._value += amount
    
    @property
    def value(self):
        with self._lock:
            return self._value


def get_all_accounts():
    """Lấy campaigns với intelligent filtering"""
    query = """
        SELECT DISTINCT 
            c.campaign_id, 
            c.market, 
            c.access_token,
            c.campaign_name,
            c.updated_time
        FROM fb_ad_campaigns c
        WHERE c.access_token IS NOT NULL 
        AND c.access_token != ''
        AND c.market = 'Thailand'
        ORDER BY c.updated_time DESC  -- Ưu tiên campaigns mới update
    """

    try:
        mdb = MariaDBHandler()
        accounts = mdb.read_from_db(
            database=TARGET_DATABASE, query=query, output_type="list_of_dicts"
        )

        if accounts:
            print(f"✅ Found {len(accounts)} active campaigns")
            return accounts
        else:
            print("No accounts found")
            return []

    except Exception as e:
        print(f"Error getting accounts: {e}")
        return []


def extract_ads_for_campaign_safe(args):
    """Extract ads with rate limiting and error handling"""
    account, thread_id = args
    campaign_id = account["campaign_id"]
    token = account["access_token"]
    market = account["market"]
    campaign_name = account.get("campaign_name", "")

    try:
        # Wait for rate limit
        rate_limiter.wait_if_needed()
        
        # Random delay to spread requests
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        time.sleep(delay)
        
        fb_handler = FacebookAPIHandler(
            access_token=token, 
            timeout=API_TIMEOUT, 
            max_retries=MAX_RETRIES
        )

        ads_endpoint = f"{campaign_id}/ads"
        
        # Optimized fields for faster response
        params = {
            "fields": "id,name,status,created_time,updated_time,campaign_id,adset_id,account_id,ad_active_time,creative{call_to_action_type}",
            "limit": 500,  # Larger limit to reduce API calls
            "filtering": f'''[{{"field": "ad.updated_time", "operator": "GREATER_THAN", "value": "{START_DATE}"}}]'''
        }

        print(f"🔄 [T{thread_id}] Fetching {campaign_id[:15]}...", end=" ")
        ads = fb_handler.get_all(endpoint=ads_endpoint, params=params)

        if ads:
            # Quick processing
            processed_ads = []
            for ad in ads:
                processed_ad = process_ad_fast(ad, market, campaign_name)
                if processed_ad:
                    processed_ads.append(processed_ad)
            
            print(f"✅ {len(processed_ads)} ads")
            
            return {
                'success': True,
                'campaign_id': campaign_id,
                'ads': processed_ads,
                'count': len(processed_ads),
                'thread_id': thread_id
            }
        else:
            print(f"📭 No ads")
            return {
                'success': True,
                'campaign_id': campaign_id,
                'ads': [],
                'count': 0,
                'thread_id': thread_id
            }

    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error: {error_msg[:30]}...")
        
        # Check if it's a rate limit error
        if 'rate limit' in error_msg.lower() or 'too many requests' in error_msg.lower():
            print(f"⏳ Rate limited, waiting 30s...")
            time.sleep(30)
        
        return {
            'success': False,
            'campaign_id': campaign_id,
            'error': error_msg[:100],
            'thread_id': thread_id
        }
    finally:
        if 'fb_handler' in locals():
            fb_handler.close()


def process_ad_fast(ad, market, campaign_name):
    """Fast ad processing with minimal data extraction"""
    try:
        creative = ad.get("creative", {}) or {}
        
        clean_record = {
            "campaign_id": str(ad.get("campaign_id", ""))[:50] or None,
            "adset_id": str(ad.get("adset_id", ""))[:50] or None,
            "account_id": str(ad.get("account_id", ""))[:50] or None,
            "ad_id": str(ad.get("id", ""))[:50] or None,
            "ad_name": str(ad.get("name", ""))[:255] or None,
            "status": str(ad.get("status", ""))[:50] or None,
            "created_time": normalize_datetime_fast(ad.get("created_time")),
            "updated_time": normalize_datetime_fast(ad.get("updated_time")),
            "ad_active_time": safe_int(ad.get("ad_active_time")),
            "call_to_action_type": str(creative.get("call_to_action_type", ""))[:100] or None,
            "market": str(market)[:100] if market else None,
        }

        # Remove None values
        return {k: v for k, v in clean_record.items() if v is not None}

    except Exception:
        return None


def safe_int(value):
    """Safe integer conversion"""
    try:
        return int(value) if value is not None else None
    except:
        return None


def normalize_datetime_fast(dt_str):
    """Fast datetime normalization"""
    if not dt_str or not isinstance(dt_str, str):
        return None
    try:
        if 'T' in dt_str:
            date_part = dt_str.split('T')[0]
            time_part = dt_str.split('T')[1].split('+')[0].split('Z')[0]
            return f"{date_part} {time_part}"
    except:
        pass
    return None


def save_ads_batch_fast(ads_list):
    """Fast batch save"""
    if not ads_list:
        return 0

    try:
        mdb = MariaDBHandler()
        mdb.insert_and_update_from_dict(
            database=TARGET_DATABASE,
            table=TARGET_TABLE,
            data=ads_list,
            unique_columns=["ad_id"],
            log=False
        )
        return len(ads_list)
        
    except Exception as e:
        print(f"❌ Save failed: {str(e)[:30]}...")
        return 0


def main():
    """Main function with intelligent rate limiting"""
    print(f"🚀 Facebook Ads Extractor - Rate Limited")
    print(f"⚡ Workers: {MAX_WORKERS}, Batch: {BATCH_SIZE}, Delay: {MIN_DELAY}-{MAX_DELAY}s")
    print(f"📊 Rate limit: {rate_limiter.calls_per_minute} calls/minute")
    
    start_time = datetime.now()
    
    try:
        # Get campaigns
        print("\n🔄 Getting campaigns...")
        accounts = get_all_accounts()
        
        if not accounts:
            print("❌ No campaigns to process")
            return
        
        # Counters
        total_ads = ThreadSafeCounter()
        total_saved = ThreadSafeCounter()
        completed_campaigns = ThreadSafeCounter()
        failed_campaigns = ThreadSafeCounter()
        pending_ads = []
        
        print(f"🔧 Processing {len(accounts)} campaigns with rate limiting...")
        
        # Process in smaller batches to respect rate limits
        batch_size = MAX_WORKERS * 2  # Process 2 rounds per worker set
        
        for batch_start in range(0, len(accounts), batch_size):
            batch_accounts = accounts[batch_start:batch_start + batch_size]
            print(f"\n📦 Processing batch {batch_start//batch_size + 1}/{(len(accounts)-1)//batch_size + 1} ({len(batch_accounts)} campaigns)")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Create tasks
                tasks = [(account, i % MAX_WORKERS) for i, account in enumerate(batch_accounts)]
                
                # Submit tasks
                future_to_task = {executor.submit(extract_ads_for_campaign_safe, task): task for task in tasks}
                
                # Process results
                for future in as_completed(future_to_task):
                    try:
                        result = future.result()
                        completed_campaigns.increment()
                        
                        if result['success']:
                            ads = result['ads']
                            if ads:
                                pending_ads.extend(ads)
                                total_ads.increment(result['count'])
                            
                            # Save batch when full
                            if len(pending_ads) >= BATCH_SIZE:
                                saved = save_ads_batch_fast(pending_ads)
                                total_saved.increment(saved)
                                print(f"💾 Saved {saved} ads (Total: {total_saved.value})")
                                pending_ads = []
                        else:
                            failed_campaigns.increment()
                            # If rate limited, wait longer between batches
                            if 'rate limit' in result.get('error', '').lower():
                                print(f"⏳ Rate limit detected, waiting 60s...")
                                time.sleep(60)
                    
                    except Exception as e:
                        failed_campaigns.increment()
                        print(f"❌ Task error: {str(e)[:30]}...")
            
            # Wait between batches to respect rate limits
            if batch_start + batch_size < len(accounts):
                wait_time = random.uniform(10, 20)  # 10-20s between batches
                print(f"⏸️ Waiting {wait_time:.1f}s between batches...")
                time.sleep(wait_time)
        
        # Save remaining ads
        if pending_ads:
            saved = save_ads_batch_fast(pending_ads)
            total_saved.increment(saved)
            print(f"💾 Final save: {saved} ads")
        
        # Summary
        duration = datetime.now() - start_time
        success_rate = (total_saved.value / total_ads.value * 100) if total_ads.value > 0 else 0
        campaigns_success = ((completed_campaigns.value - failed_campaigns.value) / completed_campaigns.value * 100) if completed_campaigns.value > 0 else 0
        
        print(f"\n🎯 EXTRACTION COMPLETE")
        print(f"⚡ Campaigns: {completed_campaigns.value}/{len(accounts)} ({campaigns_success:.1f}% success)")
        print(f"📊 Ads extracted: {total_ads.value}")
        print(f"📊 Ads saved: {total_saved.value} ({success_rate:.1f}% success)")
        print(f"⏱️ Duration: {duration}")
        print(f"📈 Speed: {total_ads.value / duration.total_seconds():.1f} ads/sec")
        print(f"🔄 API calls/min: {total_ads.value / (duration.total_seconds() / 60):.1f}")
        
    except Exception as e:
        print(f"❌ Main error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()