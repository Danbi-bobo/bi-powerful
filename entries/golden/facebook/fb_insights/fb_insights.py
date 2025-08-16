import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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
TARGET_TABLE = "fb_ad_insights"

# Fast configuration
START_DATE = "2024-01-01"
END_DATE = "2025-08-15"
MAX_WORKERS = 8  # Parallel threads
CHUNK_DAYS = 30  # Bigger chunks
BATCH_SIZE = 500  # Smaller batches for faster saves

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


def safe_convert_float(value, default=0.0):
    if value is None or str(value).strip() in ['', 'nan', 'None', 'null']:
        return default
    try:
        val = float(value)
        if val != val or val == float('inf') or val == float('-inf'):
            return default
        return val
    except:
        return default


def safe_convert_int(value, default=0):
    if value is None or str(value).strip() in ['', 'nan', 'None', 'null']:
        return default
    try:
        val = float(value)
        if val != val or val == float('inf') or val == float('-inf'):
            return default
        return int(val)
    except:
        return default


def safe_process_actions(actions):
    action_dict = {}
    if not actions or not isinstance(actions, list):
        return action_dict
    
    try:
        action_mappings = {
            'page_engagement': 'page_engagement',
            'post_engagement': 'post_engagement',
            'link_click': 'link_click',
            'video_view': 'video_view',
            'post_reaction': 'post_reaction',
            'like': 'page_like',
            'comment': 'comment',
            'post': 'post_share',
            'landing_page_view': 'landing_page_view',
            'onsite_conversion.messaging_first_reply': 'message',
            'onsite_conversion.total_messaging_connection': 'total_messaging_connection',
            'offsite_conversion.fb_pixel_complete_registration': 'complete_registration',
        }
        
        for action in actions:
            if isinstance(action, dict) and 'action_type' in action and 'value' in action:
                action_type = action.get('action_type')
                value = action.get('value')
                
                if action_type in action_mappings:
                    column_name = action_mappings[action_type]
                    if column_name == 'link_click':
                        action_dict[column_name] = safe_convert_float(value, default=0.0)
                    else:
                        action_dict[column_name] = safe_convert_int(value, default=0)
        
        return action_dict
    except:
        return {}


def create_fast_record(row, market=None):
    """Simplified record creation for speed"""
    try:
        actions_data = safe_process_actions(row.get('actions', []))
        
        record = {
            "account_id": str(row.get("account_id", ""))[:50] or None,
            "ad_account": str(row.get("account_name", ""))[:255] or None,
            "ad_id": str(row.get("ad_id", ""))[:50] or None,
            "ad_name": str(row.get("ad_name", ""))[:255] or None,
            "adset_id": str(row.get("adset_id", ""))[:50] or None,
            "adset_name": str(row.get("adset_name", ""))[:255] or None,
            "campaign_id": str(row.get("campaign_id", ""))[:50] or None,
            "campaign_name": str(row.get("campaign_name", ""))[:255] or None,
            "objective": str(row.get("objective", ""))[:100] or None,
            "market": str(market)[:255] if market else None,
            
            "spend": safe_convert_float(row.get("spend"), default=0.0),
            "reach": safe_convert_int(row.get("reach"), default=0),
            "clicks": safe_convert_int(row.get("clicks"), default=0),
            "impressions": safe_convert_int(row.get("impressions"), default=0),
            "unique_clicks": safe_convert_int(row.get("unique_clicks"), default=0),
            "frequency": safe_convert_float(row.get("frequency"), default=0.0),
            
            "date": str(row.get("date_start", ""))[:10] if row.get("date_start") else None,
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        record.update(actions_data)
        
        # Only keep essential fields for speed
        essential_defaults = {
            "page_engagement": 0,
            "post_engagement": 0,
            "link_click": 0.0,
            "video_view": 0,
            "page_like": 0,
            "comment": 0,
            "message": 0,
            "total_messaging_connection": 0,
            "complete_registration": 0,
            "landing_page_view": 0,
            "post_reaction": 0
        }
        
        for field, default in essential_defaults.items():
            if field not in record:
                record[field] = default
        
        return {k: v for k, v in record.items() if v is not None}
        
    except Exception as e:
        return None


def get_campaigns_from_database():
    """Get campaigns with exclusions"""
    excluded_campaigns = [
        '120238169857720112', '120210918037800191', '120210920633150191', '120211113880650191',
        '120211191501270191', '120211323990870191', '120211397474660191', '120211434499720191',
        '120211614768540191', '120211632686960191', '120211759623680191', '120211765886460191',
        '120211816633900191', '120211857972180191', '120211874374870191', '120211876554340191',
        '120211917489870191', '120212055910040191', '120212158115900191', '120212277412410191',
        '120212303253370191', '120212323209390191', '120212366236330191', '120212414919150191',
        '120213618406880191', '120213733461070191', '120214038093820191', '120214063319190191',
        '120214063548810191', '120214242489510191', '120214270972430191', '120214937409200191',
        '120215135202290191', '120215163383930191', '120215193643220191', '120215268237610191',
        '120215302771650191', '120215359990150191', '120215392537190191', '120215455280980191',
        '120215646296860191', '120215863982790191', '120215915442350191', '120216041153610191',
        '120219915000980191', '120219915000990191', '120219922151480191', '120219964506250191',
        '120219998354590191', '120219999387800191', '120219999387810191', '120220000613370191',
        '120220001123720191', '120220002470680191', '120220003538160191', '120220003555020191',
        '120220056125190191', '120220056242060191'
    ]
    
    # Create placeholders for IN clause
    placeholders = ','.join(['%s'] * len(excluded_campaigns))
    
    query = f"""
        SELECT 
            campaign_id,
            access_token,
            market
        FROM 
            fb_ad_campaigns
        WHERE 
            access_token IS NOT NULL 
            AND access_token != ''
            AND campaign_id NOT IN ({placeholders})
        ORDER BY 
            market, campaign_id
    """
    
    try:
        mdb = MariaDBHandler()
        campaigns = mdb.read_from_db(
            database=TARGET_DATABASE,
            query=query,
            params=tuple(excluded_campaigns),
            output_type="list_of_dicts"
        )
        
        if campaigns:
            markets = {}
            for campaign in campaigns:
                market = campaign['market'] or 'Unknown'
                markets[market] = markets.get(market, 0) + 1
            
            print(f"✅ Found {len(campaigns)} campaigns (excluded {len(excluded_campaigns)}):")
            for market, count in markets.items():
                print(f"   {market}: {count}")
            
            return campaigns
        else:
            return []
            
    except Exception as e:
        print(f"❌ Error getting campaigns: {e}")
        return []


def get_fast_date_ranges(start_date, end_date, chunk_days=30):
    """Create larger date chunks for faster processing"""
    ranges = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days-1), end)
        ranges.append({
            "since": current.strftime("%Y-%m-%d"),
            "until": chunk_end.strftime("%Y-%m-%d")
        })
        current = chunk_end + timedelta(days=1)
    
    return ranges


def extract_campaign_chunk(args):
    """Extract data for one campaign and one date range - optimized for parallel processing"""
    campaign, date_range, thread_id = args
    campaign_id = campaign['campaign_id']
    access_token = campaign['access_token']
    market = campaign['market']
    
    try:
        # Faster API configuration
        fb_handler = FacebookAPIHandler(access_token=access_token, timeout=120, max_retries=2)
        
        endpoint = f"{campaign_id}/insights"
        params = {
            # Reduced fields for speed
            "fields": "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,reach,clicks,impressions,unique_clicks,frequency,date_start,actions,objective",
            "level": "ad",
            "time_increment": 1,
            "time_range": f'{{"since":"{date_range["since"]}","until":"{date_range["until"]}"}}',
            "limit": 1000,
            "filtering": '''[{"field": "spend", "operator": "GREATER_THAN", "value": 0}]'''
        }
        
        data = fb_handler.get_all(endpoint=endpoint, params=params)
        
        if data:
            records = []
            for row in data:
                record = create_fast_record(row, market=market)
                if record:
                    records.append(record)
            
            return {
                'success': True,
                'campaign_id': campaign_id,
                'date_range': f"{date_range['since']} to {date_range['until']}",
                'records': records,
                'count': len(records),
                'thread_id': thread_id
            }
        else:
            return {
                'success': True,
                'campaign_id': campaign_id,
                'date_range': f"{date_range['since']} to {date_range['until']}",
                'records': [],
                'count': 0,
                'thread_id': thread_id
            }
            
    except Exception as e:
        return {
            'success': False,
            'campaign_id': campaign_id,
            'date_range': f"{date_range['since']} to {date_range['until']}",
            'error': str(e)[:100],
            'thread_id': thread_id
        }
    finally:
        if 'fb_handler' in locals():
            fb_handler.close()


def save_fast_batch(records_list):
    """Fast batch save with minimal error handling"""
    if not records_list:
        return 0
    
    try:
        mdb = MariaDBHandler()
        mdb.insert_and_update_from_dict(
            database=TARGET_DATABASE,
            table=TARGET_TABLE,
            data=records_list,
            unique_columns=["ad_id", "date"],
            log=False
        )
        return len(records_list)
    except Exception as e:
        print(f"❌ Fast save failed: {str(e)[:50]}...")
        return 0


def main():
    print(f"🚀 Facebook Insights - FAST PARALLEL EXTRACTION (Incremental Save)")
    print(f"⚡ Max workers: {MAX_WORKERS}, Chunk days: {CHUNK_DAYS}, Batch size: {BATCH_SIZE}")
    print(f"📅 Date range: {START_DATE} to {END_DATE}")
    
    start_time = datetime.now()
    
    try:
        # Get campaigns
        print("\n🔄 Getting campaigns...")
        campaigns = get_campaigns_from_database()
        
        if not campaigns:
            print("❌ No campaigns to process")
            return
        
        # Get date ranges
        date_ranges = get_fast_date_ranges(START_DATE, END_DATE, chunk_days=CHUNK_DAYS)
        print(f"📊 Processing {len(date_ranges)} chunks × {len(campaigns)} campaigns = {len(date_ranges) * len(campaigns)} tasks")
        
        # Create task list for parallel processing
        tasks = []
        for campaign in campaigns:
            for date_range in date_ranges:
                tasks.append((campaign, date_range, len(tasks) % MAX_WORKERS))
        
        print(f"🔧 Created {len(tasks)} parallel tasks")
        
        # Counters
        total_records = 0
        total_saved = 0
        completed_tasks = 0
        failed_tasks = 0
        pending_records = []  # Buffer for incremental saves
        
        # Parallel processing with incremental saves
        print(f"\n🚀 Starting parallel extraction with incremental saves...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_task = {executor.submit(extract_campaign_chunk, task): task for task in tasks}
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                try:
                    result = future.result()
                    completed_tasks += 1
                    
                    if result['success']:
                        records = result['records']
                        if records:
                            pending_records.extend(records)
                            total_records += result['count']
                        
                        # Incremental save every BATCH_SIZE records OR every 50 tasks
                        if len(pending_records) >= BATCH_SIZE or completed_tasks % 100 == 0:
                            if pending_records:  # Only save if there are records
                                print(f"\n💾 Saving batch of {len(pending_records)} records...", end=" ")
                                saved = save_fast_batch(pending_records)
                                total_saved += saved
                                pending_records = []  # Clear buffer
                                print(f"✅ Saved {saved} records (Total saved: {total_saved})")
                        
                        # Progress update every 50 tasks
                        if completed_tasks % 50 == 0:
                            elapsed = datetime.now() - start_time
                            progress = (completed_tasks / len(tasks)) * 100
                            rate = total_records / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                            print(f"📊 Progress: {completed_tasks}/{len(tasks)} ({progress:.1f}%) - {total_records} extracted, {total_saved} saved - {rate:.1f} rec/sec")
                    else:
                        failed_tasks += 1
                        if failed_tasks <= 5:  # Only show first 5 errors
                            print(f"❌ Failed: {result['campaign_id']} - {result.get('error', 'Unknown')}")
                
                except Exception as e:
                    failed_tasks += 1
                    print(f"❌ Task exception: {str(e)[:50]}...")
        
        # Save any remaining records
        if pending_records:
            print(f"💾 Saving final batch of {len(pending_records)} records...", end=" ")
            saved = save_fast_batch(pending_records)
            total_saved += saved
            print(f"✅ Saved {saved} records")
        
        # Final summary
        duration = datetime.now() - start_time
        success_rate = (total_saved / total_records * 100) if total_records > 0 else 0
        task_success_rate = ((completed_tasks - failed_tasks) / completed_tasks * 100) if completed_tasks > 0 else 0
        
        print(f"\n🎯 FAST EXTRACTION COMPLETE")
        print(f"⚡ Tasks completed: {completed_tasks}/{len(tasks)} ({task_success_rate:.1f}% success)")
        print(f"📊 Records extracted: {total_records}")
        print(f"📊 Records saved: {total_saved} ({success_rate:.1f}% success)")
        print(f"⏱️ Duration: {duration}")
        print(f"🚀 Speed: {total_records / duration.total_seconds():.1f} records/second")
        
    except Exception as e:
        print(f"❌ Main error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()