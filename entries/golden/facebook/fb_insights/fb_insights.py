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
END_DATE = "2025-08-24"
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

# Helper functions to add:

def extract_website_ctr(website_ctr_data):
    """Extract website CTR from API response"""
    if not website_ctr_data or not isinstance(website_ctr_data, list):
        return 0.0
    
    for item in website_ctr_data:
        if isinstance(item, dict) and item.get('action_type') == 'link_click':
            return safe_convert_float(item.get('value'), default=0.0)
    
    return 0.0


def extract_video_metric(video_data):
    """Extract video metrics from API response"""
    if not video_data or not isinstance(video_data, list):
        return 0
    
    for item in video_data:
        if isinstance(item, dict) and item.get('action_type') == 'video_view':
            return safe_convert_int(item.get('value'), default=0)
    
    return 0


# Updated action mappings - add missing ones:
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
            
            # ADD MISSING MAPPINGS:
            'onsite_conversion.post_save': 'post_save',
            'onsite_conversion.purchase': 'purchase',
            'onsite_app_purchase': 'purchase',  # Alternative purchase action
            'onsite_web_purchase': 'purchase',  # Alternative purchase action
            'omni_purchase': 'purchase',        # Alternative purchase action
            
            # For message_all, might need special logic to sum multiple message types
        }
        
        for action in actions:
            if isinstance(action, dict) and 'action_type' in action and 'value' in action:
                action_type = action.get('action_type')
                value = action.get('value')
                
                if action_type in action_mappings:
                    column_name = action_mappings[action_type]
                    
                    if column_name == 'link_click':
                        action_dict[column_name] = safe_convert_float(value, default=0.0)
                    elif column_name == 'purchase':
                        # Sum multiple purchase types
                        current_val = action_dict.get(column_name, 0)
                        action_dict[column_name] = current_val + safe_convert_int(value, default=0)
                    else:
                        action_dict[column_name] = safe_convert_int(value, default=0)
        
        return action_dict
    except:
        return {}

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

def process_video_data(row):
    """Process video data from Facebook API response"""
    video_fields = {
        'video_play_actions': 'video_play',
        'video_avg_time_watched_actions': 'video_avg_time_watched', 
        'video_p25_watched_actions': 'video_p25_watched',
        'video_p50_watched_actions': 'video_p50_watched',
        'video_p75_watched_actions': 'video_p75_watched',
        'video_p95_watched_actions': 'video_p95_watched',
        'video_p100_watched_actions': 'video_p100_watched'
    }
    
    video_data = {}
    
    for api_field, db_field in video_fields.items():
        video_array = row.get(api_field, [])
        if video_array and isinstance(video_array, list):
            for item in video_array:
                if isinstance(item, dict) and item.get('action_type') == 'video_view':
                    if db_field == 'video_avg_time_watched':
                        # Average time is usually in seconds, keep as int
                        video_data[db_field] = safe_convert_int(item.get('value'), default=0)
                    else:
                        # All other video metrics are counts
                        video_data[db_field] = safe_convert_int(item.get('value'), default=0)
                    break
        else:
            video_data[db_field] = 0
    
    return video_data

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
            "fields": "clicks,unique_clicks,account_id,reach,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,spend,impressions,frequency,actions,website_ctr,video_play_actions,video_avg_time_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions",
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

def create_fast_record(row, market=None):
    """Simplified record creation for speed - COMPLETE VERSION"""
    try:
        actions_data = safe_process_actions(row.get('actions', []))
        video_data = process_video_data(row)
        
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
            
            # Website CTR
            "website_ctr": extract_website_ctr(row.get("website_ctr", [])),
            
            # Raw actions for reference
            # "actions": json.dumps(row.get("actions", []), ensure_ascii=False) if row.get("actions") else None,
            
            "date": str(row.get("date_start", ""))[:10] if row.get("date_start") else None,
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "tf_partition_date": str(row.get("date_start", ""))[:10] if row.get("date_start") else None,
        }
        
        # Add video data
        record.update(video_data)
        
        # Add actions data
        record.update(actions_data)
        
        # Set defaults for missing fields
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
            "post_reaction": 0,
            "post_share": 0,
            "post_save": 0,
            "purchase": 0,
            "message_all": 0,
        }
        
        for field, default in essential_defaults.items():
            if field not in record:
                record[field] = default
        
        return {k: v for k, v in record.items() if v is not None}
        
    except Exception as e:
        logging.error(f"Error creating record: {e}")
        return None

def get_campaigns_from_database():
    """Get campaigns with exclusions"""
    excluded_campaigns = []
    
    # Create placeholders for IN clause
    # placeholders = ','.join(['%s'] * len(excluded_campaigns))
    
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



# def create_record(row, market=None):
#     """Create database record from API response"""
#     try:
#         actions_data = safe_process_actions(row.get('actions', []))
#         video_data = process_video_data(row)
        
#         record = {
#             "account_id": str(row.get("account_id", ""))[:50] or None,
#             "ad_account": str(row.get("account_name", ""))[:255] or None,
#             "ad_id": str(row.get("ad_id", ""))[:50] or None,
#             "ad_name": str(row.get("ad_name", ""))[:255] or None,
#             "adset_id": str(row.get("adset_id", ""))[:50] or None,
#             "adset_name": str(row.get("adset_name", ""))[:255] or None,
#             "campaign_id": str(row.get("campaign_id", ""))[:50] or None,
#             "campaign_name": str(row.get("campaign_name", ""))[:255] or None,
#             "objective": str(row.get("objective", ""))[:100] or None,
#             "market": str(market)[:255] if market else None,
            
#             "spend": safe_convert_float(row.get("spend"), default=0.0),
#             "reach": safe_convert_int(row.get("reach"), default=0),
#             "clicks": safe_convert_int(row.get("clicks"), default=0),
#             "impressions": safe_convert_int(row.get("impressions"), default=0),
#             "unique_clicks": safe_convert_int(row.get("unique_clicks"), default=0),
#             "frequency": safe_convert_float(row.get("frequency"), default=0.0),
            
#             # Website CTR
#             "website_ctr": extract_website_ctr(row.get("website_ctr", [])),
            
#             # Raw actions for reference
#             # "actions": json.dumps(row.get("actions", []), ensure_ascii=False) if row.get("actions") else None,
            
#             "date": str(row.get("date_start", ""))[:10] if row.get("date_start") else None,
#             "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
#             "tf_partition_date": str(row.get("date_start", ""))[:10] if row.get("date_start") else None,
#         }
        
#         # Add video data
#         record.update(video_data)
        
#         # Add actions data
#         record.update(actions_data)
        
#         # Set defaults for missing fields
#         essential_defaults = {
#             "page_engagement": 0,
#             "post_engagement": 0,
#             "link_click": 0.0,
#             "video_view": 0,
#             "page_like": 0,
#             "comment": 0,
#             "message": 0,
#             "total_messaging_connection": 0,
#             "complete_registration": 0,
#             "landing_page_view": 0,
#             "post_reaction": 0,
#             "post_share": 0,
#             "post_save": 0,
#             "purchase": 0,
#             "message_all": 0,
#         }
        
#         for field, default in essential_defaults.items():
#             if field not in record:
#                 record[field] = default
        
#         return {k: v for k, v in record.items() if v is not None}
        
#     except Exception as e:
#         logging.error(f"Error creating record: {e}")
#         return None


# def get_campaign_info(campaign_id):
#     """Get campaign info from database"""
#     query = """
#         SELECT 
#             campaign_id,
#             access_token,
#             market,
#             campaign_name
#         FROM 
#             fb_ad_campaigns
#         WHERE 
#             campaign_id = %s
#             AND access_token IS NOT NULL 
#             AND access_token != ''
#     """
    
#     try:
#         mdb = MariaDBHandler()
#         result = mdb.read_from_db(
#             database=TARGET_DATABASE,
#             query=query,
#             params=(campaign_id,),
#             output_type="list_of_dicts"
#         )
        
#         if result:
#             campaign = result[0]
#             logging.info(f"✅ Found campaign: {campaign['campaign_name']} ({campaign['market']})")
#             return campaign
#         else:
#             logging.error(f"❌ Campaign {campaign_id} not found in database")
#             return None
            
#     except Exception as e:
#         logging.error(f"❌ Error getting campaign info: {e}")
#         return None

# def extract_single_campaign(campaign_id, start_date, end_date):
#     """Extract insights for single campaign"""
#     # Get campaign info
#     campaign = get_campaign_info(campaign_id)
#     if not campaign:
#         return
    
#     access_token = campaign['access_token']
#     market = campaign['market']
#     campaign_name = campaign['campaign_name']
    
#     logging.info(f"🚀 Extracting insights for campaign: {campaign_name}")
#     logging.info(f"📅 Date range: {start_date} to {end_date}")
#     logging.info(f"🏷️  Market: {market}")
    
#     try:
#         # Initialize Facebook API handler
#         fb_handler = FacebookAPIHandler(access_token=access_token, timeout=120, max_retries=3)
        
#         endpoint = f"{campaign_id}/insights"
#         params = {
#             "fields": "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,reach,clicks,impressions,unique_clicks,frequency,date_start,actions,objective,website_ctr,video_play_actions,video_avg_time_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_p100_watched_actions",
#             "level": "ad",
#             "time_increment": 1,
#             "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
#             "limit": 1000,
#             "filtering": '''[{"field": "spend", "operator": "GREATER_THAN", "value": 0}]'''
#         }
        
#         logging.info("📡 Making API request...")
#         data = fb_handler.get_all(endpoint=endpoint, params=params)
        
#         if not data:
#             logging.warning("📭 No data returned from API")
#             return
        
#         logging.info(f"📊 Retrieved {len(data)} records from API")
        
#         # Process records
#         records = []
#         for row in data:
#             record = create_record(row, market=market)
#             if record:
#                 records.append(record)
        
#         logging.info(f"✅ Processed {len(records)} valid records")
        
#         # Save to database in batches
#         if records:
#             batch_size = 100
#             total_saved = 0
            
#             for i in range(0, len(records), batch_size):
#                 batch = records[i:i+batch_size]
                
#                 try:
#                     mdb = MariaDBHandler()
#                     mdb.insert_and_update_from_dict(
#                         database=TARGET_DATABASE,
#                         table=TARGET_TABLE,
#                         data=batch,
#                         unique_columns=["ad_id", "date"],
#                         log=False
#                     )
                    
#                     total_saved += len(batch)
#                     logging.info(f"💾 Saved batch {i//batch_size + 1}: {len(batch)} records")
                    
#                 except Exception as e:
#                     logging.error(f"❌ Batch save failed: {e}")
#                     continue
            
#             logging.info(f"🎯 COMPLETED: {total_saved}/{len(records)} records saved to database")
            
#             # Show sample data
#             if records:
#                 sample = records[0]
#                 logging.info(f"📋 Sample record:")
#                 logging.info(f"   Ad ID: {sample.get('ad_id')}")
#                 logging.info(f"   Date: {sample.get('date')}")
#                 logging.info(f"   Spend: ${sample.get('spend', 0):.2f}")
#                 logging.info(f"   Impressions: {sample.get('impressions', 0):,}")
#                 logging.info(f"   Clicks: {sample.get('clicks', 0):,}")
#                 logging.info(f"   Video Play: {sample.get('video_play', 0):,}")
#                 logging.info(f"   Website CTR: {sample.get('website_ctr', 0):.2f}%")
        
#     except Exception as e:
#         logging.error(f"❌ Extraction failed: {e}")
#         import traceback
#         traceback.print_exc()
#     finally:
#         if 'fb_handler' in locals():
#             fb_handler.close()

# def main():
#     """Main function - extract single campaign"""
#     logging.info(f"🎯 Single Campaign Facebook Insights Extractor")
#     logging.info(f"📋 Target Campaign: {120238384811600112}")
#     logging.info(f"🗄️  Database: {TARGET_DATABASE}.{TARGET_TABLE}")
    
#     start_time = datetime.now()
    
#     # Extract the campaign
#     extract_single_campaign(120238384811600112, START_DATE, END_DATE)
    
#     # Final summary
#     duration = datetime.now() - start_time
#     logging.info(f"⏱️  Total duration: {duration}")
#     logging.info(f"✅ Extraction completed!")


if __name__ == "__main__":
    main()