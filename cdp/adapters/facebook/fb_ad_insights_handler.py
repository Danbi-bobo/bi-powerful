import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.facebook.fb_api_handler import FacebookAPIHandler
import pandas as pd
import numpy as np
import time

db_golden_name = os.getenv("DB_GOLDEN_NAME")
golden_table_name = "fb_ad_insights"
db_raw_name = os.getenv("DB_RAW_NAME")
raw_table_name = "raw_fb_ad_insights"

def action_handler(df):
    df = df.copy()
    df.loc[:, 'actions'] = df['actions'].apply(lambda x: x if isinstance(x, list) else [])

    actions_expanded = df['actions'].apply(lambda x: {action['action_type']: action['value'] for action in x}).apply(pd.Series)

    df_cleaned = pd.concat([df.drop(columns=['actions']), actions_expanded], axis=1)

    df_cleaned[['ad_id', 'date_start']] = df[['ad_id', 'date_start']]

    return df_cleaned

def video_data_handler(df):
    video_columns = [
        'website_ctr',
        'video_play_actions', 
        'video_avg_time_watched_actions', 
        'video_p25_watched_actions', 
        'video_p50_watched_actions',
        'video_p75_watched_actions',
        'video_p95_watched_actions',
        'video_p100_watched_actions'
    ]
    
    video_data = {}

    def process_video_column(col):
        if col in df.columns:
            df[col] = df[col].apply(lambda x: {action['action_type']: action['value'] for action in x} if isinstance(x, list) else {})
            video_expanded = df[col].apply(pd.Series)
            video_expanded.columns = [f"{col}_{col_name}" for col_name in video_expanded.columns]
            return video_expanded
        return pd.DataFrame()

    for col in video_columns:
        video_data[col] = process_video_column(col)
    
    df_video = pd.concat(video_data.values(), axis=1)
    
    df_video[['ad_id', 'date_start']] = df[['ad_id', 'date_start']]
    
    return df_video

def get_ad_accounts_insights(ad_account_dict, fb_token, params):
    dfs = []

    for ad_account in ad_account_dict:
        account_id = ad_account['id']

        team = ad_account['team']
        token = fb_token.get(team)
        insight_endpoint = f'{account_id}/insights'

        if token:
            fb_api_handler = FacebookAPIHandler(token)
            try:
                insights = fb_api_handler.get_all(endpoint=insight_endpoint, params=params.copy())

                df = pd.DataFrame(insights)
                df[['team', 'account_id']] = team, account_id
                dfs.append(df)
                time.sleep(4)
            except Exception as e:
                continue
    return dfs

def prepare_golden_df(raw_df):
    action_df = action_handler(raw_df[['actions', 'ad_id', 'date_start']])
    video_df = video_data_handler(raw_df)
    golden_df = raw_df.merge(action_df, on=['ad_id', 'date_start'], how='left').merge(video_df, on=['ad_id', 'date_start'], how='left')
    golden_df = golden_df.loc[:, golden_df.columns.intersection([
        'account_id', 'team', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name',
        'spend', 'reach', 'clicks', 'impressions', 'unique_clicks', 'frequency', 'objective', 'optimization_goal', 'date_start',
        'page_engagement', 'landing_page_view', 'like', 'post_reaction', 'link_click', 'post', 'comment',
        'onsite_conversion.messaging_first_reply', 'onsite_conversion.total_messaging_connection',
        'offsite_conversion.fb_pixel_complete_registration',
        'website_ctr_link_click', 'video_play_actions_video_view', 'video_avg_time_watched_actions_video_view',
        'video_p25_watched_actions_video_view', 'video_p50_watched_actions_video_view', 'video_p75_watched_actions_video_view',
        'video_p95_watched_actions_video_view', 'video_p100_watched_actions_video_view'
    ])]

    rename_dict = {
        'date_start': 'date',
        'like': 'page_like',
        'post': 'post_share',
        'comment': 'post_comment',
        'onsite_conversion.messaging_first_reply': 'messaging_first_reply',
        'onsite_conversion.total_messaging_connection': 'total_messaging_connection',
        'offsite_conversion.fb_pixel_complete_registration': 'fb_pixel_complete_registration',
        'website_ctr_link_click': 'website_ctr',
        'video_play_actions_video_view': 'video_play',
        'video_avg_time_watched_actions_video_view': 'video_avg_time_watched',
        'video_p25_watched_actions_video_view': 'video_p25_watched',
        'video_p50_watched_actions_video_view': 'video_p50_watched',
        'video_p75_watched_actions_video_view': 'video_p75_watched',
        'video_p95_watched_actions_video_view': 'video_p95_watched',
        'video_p100_watched_actions_video_view': 'video_p100_watched'
    }

    golden_df.rename(columns=rename_dict, inplace=True, errors='ignore')

    numeric_cols = [
        'spend', 'reach', 'clicks', 'impressions', 'unique_clicks', 'frequency',
        'total_messaging_connection', 'page_engagement', 'landing_page_view',
        'post_comment', 'messaging_first_reply', 'page_like', 'post_reaction',
        'fb_pixel_complete_registration', 'link_click', 'post_share',
        'website_ctr', 'video_play', 'video_avg_time_watched',
        'video_p25_watched', 'video_p50_watched', 'video_p75_watched',
        'video_p95_watched', 'video_p100_watched'
    ]

    # Lọc ra các cột số có trong DataFrame
    existing_num_cols = [col for col in numeric_cols if col in golden_df.columns]
    golden_df[existing_num_cols] = golden_df[existing_num_cols].apply(pd.to_numeric, errors='coerce')

    if 'date' in golden_df.columns:
        golden_df['date'] = pd.to_datetime(golden_df['date'], errors='coerce')

    # Lọc ra các cột chuỗi có trong DataFrame
    existing_str_cols = list(set(golden_df.columns) - set(existing_num_cols) - {'date'})
    golden_df[existing_str_cols] = golden_df[existing_str_cols].astype(str)

    golden_df.replace(np.nan, None, inplace=True)

    return golden_df

def get_insights(date_preset):
    get_ad_accounts_query = '''
        SELECT
            id
            , team
        FROM
            lollibooks_data_golden.fb_ad_accounts
        WHERE
            account_status IN (1, 3)
    '''
    insight_params = {
        'fields': 'campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name, spend, reach, clicks, impressions, unique_clicks, frequency, actions, objective, optimization_goal, website_ctr, video_play_actions, video_avg_time_watched_actions, video_p25_watched_actions, video_p50_watched_actions, video_p75_watched_actions, video_p95_watched_actions, video_p100_watched_actions',
        'level': 'ad',
        'date_preset': date_preset,
        'time_increment': 1,
        'limit': 200,
        'filtering': '''[
            {
                "field": "spend",
                "operator": "GREATER_THAN",
                "value": 0
            }
        ]'''
    }

    fb_token = LarkApiHandle().get_fb_tokens_in_lark()
    ad_accounts = MariaDBHandler().read_from_db(database=db_golden_name, query=get_ad_accounts_query, output_type='list_of_dicts')

    all_insights = get_ad_accounts_insights(ad_accounts, fb_token, insight_params)
    raw_df = pd.concat(all_insights, ignore_index=True) if all_insights else pd.DataFrame()

    golden_df = prepare_golden_df(raw_df)

    if not raw_df.empty:
        raw_df = raw_df.astype(str)
        raw_df['date_start'] = pd.to_datetime(raw_df['date_start'], errors='coerce')
        raw_df['date_stop'] = pd.to_datetime(raw_df['date_stop'], errors='coerce')
        MariaDBHandler().insert_and_update_from_df(database=db_raw_name, table=raw_table_name, df=raw_df, unique_columns=["ad_id", "date_start"])
    if not golden_df.empty:
        MariaDBHandler().insert_and_update_from_df(database=db_golden_name, table=golden_table_name, df=golden_df, unique_columns=["ad_id", "date"], log=True)