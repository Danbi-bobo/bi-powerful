import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.http.http_client import HttpClient
from fuzzy_search import match_address
import logging

def maps_geocoding(api_key, address):
    res = HttpClient(timeout=10).get(
        url='https://maps.googleapis.com/maps/api/geocode/json',
        params={
            'key': api_key,
            'address': address
        }
    )

    return res.json()

def maps_findplacefromtext(api_key, address):
    res = HttpClient(timeout=10).get(
        url='https://maps.googleapis.com/maps/api/place/findplacefromtext/json',
        params={
            'key': api_key,
            'fields': 'place_id,name,formatted_address,geometry',
            'inputtype': 'textquery',
            'input': address
        }
    )
    return res.json()

def handle_maps_data(address_df, raw_data, api_key):
    province_name = raw_data['data']['shipping_address']['province_name']
    district_name = raw_data['data']['shipping_address'].get('district_name', None)
    full_address = raw_data['data']['shipping_address']['full_address']

    geocoding_results = maps_geocoding(api_key, full_address)
    geocoding_results = geocoding_results.get('results', []) or []
    if geocoding_results:
        for address in geocoding_results:
            formatted_address = address['formatted_address']
            fuzzy_result, status = match_address(
                province_input=province_name,
                district_input=district_name,
                address_detail=formatted_address,
                admin_df=address_df
            )
            if status == 'OK_EXACT':
                return fuzzy_result
        
    
    # findplacefromtext_results = maps_findplacefromtext(api_key, full_address)
    # findplacefromtext_results = findplacefromtext_results.get('candidates', []) or []
    # if findplacefromtext_results:
    #     for address in findplacefromtext_results:
    #         formatted_address = address['formatted_address']
    #         fuzzy_result, status = match_address(
    #             province_input=province_name,
    #             district_input=district_name,
    #             address_detail=formatted_address,
    #             admin_df=address_df
    #         )
    #         if status == 'OK_EXACT':
    #             return fuzzy_result

    if geocoding_results:
        logging.info('no result matched, call chat gpt')
        HttpClient().post(
            url = 'https://auto.skywardvn.com/webhook/direct_phil/find_barangay',
            data={
                'raw': raw_data,
                'data': geocoding_results
            }
        )
    return None

def maps_handle_address(api_key, data, address_df, maps_search_tag_id):
    tag_ids = data['data'].get('tags', []) or []
    
    if maps_search_tag_id not in tag_ids and 430 not in tag_ids:
        result = handle_maps_data(address_df, data, api_key)
        if result:
            tag_ids.append(maps_search_tag_id)
            data['data']['tags'] = list(set(tag_ids))
            data['data']['shipping_address']['district_id'] = result['district_id']
            data['data']['shipping_address']['commune_id'] = result['commune_id']

            return data
        else:
            tag_ids.append(430)
            data['data']['tags'] = list(set(tag_ids))
            return data
    return None