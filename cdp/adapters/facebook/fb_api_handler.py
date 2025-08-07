import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH", "")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from typing import Dict, Any
from cdp.adapters.http.http_client import HttpClient
import time
import logging

api_version = os.getenv("FB_API_VERSION")

class FacebookAPIHandler:
    BASE_URL = f"https://graph.facebook.com/{api_version}"

    def __init__(self, access_token: str, timeout: int = 60, max_retries: int = 3):
        self.access_token = access_token
        self.client = HttpClient(timeout=timeout)
        self.max_retries = max_retries
    
    def get_all(self, endpoint: str, params: Dict[str, Any] = None):
        params = params or {}
        params["access_token"] = self.access_token
        url = f"{self.BASE_URL}/{endpoint}"
        results = []
        
        while True:
            retries = 0
            while retries < self.max_retries:
                try:
                    response = self.client.get(url, params=params)
                    data = self._handle_response(response)

                    if 'data' in data:
                        results.extend(data['data'])
                    else:
                        results.append(data)
                    
                    if data.get("paging", {}).get("next"):
                        params['after'] = data.get("paging", {}).get('cursors', {}).get('after')
                        time.sleep(4)
                    else:
                        return results
                
                except Exception as e:
                    retries += 1
                    if retries < self.max_retries:
                        wait_time = 4 ** retries
                        logging.info(f"Retry {retries}/{self.max_retries} after {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logging.info(f"❌ Failed after {self.max_retries} retries. Skipping...")
                        return results
    
    def get_one(self, endpoint: str, params: Dict[str, Any] = None):
        params = params or {}
        params["access_token"] = self.access_token
        url = f"{self.BASE_URL}/{endpoint}"

        response = self.client.get(url, params=params)
        data = self._handle_response(response)
        if 'data' in data:
            return data['data']
        else:
            return []

    def post(self, endpoint: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
        data = data or {}
        data["access_token"] = self.access_token
        url = f"{self.BASE_URL}/{endpoint}"
        response = self.client.post(url, data=data)
        return self._handle_response(response)
    
    def delete(self, endpoint: str) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint}"
        response = self.client.get(url, params={"access_token": self.access_token})
        return self._handle_response(response)
    
    def _handle_response(self, response) -> Dict[str, Any]:
        try:
            return response.json()
        except ValueError:
            return {"error": "Invalid JSON response", "details": response.text}
    
    def close(self):
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
