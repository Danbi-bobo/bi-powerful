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

POS_BASE_URL = os.getenv("POS_API_URL")


class PosAPIHandler:
    def __init__(self, shop_id: str, api_key: str, timeout: int = 60, max_retries: int = 3):
        self.shop_id = shop_id
        self.api_key = api_key
        self.client = HttpClient(timeout=timeout)
        self.max_retries = max_retries
    
    def get_all(self, endpoint: str, params: Dict[str, Any] = None):
        params = params or {}
        params["api_key"] = self.api_key
        params.setdefault("page_number", 1)  # Đảm bảo page_number tồn tại

        results = []

        while True:
            retries = 0
            while retries < self.max_retries:  
                try:
                    response = self.client.get(f"{POS_BASE_URL}/{self.shop_id}/{endpoint}", params=params)
                    data = self._handle_response(response)

                    if "data" in data:
                        results.extend(data["data"])

                    # Lấy thông tin phân trang
                    total_pages = data.get("total_pages", 1)
                    current_page = data.get("page_number", params["page_number"])

                    if current_page < total_pages:
                        params["page_number"] += 1
                        time.sleep(1)
                        break
                    else:
                        return results  # Trả về kết quả khi lấy hết data

                except Exception as e:
                    retries += 1
                    if retries < self.max_retries:
                        wait_time = 2 ** retries
                        logging.info(f"Retry {retries}/{self.max_retries} after {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logging.error(f"❌ API failed after {self.max_retries} retries. Returning partial data.")
                        return results
            
    def get_one(self, endpoint: str, params: Dict[str, Any] = None):
        params = params or {}
        params["api_key"] = self.api_key
        url = f"{POS_BASE_URL}/{self.shop_id}/{endpoint}"
        
        response = self.client.get(url, params=params)
        data = self._handle_response(response)
        
        if 'data' in data:
            return data['data']
        else:
            return []
    
    def update_order(self, order_id, data: Dict):
        params = {
            'api_key': self.api_key
        }
        url = f"{POS_BASE_URL}/{self.shop_id}/orders/{order_id}"
        res = self.client.put(
            url = url,
            params=params,
            data = data
        )
        data = self._handle_response(res)
        if res.status_code == 200:
            return {'status': f'successfully update order {order_id}'}
        elif res.status_code == 422:
            return {'status': f'try again later'}
        else:
            return {'status': 'error'}
        

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
