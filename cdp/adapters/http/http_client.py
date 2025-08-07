import httpx
import logging
import json

class HttpClient:
    def __init__(self, timeout=60):
        self.client = httpx.Client(timeout=timeout)

    def get(self, url, params=None, headers=None):
        try:
            res = self.client.get(url, params=params, headers=headers)
            res.raise_for_status()  # Tự động raise lỗi nếu HTTP status code >= 400
            return res
        except httpx.HTTPStatusError as e:
            try:
                response_data = json.loads(e.response.text)
                response_text = json.dumps(response_data, ensure_ascii=False)
            except json.JSONDecodeError:
                response_text = e.response.text

            logging.error(f"""❌ HTTP error {e.response.status_code} while requesting {url!r}.\nResponse: {response_text}""")
            raise e
        except httpx.RequestError as e:
            logging.error(f"❌ Request error: {e}")
            raise e

    def post(self, url, data, params=None, headers=None):
        try:
            res = self.client.post(url, json=data, params=params, headers=headers)
            res.raise_for_status()
            return res
        except httpx.RequestError as e:
            logging.error(f"❌ An error occurred while requesting {e.request.url!r}.")
            raise e

    def put(self, url, data, params=None, headers=None):
        try:
            res = self.client.put(url, json=data, params=params, headers=headers)
            res.raise_for_status()
            return res
        except httpx.RequestError as e:
            logging.error(f"❌ An error occurred while requesting {e.request.url!r}.")
            raise e

    def close(self):
        self.client.close()