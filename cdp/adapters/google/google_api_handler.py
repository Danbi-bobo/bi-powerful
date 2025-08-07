import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from typing import Literal
from cdp.domain.utils.udfs import gaql_query_handler
from enum import Enum

class GoogleAdsHandler:
    def __init__(self, developer_token: str, client_id: str, client_secret: str, refresh_token: str, mcc_id: str):
        self.developer_token = developer_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.mcc_id = mcc_id

        self.client = GoogleAdsClient.load_from_dict({
            "developer_token": self.developer_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "use_proto_plus": True
        })

    def run_query(self, query: str, customer_id: str = None, output_type: Literal["dataframe", "list"] = "dataframe", boolean_as: Literal['bool', 'int'] = 'bool', enum_as: Literal["name", "value"] = "name"):
        new_query, field_mappings, full_field_list = gaql_query_handler(query)
        google_ads_service = self.client.get_service("GoogleAdsService")
        metadata = [("login-customer-id", str(self.mcc_id))] if customer_id and customer_id != self.mcc_id else []
        response = google_ads_service.search(customer_id=customer_id or self.mcc_id, query=new_query, metadata=metadata)

        results = []
        for row in response:
            row_dict = {}

            for full_field in full_field_list:
                parts = full_field.split(".")
                obj = row

                for attr in parts:
                    obj = getattr(obj, attr, None) if obj else None  

                if isinstance(obj, bool):
                    obj = obj if boolean_as == 'bool' else int(obj)
                elif isinstance(obj, Enum):
                    obj = obj.name if enum_as == "name" else obj.value

                row_dict[field_mappings[full_field]] = obj

            results.append(row_dict)

        if output_type == "dataframe":
            return pd.DataFrame(results) 
        return results

