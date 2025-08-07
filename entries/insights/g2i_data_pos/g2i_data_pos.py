import sys
import os
from dotenv import load_dotenv
import requests
import time
load_dotenv()
from datetime import datetime
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from cdp.adapters.lark_suite.lark_api_handler import LarkApiHandle
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from queries import resell, cv
from cdp.domain.utils.log_helper import setup_logger
setup_logger(__file__)

db_golden_name = os.getenv("DB_GOLDEN_NAME")
table_name = "pos_orders"
lark_api_handler = LarkApiHandle()

orders = MariaDBHandler().read_from_db(query=cv, output_type='dataframe')

orders_dict = orders.to_dict(orient="records")
i = 1
for order in orders_dict:
    inserted_at = order['Thời điểm tạo đơn']
    product_name = order['Sản phẩm']
    bill_full_name = order['Tên Khách hàng']
    bill_phone_number = order['Số điện thoại']
    address = order['Địa chỉ']
    gender = order['Giới tính']
    destiny =  order['Mệnh']
    formated_date = datetime.strftime(inserted_at, '%d-%m-%Y %H:%M:%S')
    account = order['Nguồn đơn'] if order['Nguồn đơn'].isdigit() else None
    note = order['note']

    note = f'''Mệnh: {destiny},
Giới tính: {gender},
Ngày tạo đơn cũ: {formated_date},
Sản phẩm đơn cũ: {product_name},
Note cũ: {note}
'''

    if account:
        try:
            create_order = requests.post(
                url = 'https://pos.pages.fm/api/v1/shops/1021208973/orders',
                params={'api_key': '7d5e02afce28465a877da4804b1f8e7d'},
                json={
                    'bill_full_name': bill_full_name,
                    'bill_phone_number': bill_phone_number,
                    'note': note,
                    'shipping_address': {
                        'address': address
                    },
                    'account': account
                }
            )
            print('Created Order: ' + str(create_order.json().get('data').get('id') or None))
            time.sleep(0.25)
        except Exception as e:
            print(f"Lỗi không xác định: {e}")
            continue