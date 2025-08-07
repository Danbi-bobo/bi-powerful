mapping_dict = {
    'Anousith': {
        'table_name': 'lark_seto_laos_partner_delivery_status',
        'file_type': 'html',
        'mapping_dict': {
            'bill_code': {'path': 'ເລກບິນ', 'type': 'str'},
            'bill_name': {'path': 'ຊື່ຜູ້ຮັບ', 'type': 'str'},
            'bill_phone': {'path': 'ເບີຜູ້ຮັບ', 'type': 'str'},
            'hub_arrival_date': {'path': 'date_from_file_name', 'type': 'date', 'format': "%Y%m%d"}
        }
    },
    'HAL': {
        'table_name': 'lark_seto_laos_partner_delivery_status',
        'file_type': 'xlsx',
        'mapping_dict': {
            'bill_code': {'path': 'ເລກທີບິນ', 'type': 'str'},
            'bill_name': {'path': 'ຜູ້ຮັບ', 'type': 'str'},
            'bill_phone': {'path': 'ເບີຜູ້ຮັບ', 'type': 'str'},
            'hub_arrival_date': {'path': 'ວັນທີເຄື່ອງຮອດ', 'type': 'date', 'format': "%d/%m/%Y %H:%M"}
        }
    }
}