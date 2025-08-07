mapping_lark_fields = {
    'Biến động Tài khoản Thẻ': 'account_transaction',
    'Biến động Nạp/Rút Thẻ': 'card_withdrawal',
    'Biến động Thanh toán Thẻ': 'card_transaction',
    'Biến động Tài khoản Tổng Phil': 'global_account_transaction',
    'Biến động Tài khoản Tổng Topas': 'global_account_transaction'
}

mapping_dict = {
    'account_transaction': {
        'table_name': 'pingpong_account_transactions',
        'columns': {
            'string': ['transaction_id', 'type', 'last4', 'card_remark', 'currency'],
            'numeric': ['amount', 'balance', 'outbound_fee'],
            'datetime': ['transaction_time']
        },
        'rename_dict': {
            'Record ID': 'transaction_id',
            'Type': 'type',
            'Last4': 'last4',
            'Card Remark': 'card_remark',
            'Currency': 'currency',
            'Amount': 'amount',
            'Account Balance': 'balance',
            'Outbound Fee': 'outbound_fee',
            'Accounting Time': 'transaction_time'
        }
    },
    'card_transaction': {
        'table_name': 'pingpong_card_transactions',
        'columns': {
            'string': ['transaction_id', 'status', 'card_number', 'merchant_name', 'nickname', 'currency', 'merchant_currency', 'type', 'transaction_fee'],
            'numeric': ['amount', 'merchant_amount'],
            'datetime': ['transaction_time']
        },
        'rename_dict': {
            'Transaction ID': 'transaction_id',
            'Authorization Status': 'status',
            'Type': 'type',
            'Card Number': 'card_number',
            'Billing Currency': 'currency',
            'Billing Amount': 'amount',
            'Transaction Fee': 'transaction_fee',
            'Merchant Name': 'merchant_name',
            'Merchant Currency': 'merchant_currency',
            'Merchant Amount': 'merchant_amount',
            'Transaction Date(GMT +8)': 'transaction_time',
            'Nickname': 'nickname'  
        }
    },
    'card_withdrawal': {
        'table_name': 'pingpong_card_transactions',
        'columns': {
            'string': ['transaction_id', 'card_number', 'type', 'status', 'detail', 'note'],
            'numeric': ['amount'],
            'datetime': ['transaction_time']
        },
        'rename_dict': {
            'Record ID': 'transaction_id',
            'Card Number': 'card_number',
            'Type': 'type',
            'Status': 'status',
            'Detail': 'detail',
            'Note': 'note',
            'Amount（USD）': 'amount',
            'Create Time': 'transaction_time'
        }
    },
    'global_account_transaction': {
        'table_name': 'pingpong_global_account_transactions',
        'columns': {
            'string': ['currency', 'transaction_id', 'card_no', 'type', 'status', 'note', 'counterparty', 'fee', 'net'],
            'numeric': ['amount'],
            'datetime': ['transaction_time']
        },
        'rename_dict': {
            'TransactionId': 'transaction_id',
            'CardNo': 'card_no',
            'Type': 'type',
            'Status': 'status',
            'Note': 'note',
            'Currency': 'currency',
            'Time': 'transaction_time',
            'Fee': 'fee',
            'Net': 'net',
            'Amount': 'amount',
            'From/To': 'counterparty'
        }
    }
}
