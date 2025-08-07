account_transaction = '''
WITH daily_summary AS (
	SELECT
		date(transaction_time) AS `Ngày`
		, SUM(CASE WHEN type = 'Account Top-up' THEN amount ELSE 0 END) AS `Nạp vào TK thẻ`
		, SUM(CASE WHEN type = 'Card Top-up' THEN amount ELSE 0 END) AS `Nạp tiền vào thẻ`
		, SUM(CASE WHEN type = 'Card Withdrawal' THEN amount ELSE 0 END) AS `Rút tiền từ thẻ`
		, SUM(CASE WHEN type NOT IN ('Card Withdrawal', 'Card Top-up', 'Account Top-up') THEN amount ELSE 0 END) AS `Phí khác`
	FROM
		alomix_skyward_data.pingpong_account_transactions
	WHERE
		transaction_time >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
	GROUP BY
		date(transaction_time)
),
daily_balance AS (
	SELECT
	    `Ngày`,
	    balance `Số dư`
	FROM (
	    SELECT
	        DATE(transaction_time) AS `Ngày`,
	        balance,
	        ROW_NUMBER() OVER (PARTITION BY DATE(transaction_time) ORDER BY transaction_time DESC) AS rn
	    FROM
	        alomix_skyward_data.pingpong_account_transactions
	    WHERE
	        transaction_time >= CURRENT_DATE - INTERVAL 30 DAY
	) AS ranked
	WHERE
	    rn = 1
)
SELECT
	UNIX_TIMESTAMP(a.`Ngày`) * 1000 AS `Ngày`,
	a.`Nạp vào TK thẻ`,
	a.`Nạp tiền vào thẻ`,
	a.`Rút tiền từ thẻ`,
	a.`Phí khác`,
	b.`Số dư`
FROM
	daily_summary a
	INNER JOIN daily_balance b ON a.`Ngày` = b.`Ngày`
'''

card_transaction = '''
WITH pivot_data AS (
	SELECT
		UNIX_TIMESTAMP(DATE(transaction_time)) * 1000 AS `Ngày`
		, last4 AS `Thẻ`
		, SUM(
        	CASE 
            	WHEN transaction_type = 'Deposit/Withdrawal' 
					AND `type` = 'Top-up' 
					AND status <> 'Declined' 
                THEN amount ELSE 0 
            END
		) AS `Nạp vào thẻ`
		, SUM(
        	CASE 
            	WHEN transaction_type = 'Deposit/Withdrawal' 
                	AND `type` = 'Withdrawal' 
                    AND status <> 'Declined' 
                THEN amount ELSE 0 END
        ) AS `Rút từ thẻ`
		, SUM(
        	CASE 
            	WHEN transaction_type = 'Payment' 
                	AND `type` = 'Auth' 
                    AND status <> 'Declined' 
                THEN amount ELSE 0 END
        ) AS `Thanh toán thẻ`
		, SUM(
        	CASE 
            	WHEN transaction_type = 'Payment' 
                	AND `type` = 'Reversal' 
                    AND status <> 'Declined' 
                THEN -1 * amount ELSE 0 END
        ) AS `Hoàn tiền`
		, CAST(
        	SUM(
            	CASE 
					WHEN `status` = 'Declined' 
					THEN 1 ELSE 0 
                END
            ) AS DOUBLE
        ) AS `Số lượng giao dịch bị từ chối`
		, SUM(
        	CASE 
            	WHEN `status` = 'Declined'
				THEN COALESCE(decline_fee, 0) ELSE 0 END
        ) AS `Decline fee`
	FROM alomix_skyward_data.pingpong_card_transactions
	GROUP BY DATE(transaction_time), last4
),
daily_balance AS (
	SELECT
		`Ngày`,
		`Thẻ`,
		`Nạp vào thẻ`,
		`Rút từ thẻ`,
		`Thanh toán thẻ`,
		`Hoàn tiền`,
		SUM(`Nạp vào thẻ` - `Rút từ thẻ` - `Thanh toán thẻ` + `Hoàn tiền`) OVER (
			PARTITION BY `Thẻ`
			ORDER BY `Ngày`
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS `Số dư`
		, `Số lượng giao dịch bị từ chối`
		, `Decline fee`
	FROM pivot_data
)
SELECT *
FROM daily_balance
ORDER BY `Thẻ`, `Ngày`;
'''

topas_global_account = '''
WITH daily_summary AS (
    SELECT
        DATE(transaction_time) AS date,
        currency,
        SUM(CASE WHEN type = 'Receive' THEN amount ELSE 0 END) AS received,
        SUM(CASE WHEN type = 'Send' THEN -amount ELSE 0 END) AS sent,
        SUM(amount) AS net_change
    FROM
        alomix_skyward_data.pingpong_global_account_transactions
    WHERE
        status = 'Success'
        AND market = 'TOPAS'
    GROUP BY
        DATE(transaction_time), currency
)
SELECT
    UNIX_TIMESTAMP(date) * 1000 AS `Ngày`,
    currency AS `Tiền tệ`,
    received AS `Tiền vào`,
    sent AS `Tiền ra`,
    net_change AS `Biến động ngày`,
    SUM(net_change) OVER (
        PARTITION BY currency
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS `Số dư`
FROM
    daily_summary
ORDER BY
    currency, date;
'''

seto_phil_global_account = '''
WITH daily_summary AS (
    SELECT
        DATE(transaction_time) AS date,
        currency,
        SUM(CASE WHEN type = 'Receive' THEN amount ELSE 0 END) AS received,
        SUM(CASE WHEN type = 'Send' THEN -amount ELSE 0 END) AS sent,
        SUM(amount) AS net_change
    FROM
        alomix_skyward_data.pingpong_global_account_transactions
    WHERE
        status = 'Success'
        AND market = 'SETO PHIL'
    GROUP BY
        DATE(transaction_time), currency
)
SELECT
    UNIX_TIMESTAMP(date) * 1000 AS `Ngày`,
    currency AS `Tiền tệ`,
    received AS `Tiền vào`,
    sent AS `Tiền ra`,
    net_change AS `Biến động ngày`,
    SUM(net_change) OVER (
        PARTITION BY currency
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS `Số dư`
FROM
    daily_summary
ORDER BY
    currency, date;
'''