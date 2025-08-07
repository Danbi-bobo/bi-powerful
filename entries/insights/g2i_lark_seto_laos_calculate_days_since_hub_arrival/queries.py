query = '''
	SELECT
		bill_code
		, UNIX_TIMESTAMP(hub_arrival_date) * 1000 AS hub_arrival_date
		, DATEDIFF(CURRENT_DATE, hub_arrival_date) AS days_since_hub_arrival
	FROM
		alomix_seto_data.lark_seto_laos_partner_delivery_status
	WHERE
		hub_arrival_date >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY)
'''