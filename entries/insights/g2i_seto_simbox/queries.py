query = '''
WITH raw AS (
	SELECT
		CASE
			WHEN call_type = 'Inbound' THEN call_to
			ELSE call_from
		END AS 'Slot'
		, CASE
			WHEN call_type = 'Outbound' THEN call_to
			ELSE call_from
		END AS 'phone'
		, time_start
		, call_type
		, call_duration
		, talk_duration
		, recording
		, call_status
		, call_id
	FROM
		alomix_seto_data.simbox_call_data
	WHERE
		time_start >= DATE_SUB(CURRENT_DATE, INTERVAL 10 DAY)
        AND call_status = 'ANSWERED'
        AND talk_duration >= 20
)
SELECT
	Slot
	, CASE
    	WHEN phone LIKE '+63%' THEN CONCAT('0', SUBSTRING(phone, 4))
		WHEN phone LIKE '63%' THEN CONCAT('0', SUBSTRING(phone, 3))
		WHEN phone LIKE '0063%' THEN CONCAT('0', SUBSTRING(phone, 5))
        WHEN phone NOT LIKE '0%' THEN CONCAT('0', phone)
		ELSE phone
	END AS Phone
	, UNIX_TIMESTAMP(time_start) * 1000 AS `Time Start`
	, call_type AS `Type`
	, call_duration AS `Call Duration`
	, talk_duration AS `Talk Duration`
	, recording AS `Recording Name`
	, call_status AS `Status`
	, call_id AS `Call ID`
FROM
	raw
'''