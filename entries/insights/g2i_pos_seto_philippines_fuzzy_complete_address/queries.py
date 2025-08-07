list_address = '''
SELECT
	province_name
    , district_name
    , district_id
    , commune_name
    , commune_id
FROM
	alomix_skyward_data.pos_geo_locations
WHERE
	province_id LIKE '63%'
'''