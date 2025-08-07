list_page = '''
    SELECT
        page_id
        , page_access_token
    FROM
        alomix_skyward_data.pancake_pages
    WHERE
        page_access_token IS NOT NULL
'''