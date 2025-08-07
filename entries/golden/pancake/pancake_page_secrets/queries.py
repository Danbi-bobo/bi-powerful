query = '''
    SELECT
        page_id
        , department
    FROM
        alomix_skyward_data.pancake_pages
    WHERE
        is_activated = 1
        AND role_in_page = 'ADMINISTER'
'''