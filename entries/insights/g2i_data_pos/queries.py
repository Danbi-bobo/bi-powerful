resell = '''      
WITH temp AS (
	SELECT
		order_id
		, shop_id
		, CASE
			WHEN products_name LIKE '%,%' THEN 'Nhiều sản phẩm'
			ELSE products_name
		END AS clean_product
		, CASE
			WHEN bill_phone_number LIKE '+84%' THEN REPLACE(bill_phone_number, '+84', '0')
		    WHEN bill_phone_number LIKE '84%' THEN CONCAT('0', SUBSTRING(bill_phone_number, 3))
		    WHEN bill_phone_number NOT LIKE '0%' THEN CONCAT('0', bill_phone_number)
		    ELSE bill_phone_number
		END AS phone
	FROM
		alomix_skyward_data.pos_orders
)
SELECT
	DATE_ADD(a.inserted_at, INTERVAL 7 HOUR) AS "Thời điểm tạo đơn"
	, YEAR(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) AS "Năm"
	, products_name AS "Sản phẩm"
	, bill_full_name AS "Tên Khách hàng"
	, b.phone AS "Số điện thoại"
	, shipping_address AS "Địa chỉ"
	, CASE
		WHEN status = 3 THEN 'Đã nhận'
		WHEN status = 15 THEN 'Hoàn 1 phần'
		WHEN status = 16 THEN 'Đã thu tiền'
	END AS "Trạng thái"
	, CASE
		WHEN lower(products_name) LIKE '%nam%' THEN 'Nam'
		WHEN lower(products_name) LIKE '%nữ%' THEN 'Nữ'
		WHEN b.clean_product = 'Nhiều sản phẩm' THEN 'Không xác định'
		WHEN lower(products_name) LIKE '%hồ ly%' THEN 'Nữ'
		WHEN lower(products_name) LIKE '%di lặc%' THEN 'Nam'
		WHEN lower(products_name) LIKE '%pháp bảo%' THEN 'Nam'
		WHEN lower(products_name) LIKE '%ngân hà%' THEN 'Nữ'
		WHEN lower(products_name) LIKE '%18k%' THEN 'Nữ'
		ELSE 'Không xác định'
	END AS "Giới tính"
	, CASE 
		WHEN b.clean_product = 'Nhiều sản phẩm' THEN 'Không xác định'
		WHEN lower(products_name) LIKE '%trắng%' THEN 'Kim/Thuỷ'
		WHEN lower(products_name) LIKE '%vàng%' THEN 'Kim/Thổ'
		WHEN lower(products_name) LIKE '%xanh%' THEN 'Mộc/Thuỷ/Hoả'
		WHEN lower(products_name) LIKE '%đỏ%' THEN 'Hoả/Thổ'
	END AS "Mệnh"
	, count(*) OVER(PARTITION BY phone) AS "Số đơn hàng đã mua"
	, CASE
		WHEN YEAR(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) = 2025 THEN 'CV'
		WHEN lower(products_name) LIKE '%nam%' THEN '921026536'
		WHEN lower(products_name) LIKE '%nữ%' THEN '1535025306'
		WHEN b.clean_product = 'Nhiều sản phẩm' THEN 'Không xác định'
		WHEN lower(products_name) LIKE '%hồ ly%' THEN '1535025306'
		WHEN lower(products_name) LIKE '%di lặc%' THEN '921026536'
		WHEN lower(products_name) LIKE '%pháp bảo%' THEN '921026536'
		WHEN lower(products_name) LIKE '%ngân hà%' THEN '1535025306'
		WHEN lower(products_name) LIKE '%18k%' THEN '1535025306'
		ELSE 'Không xác định'
	END AS "Nguồn đơn"
FROM
	alomix_skyward_data.pos_orders a
	LEFT JOIN temp b ON a.order_id = b.order_id AND a.shop_id = b.shop_id
WHERE
	status IN (3, 15, 16)
	AND YEAR(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) = 2024
'''

cv = '''
WITH temp AS (
	SELECT
		order_id
		, shop_id
		, CASE
			WHEN products_name LIKE '%,%' THEN 'Nhiều sản phẩm'
			ELSE products_name
		END AS clean_product
		, CASE
			WHEN bill_phone_number LIKE '+84%' THEN REPLACE(bill_phone_number, '+84', '0')
		    WHEN bill_phone_number LIKE '84%' THEN CONCAT('0', SUBSTRING(bill_phone_number, 3))
		    WHEN bill_phone_number NOT LIKE '0%' THEN CONCAT('0', bill_phone_number)
		    ELSE bill_phone_number
		END AS phone
	FROM
		alomix_skyward_data.pos_orders
)
SELECT
	DATE_ADD(a.inserted_at, INTERVAL 7 HOUR) AS "Thời điểm tạo đơn"
	, YEAR(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) AS "Năm"
	, products_name AS "Sản phẩm"
	, bill_full_name AS "Tên Khách hàng"
	, b.phone AS "Số điện thoại"
	, shipping_address AS "Địa chỉ"
	, CASE
		WHEN status = 0 THEN 'Mới'
		WHEN status = 6 THEN 'Đã huỷ'
	END AS "Trạng thái"
	, CASE
		WHEN lower(products_name) LIKE '%nam%' THEN 'Nam'
		WHEN lower(products_name) LIKE '%nữ%' THEN 'Nữ'
		WHEN b.clean_product = 'Nhiều sản phẩm' THEN 'Không xác định'
		WHEN lower(products_name) LIKE '%hồ ly%' THEN 'Nữ'
		WHEN lower(products_name) LIKE '%di lặc%' THEN 'Nam'
		WHEN lower(products_name) LIKE '%pháp bảo%' THEN 'Nam'
		WHEN lower(products_name) LIKE '%ngân hà%' THEN 'Nữ'
		WHEN lower(products_name) LIKE '%18k%' THEN 'Nữ'
		ELSE 'Không xác định'
	END AS "Giới tính"
	, CASE 
		WHEN b.clean_product = 'Nhiều sản phẩm' THEN 'Không xác định'
		WHEN lower(products_name) LIKE '%trắng%' THEN 'Kim/Thuỷ'
		WHEN lower(products_name) LIKE '%vàng%' THEN 'Kim/Thổ'
		WHEN lower(products_name) LIKE '%xanh%' THEN 'Mộc/Thuỷ/Hoả'
		WHEN lower(products_name) LIKE '%đỏ%' THEN 'Hoả/Thổ'
	END AS "Mệnh"
	, count(*) OVER(PARTITION BY phone) AS "Số đơn hàng đã mua"
	, '921026568' AS "Nguồn đơn"
	, note
FROM
	alomix_skyward_data.pos_orders a
	LEFT JOIN temp b ON a.order_id = b.order_id AND a.shop_id = b.shop_id
WHERE
	status IN (0, 6)
	AND YEAR(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) = 2025
	AND (
		CONCAT(', ', tags_id, ',') LIKE '%, 218,%'
		OR CONCAT(', ', tags_id, ',') LIKE '%, 351,%'
		OR CONCAT(', ', tags_id, ',') LIKE '%, 352,%'
		OR CONCAT(', ', tags_id, ',') LIKE '%, 355,%'
		OR CONCAT(', ', tags_id, ',') LIKE '%, 252,%'
	)
	AND CONCAT(', ', tags_id, ',') NOT LIKE '%, 129,%'
'''