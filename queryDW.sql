USE dw;

-- 1. Top 3 tên cửa hàng có doanh thu cao nhất tháng 9/2017
SELECT store.store_name, round(SUM(total_sale)) as revenue
FROM sales JOIN date ON sales.time_id = date.time_id
        JOIN store ON sales.store_id = store.store_id
WHERE date.month = 9 and date.year = 2017
GROUP BY sales.store_id
ORDER BY revenue DESC
LIMIT 3;

-- 2. 10 nhà cung cấp hàng đầu tạo ra doanh thu cao nhất vào cuối tuần
SELECT supplier.supplier_name, round(SUM(total_sale)) as revenue
FROM sales JOIN date ON sales.time_id = date.time_id
        JOIN supplier ON sales.supplier_id = supplier.supplier_id
WHERE date.weekend = 1
GROUP BY sales.supplier_id
ORDER BY revenue DESC
LIMIT 10;

-- 3. Tổng doanh số của tất cả các sản phẩm được cung cấp bởi mỗi nhà cung cấp theo quý và tháng
SELECT product.product_name, supplier.supplier_name, date.quarter, date.month, round(sum(sales.total_sale)) as revenue
FROM sales JOIN date ON sales.time_id = date.time_id
        JOIN supplier ON sales.supplier_id = supplier.supplier_id
        JOIN product ON sales.product_id = product.product_id
GROUP BY sales.supplier_id, date.quarter, date.month, sales.product_id;

-- 4. Tổng doanh số của từng sản phẩm được bán bởi mỗi cửa hàng
SELECT store.store_name, product.product_name, round(sum(sales.total_sale)) as revenue
FROM sales JOIN store ON sales.store_id = store.store_id
        JOIN product ON sales.product_id = product.product_id
GROUP BY sales.product_id, sales.store_id;

-- 5. Phân tích doanh số hàng quý cho tất cả các cửa hàng bằng cách sử dụng khái niệm truy vấn chi tiết
SELECT store.store_name, date.quarter, round(sum(sales.total_sale)) as revenue
FROM sales JOIN store ON sales.store_id = store.store_id
        JOIN date ON sales.time_id = date.time_id
GROUP BY sales.store_id, date.quarter;

-- 6. Top 5 sản phẩm phổ biến nhất được bán trong những ngày cuối tuần
SELECT product.product_name, round(SUM(quantity)) as revenue
FROM sales JOIN date ON sales.time_id = date.time_id
        JOIN product ON sales.product_id = product.product_id
WHERE date.weekend = 0
GROUP BY sales.product_id
ORDER BY revenue DESC
LIMIT 5;

-- 7. Thao tác Rollup store, supplier và product
SELECT store_id, supplier_id, product_id from sales
group by store_id, supplier_id, product_id with rollup;

-- 8. Tổng doanh số của từng sản phẩm trong nửa đầu và nửa cuối năm 2017 cùng với tổng doanh số hàng năm
SELECT product.product_name,date.half_of_year,round(SUM(total_sale)) as Revenue FROM sales 
JOIN product on product.product_id = sales.product_id 
JOIN date on date.time_id = sales.time_id where date.year = 2017 group by sales.product_id, half_of_year;

-- 9. Tìm các điểm bất thường trong bộ dữ liệu DW

-- 10. Tạo chế độ xem củ thế hóa với tên "STORE_product_ANALYSIS"
CREATE TABLE `STORE_PRODUCT_ANALYSIS` AS 
SELECT store_name,product_name,total_sale FROM sales JOIN store on sales.store_id = store.store_id 
JOIN product on sales.product_id = product.product_id order by store_name,product_name;

SELECT * from STORE_PRODUCT_ANALYSIS;