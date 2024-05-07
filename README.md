-- Thuật toán
+ Mesh Join

-- Start Schema
+ Dimension:
	+ Time gồm year, month, day, hour, minute, second cho mỗi giao dịch
	+ Product gồm product Id, name, supplier ID, supplier name
	+ Customer gồm customer ID and name
	+ Store goomf store ID, name
+ Fact Table:
	+ Quantity: số lượng sản phẩm được mua
	+ Price: giá mỗi sản phẩm
	+ Sale: tổng tiền bán (quantity x price)

-- Enrichment
+ Lập hồ sở dữ liệu để xác định các vấn đề về chất lượng dữ liệu và sự không nhất quán
+ Làm sạch dữ liệu để loại bỏ các giá trị trùng lặp, thiếu giá trị và không nhất quán
+ Chuyển đổi dữ liệu để tạo các tính năng mới hoặc sửa dổi các tính năng hiện có
+ Tăng cường dữ liệu để thêm dữ liệu mới từ các nguồn bên ngoài
+ Tích hợp dữ liệu để kết hợp dữ liệu từ nhiều nguồnM
	
				
	