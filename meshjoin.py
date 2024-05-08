from threading import Thread
from queue import Queue

import mysql.connector

host = "127.0.0.1"
db_name = "db"
db_username = "root"
db_password = "1234"

dw_name = "dw"
dw_username = "root"
dw_password = "1234"

stream_buffer = Queue()
md_buffer_cus = Queue()
md_buffer_pdt = Queue()
dict = {}

class Datasource():
            
    def __init__(self, transaction_id, product_id, customer_id, store_id, store_name, time_id, t_date, quantity):
        self.transaction_id = transaction_id
        self.product_id = product_id
        self.customer_id = customer_id
        self.store_id = store_id
        self.store_name =store_name
        self.time_id = time_id
        self.t_date = t_date
        self.quantity = quantity

class Product():

    def __init__(self, product_id, product_name, supplier_id, supplier_name, price):
        self.product_id = product_id
        self.product_name = product_name
        self.supplier_id = supplier_id
        self.supplier_name = supplier_name
        self.price = price

class Customers():

    def __init__(self, customer_id, customer_name):
        self.customer_id = customer_id
        self.customer_name = customer_name

class Transdata:

    def __init__(self, ds_obj, product_name, supplier_id, supplier_name, price, customer_name):
        self.transaction_id = ds_obj.transaction_id
        self.product_id = ds_obj.product_id
        self.customer_id = ds_obj.customer_id
        self.time_id = ds_obj.time_id
        self.store_id = ds_obj.store_id
        self.store_name = ds_obj.store_name
        self.t_date = ds_obj.t_date
        self.quantity = ds_obj.quantity
        self.product_name = product_name
        self.supplier_id = supplier_id
        self.supplier_name = supplier_name
        self.price = price
        self.sale = self.quantity * self.price
        self.customer_name = customer_name


class Stream(Thread): 

    def run(self):
        try:
            conn = mysql.connector.connect(
                host=host,
                user=db_username,
                password=db_password,
                database=db_name
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM db.transactions")
            result = cursor.fetchall()
            
            for row in result:
                ds = Datasource(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])

                stream_buffer.put(ds)

            cursor.close()
        except mysql.connector.Error as e:
            print("An error occurred:", e)
        finally:
            if "conn" in locals() and conn.is_connected():
                conn.close()

class MasterData(Thread):
    
    def run(self):
        try:
            conn = mysql.connector.connect(
                host=host,
                user=db_username,
                password=db_password,
                database=db_name
            )

            cursor = conn.cursor()
            cursor.execute("SELECT * FROM db.customers")
            result = cursor.fetchall()

            for row in result:
                ds = Customers(row[0], row[1])
                md_buffer_cus.put(ds)

            cursor.execute("SELECT * FROM db.products")
            result = cursor.fetchall()
            
            for row in result:
                ds = Product(row[0], row[1], row[2], row[3], row[4])
                md_buffer_pdt.put(ds)

            cursor.close()
        except mysql.connector.errors as e:
            print("An error occurred:", e)
        finally:
            if "conn" in locals() and conn.is_connected():
                conn.close()

class MeshJoin ():
    
    def readFromMD(self, ds, tsd):
        foundcus = 0
        foundprd = 0

        while (foundcus != 1):
            rmhead = md_buffer_cus.get()

            if (str.lower(rmhead.customer_id) == str.lower(ds.customer_id)):
                tsd.customer_name = rmhead.customer_name
                foundcus = 1
            
            md_buffer_cus.put(rmhead)

        while (foundprd != 1):
            rmhead = md_buffer_pdt.get()

            if (str.lower(rmhead.product_id) == str.lower(ds.product_id)):
                tsd.product_name = rmhead.product_name
                tsd.supplier_id = rmhead.supplier_id
                tsd.supplier_name = rmhead.supplier_name
                tsd.price = rmhead.price
                tsd.sale = tsd.quantity * tsd.price
                foundprd = 1   

            md_buffer_pdt.put(rmhead)

md = MasterData()
md.run()

s = Stream()
s.run()

mj = MeshJoin()

while (not stream_buffer.empty()):
    ext_ds = stream_buffer.get()
    tmd = Transdata(ext_ds, "", "", "", 0.0, "")

    mj.readFromMD(ext_ds, tmd)
    dict[tmd.transaction_id] = tmd

conn = None

try:
    conn = mysql.connector.connect(
        host=host,
        username=dw_username,
        password=dw_password,
        database=dw_name
    )
except mysql.connector.errors as e:
    print("An error occurred:", e)
    
for i in dict.keys():
    cursor = conn.cursor()
    
    #Product Table
    try:
        product_id = "'" + dict.get(i).product_id + "'"
        product_name = "'" + dict.get(i).product_name + "'"
        price = dict.get(i).price

        query = "insert into product values ("+product_id+","+product_name+","+str(price)+")"
        cursor.execute(query)
        result = cursor.fetchall()

    except mysql.connector.IntegrityError:
        pass

    #Supplier Table
    try:
        supplier_id = "'" + dict.get(i).supplier_id + "'"
        supplier_name = "'" + dict.get(i).supplier_name + "'"

        query = "insert into supplier values ("+supplier_id+","+supplier_name+")"
        cursor.execute(query)

    except mysql.connector.IntegrityError:
        pass

    #Customer Table
    try:
        customer_id = "'" + dict.get(i).customer_id + "'"
        customer_name = "'" + dict.get(i).customer_name + "'"

        query = "insert into customer values ("+customer_id+","+customer_name+")"
        cursor.execute(query)

    except mysql.connector.IntegrityError:
        pass

    #Store Table
    try:
        store_id = "'" + dict.get(i).store_id + "'"
        store_name = "'" + dict.get(i).store_name + "'"

        query = "insert into store values ("+store_id+","+store_name+")"
        cursor.execute(query)

    except mysql.connector.IntegrityError:
        pass

    #Date Table
    try:
        time_id = "'" + dict.get(i).time_id + "'"
        t_date = "'" + str(dict.get(i).t_date) + "'"
        if (dict.get(i).t_date.day == 0 or dict.get(i).t_date.day == 6):
            weekend = "1"
        else:
            weekend = "0"

        if (dict.get(i).t_date.month >= 1 and dict.get(i).t_date.month <= 6):
            half_of_year = "'First'"
        if (dict.get(i).t_date.month >= 7 and dict.get(i).t_date.month <= 12):
            half_of_year = "'Second'"

        month = "'" + str(dict.get(i).t_date.month) + "'"

        if (dict.get(i).t_date.month >= 1 and dict.get(i).t_date.month <= 3):
            quarter = "'Q1'"
        if (dict.get(i).t_date.month >= 4 and dict.get(i).t_date.month <= 6):
            quarter = "'Q2'"
        if (dict.get(i).t_date.month >= 7 and dict.get(i).t_date.month <= 9):
            quarter = "'Q3'"
        if (dict.get(i).t_date.month >= 10 and dict.get(i).t_date.month <= 12):
            quarter = "'Q4'"

        year = "'" + str(dict.get(i).t_date.year) + "'"

        query = "insert into date values ("+time_id+","+t_date+","+weekend+","+half_of_year+","+month+","+quarter+","+year+")"
        cursor.execute(query)

    except mysql.connector.IntegrityError:
        pass

    #Sales Table
    try:
        transaction_id = str(i)
        product_id = "'" + dict.get(i).product_id + "'"
        customer_id = "'" + dict.get(i).customer_id + "'"
        time_id = "'" + dict.get(i).time_id + "'"
        store_id = "'" + dict.get(i).store_id + "'"
        supplier_id = "'" + dict.get(i).supplier_id + "'"
        quantity = "'" + str(dict.get(i).quantity) + "'"
        sale = "'" + str(dict.get(i).sale) + "'"

        query = "insert into sales values ("+transaction_id+","+product_id+","+customer_id+","+time_id+","+store_id+","+supplier_id+","+quantity+","+sale+")"
        cursor.execute(query)

    except mysql.connector.IntegrityError:
        pass

conn.commit()
    