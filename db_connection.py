import mysql.connector


# connect to MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="oracle",
    database="qc_schema"
)