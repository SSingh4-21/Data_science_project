# This Function will Fetch data from the calling program on the basis of the SQL query
import sqlite3
def fn_get_DB_data(sqlquery):
   #path = input("Enter the Local path of the DB file:") + "\lboro_lab.db"## Kindly put the correct path of the Copied DB file   
   try:
      path = r"lboro_lab.db"## Kindly put the correct path of the Copied DB file
      db = sqlite3.connect(path)
      c = db.cursor()
      c.execute(sqlquery)
      data = c.fetchall()
      #print(data)
      c.close()
      return data
   except Exception as e:
      print(e)    