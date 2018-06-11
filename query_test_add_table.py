#  Created by Bogdan Trif on 13-01-2018 , 10:24 PM.

from time import localtime, strftime

import pymysql

from conf.db_conn import *
from includes.DB_functions import *

cur_date = strftime("%Y-%m-%d %H:%M:%S", localtime())
print( 'Current Date  :  ', cur_date ,'\n'  )


conn_to_DB = pymysql.connect(host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB1)
cur = conn_to_DB.cursor()


# cur.execute( add_table, (  cur_date ) )
# cur.execute( TABLES['employees']  )
market = 'BTC-AAA'
cur.execute( TABLES['create_market_name'].replace('table_name', market) )

print( type(TABLES['create_market_name']), TABLES['create_market_name'].replace('table_name', market)  )


result1 = cur.execute( "SELECT * FROM information_schema.tables WHERE table_schema = 'BTC-XZC' LIMIT 1;"   )
result2 = cur.execute( "SHOW TABLES LIKE 'BTC-XRP';"  )               #   !!!!!!!!!!!!!!!!!!! THE MOST EFFICIENT  !!!!!!!!!!!!!!!!!
result3 = cur.execute( "SHOW TABLES LIKE 'BTC-XRP'; "  )


result5 = cur.execute(  QUERIES['check_if_table_exist'].replace('table_name',  'BTC-XRP' ) )      #      !!! Success finally
result6 = cur.execute("SHOW TABLES ;")
result61 = cur.fetchall()


print('result1 : ', result1)
print('result2 : ', result2)                #   !!!!!!!!!!!!!!!!!!! THE MOST EFFICIENT  !!!!!!!!!!!!!!!!!
print('result3 : ', result3)

print('result5 : ', result5)
print('result6 : ', result6  )
print('result61 : ', type(result61), result61   )
print('check if table is in the list : ',  ('BTC-ENG',) in result61 )

for i in result61 : print(i)




def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False



# Close the query and the the connection to the DB
cur.close()
conn_to_DB.close()









