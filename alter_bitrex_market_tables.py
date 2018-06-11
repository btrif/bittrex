#  Created by Bogdan Trif on 01-02-2018 , 11:30 AM.

import time
import operator
import pymysql
import requests  # to make GET request
from conf.db_conn import *
from includes.DB_functions import *
from includes.app_functions import *


connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB1 )
cur = connection.cursor()


valid_MARKETS = sorted(read_file_line_by_line('tmp/valid_Markets.txt'))

print(valid_MARKETS)


CNT = 1
for cnt, market in enumerate(valid_MARKETS) :

    # print(str(cnt+1)+'.      ', market)
    # query1  = 'ALTER TABLE `market_name` MODIFY last_price_ch DECIMAL(7,2) AFTER last_price ;'
    # result1 = cur.execute( query1.replace('market_name' , market) )
    #
    # query2 = 'ALTER TABLE `market_name` MODIFY buy_vs_sell_ch DECIMAL(7,2) AFTER buy_vs_sell ;'
    # result2 = cur.execute( query2.replace('market_name' , market) )
    #
    # query3 = 'ALTER TABLE `market_name` MODIFY vol_ch DECIMAL(7,2) AFTER volume ;'
    # result3 = cur.execute( query3.replace('market_name' , market) )
    #
    # query4 = 'ALTER TABLE `market_name` MODIFY buy_orders INT(10) AFTER ask ;'
    # result4 = cur.execute( query4.replace('market_name' , market) )
    #
    # query5 = 'ALTER TABLE `market_name` MODIFY sell_orders INT(10) AFTER buy_orders ;'
    # result5 = cur.execute( query5.replace('market_name' , market) )
    # # print(query5)
    # # print(result5)

    # print(str(cnt+1)+'.      ', market)



    check_query = 'SELECT vol_buy from `market_name` ORDER BY id DESC LIMIT 1 ;'

    try :
        check_res = cur.execute(check_query.replace('market_name' , market  ) )
        # print('check_res : ', check_res)
    except pymysql.err.InternalError :
        print(str(cnt+1)+'.      ', market ,'      ', CNT)
        CNT += 1
        print('no such field, we will make the required operations :')

        query1  = 'ALTER TABLE `market_name` ADD vol_buy decimal(11,4) AFTER sell_orders;'
        result1 = cur.execute( query1.replace('market_name' , market) )

        query2 = 'ALTER TABLE `market_name` ADD vol_sell decimal(11,4) AFTER vol_buy;'
        result2 = cur.execute( query2.replace('market_name' , market) )
        print('result1 : ', result1, '     result2 : ', result2 )


connection.commit()
connection.close()

