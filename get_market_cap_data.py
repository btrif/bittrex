#  Created by Bogdan Trif on 10-01-2018 , 12:45 PM.

import time
import operator
import pymysql
import requests  # to make GET request
from conf.db_conn import *
from includes.DB_functions import *
from includes.app_functions import *


t1  = time.time()

debug_level = 0

connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB2 )
cur = connection.cursor()

date = time.strftime("%Y-%m-%d %H:%M:%S",   time.localtime() )
# print( 'Current Date   --->   ', date ,'\n'  )

URL = 'https://api.coinmarketcap.com/v1/ticker/?convert=EUR&limit=200'
# filename = 'tmp/market_data.html'
# get_API_bittrex_data_log =  'tmp/get_API_bittrex_data.log'

req  = requests.get(URL )
MARKETS = req.json()

# print(MARKETS)

MONITORING = ['BTC', 'ETH', 'XRP', 'ADA', 'XLM', 'NEO', 'XEM', 'XMR', 'LSK', 'QTUM', 'OMG', 'STRAT', 'SC', 'XVG', 'RDD', 'PART', 'LTC', 'ETC', 'ZEC']


# t2  = time.time()
# file_append_with_text(get_API_bittrex_data_log,  str(date)+  '       getExternalContent( URL  ) took    ' + str(round((t2-t1)*1000,2)) + ' ms' )




# t3  = time.time()
# file_append_with_text(get_API_bittrex_data_log, str(date)+  '       valid_MARKETS took    ' + str(round((t3-t2)*1000,2)) + ' ms' )

if debug_level >= 1 :     print(len( MARKETS) ,MARKETS,'\n')


for cnt, ITEM in enumerate(MARKETS) :
    market = ITEM["symbol"]
    if market in MONITORING :
        if debug_level >=1 :
            print(ITEM)

        date = time.strftime(date_pattern, time.localtime( int(ITEM["last_updated"])))

        price_USD = format( float(ITEM["price_usd"]), '.4f')
        price_EUR = format( float(ITEM["price_eur"]), '.4f')
        price_BTC = format(float(ITEM["price_btc"]), '.8f')
        vol_24h_EUR = int(float(ITEM["24h_volume_eur"]))
        market_cap_EUR = int(float(ITEM["market_cap_eur"]))

        price_ch_1h = format(float(ITEM["percent_change_1h"]), '.2f')
        price_ch_24h = format(float(ITEM["percent_change_24h"]), '.2f')
        price_ch_7d = format(float(ITEM["percent_change_7d"]), '.2f')

        available_supply = int(float(ITEM["available_supply"]))
        rank = int(ITEM["rank"])

        if debug_level >=1 :
            print(str(cnt+1) +'.     ',market, '    date =' ,date, '    price_EUR = ' ,price_EUR,'      price_BTC =' , price_BTC,'       vol_24h_EUR =  ' , vol_24h_EUR ,'     market_cap_EUR = ' , market_cap_EUR,'     price_ch_1h =',price_ch_1h,'    available_supply = ', available_supply,'    rank = ', rank )


        ###### Select data from table using SQL query.      ########

        prepQuery  = 'SELECT price_EUR, price_BTC, vol_24h_EUR, market_cap_EUR, date, id FROM `market_name` ORDER BY ID DESC LIMIT 1;'

        try :
            if debug_level >=2 :
                print('prepQuery : \t', prepQuery)
            prepResult = cur.execute(prepQuery.replace('market_name' , market))
            if debug_level >=2 :
                print('prepResult : \t', prepResult)

            row = cur.fetchone()

            if (prepResult > 0) :
                prev_price_EUR =  float(row[0])
                prev_price_BTC = float(row[1])
                prev_vol24h =  int( row[2] )
                prev_market_cap_EUR =  int( row[3] )

                if debug_level >=2 :  print(' row values : \t', row )

                epoch = int(time.mktime(time.strptime(str(row[4]), date_pattern)))

                if debug_level >= 2 : print('epoch : ', epoch)
                if debug_level >= 1 : print( '  epoch != DATA[last_updated]    ' , epoch != int(ITEM['last_updated']) ,'        epoch =   ' ,epoch , '    DATA[] =' , ITEM['last_updated']  )


                EUR_ch = round( (( float(price_EUR) - prev_price_EUR ) / prev_price_EUR) * 100 , 2)
                BTC_ch = round( (( float(price_BTC) - prev_price_BTC ) / prev_price_BTC) * 100 , 2)
                vol_24h_ch = round( (( float(vol_24h_EUR) - prev_vol24h ) / prev_vol24h) * 100 , 2)
                market_cap_ch = round( (( float(market_cap_EUR) - prev_market_cap_EUR ) / prev_market_cap_EUR) * 100 , 2)

                if epoch != int(ITEM['last_updated']) :


                    second_insert_Result = cur.execute( QUERIES['second_insert_market_cap'].replace('table_name', market),
                        (date, price_USD, price_EUR, EUR_ch, price_BTC, BTC_ch, vol_24h_EUR, vol_24h_ch, market_cap_EUR, market_cap_ch, price_ch_1h, price_ch_24h, price_ch_7d, available_supply, rank) )



        except:
            if debug_level >= 1 :               print('No such table ! We will create the table')
            #### ====== CREATE IF TABLE DOES NOT EXIST !!!!
            if debug_level >= 1 :                 print('market ', market, ' was not found in TABLES_SET !!!!')
            createTableQuery = TABLES['create_coin_market_cap_table']
            if debug_level >= 2 :            print('createTableQuery : \t', createTableQuery )
            createTableResult = cur.execute( TABLES['create_coin_market_cap_table'].replace('table_name', market ) )
            if debug_level >= 2 : print('createTableResult : \t', createTableResult )

            if debug_level >= 3 : print(QUERIES['initial_insert_market_cap'].replace('table_name', market))
            initial_insert_Result = cur.execute( QUERIES['initial_insert_market_cap'].replace('table_name', market),
                                                 (date, price_USD, price_EUR, price_BTC, vol_24h_EUR, market_cap_EUR, price_ch_1h, price_ch_24h, price_ch_7d, available_supply, rank) )

            if debug_level >= 2 :               print('initial_insert_Result : ', initial_insert_Result)


connection.commit()
connection.close()




# t4  = time.time()
# file_append_with_text(get_API_bittrex_data_log,  str(date)+  '       market INSERT in tables took    ' + str(round((t4-t3)*1000,2)) + ' ms' )
