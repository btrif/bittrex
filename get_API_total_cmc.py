#  Created by Bogdan Trif on 30-01-2018 , 11:28 PM.



import time
import operator
import pymysql
import requests  # to make GET request
from conf.sensitive import *
from includes.DB_functions import *
from includes.app_functions import *


time.sleep(3)

debug_level = 0

connection = pymysql.connect( host= localhost, port=3306, user=USER, passwd=PASSWD, db=cmc )
cur = connection.cursor()


URL_global = 'https://api.coinmarketcap.com/v1/global/?convert=EUR'

req  = requests.get(URL_global )
DATA = req.json()

if debug_level >= 1 : print(DATA)


date_pattern = '%Y-%m-%d %H:%M:%S'
date = time.strftime(date_pattern, time.localtime(DATA['last_updated']))

total_market_cap_eur = int(DATA['total_market_cap_eur'])
total_market_cap_usd = int(DATA['total_market_cap_usd'])
total_24h_volume_eur = int(DATA['total_24h_volume_eur'])
BTC_dominance = format( DATA["bitcoin_percentage_of_market_cap"], '.2f' )

if debug_level >= 1 : print('date:', date, '   total_market_cap_eur=', total_market_cap_eur, '    total_24h_volume_eur=', total_24h_volume_eur, '   BTC_dominance=', BTC_dominance )


prepQuery  = 'SELECT market_cap_EUR, volume_24h, date, id FROM `_market_cap` ORDER BY ID DESC LIMIT 1;'

try :
    if debug_level >= 2 : print('\nprepQuery : ', prepQuery)
    prepResult = cur.execute(prepQuery)
    if debug_level >= 2 : print('prepResult : ', prepResult)
    row = cur.fetchone()



    if (prepResult > 0) :

        prev_total_market_cap =  int(row[0])
        prev_total_24h_volume = int(row[1])

        if debug_level >= 2 : print('  row = ',row, '    ',  prev_total_market_cap , prev_total_24h_volume    )

        total_market_cap_Ch = round( (( total_market_cap_eur - prev_total_market_cap ) / prev_total_market_cap) * 100 , 2)
        total_24h_volume_Ch = round( (( total_24h_volume_eur - prev_total_24h_volume) / prev_total_24h_volume) * 100 , 2)

        if debug_level >= 2 : print('  total_market_cap , prev_total_24h_volume : \t', total_24h_volume_eur ,'       ' ,prev_total_24h_volume )

        epoch = int(time.mktime(time.strptime(str(row[2]), date_pattern)))
        if debug_level >= 2 : print('epoch : ', epoch)
        if debug_level >= 1 : print( '  epoch != DATA[last_updated]    ' , epoch != DATA['last_updated'] ,'        epoch =   ' ,epoch , '    DATA[] =' , DATA['last_updated']  )

        ##### We insert the data only if it is new ! We prevent to accumulate DOUBLE DATA !!!!
        if epoch != int(DATA['last_updated']) :

            insert_query  = 'INSERT INTO `_market_cap` (date, market_cap_EUR, market_cap_USD, market_cap_ch, volume_24h, volume_24h_ch, BTC_dominance) ' \
                            'VALUES (%s, %s, %s, %s, %s, %s, %s);'
            if debug_level >= 2 : print('insert_query : \t', insert_query)
            insert_result = cur.execute( insert_query, (date, total_market_cap_eur, total_market_cap_usd, total_market_cap_Ch, total_24h_volume_eur, total_24h_volume_Ch, BTC_dominance) )



    else :
        initial_insert_query =  'INSERT INTO `_market_cap` (date, total_market_cap, total_24h_volume, BTC_dominance) VALUES (%s, %s, %s, %s);'
        if debug_level >= 2 : print('initial_insert_query : ', initial_insert_query )
        initial_insert_result = cur.execute(initial_insert_query, ( date, total_market_cap_eur, total_market_cap_usd, total_24h_volume_eur, BTC_dominance ))
        if debug_level >= 2 : print('initial_insert_result : \t', initial_insert_result)


except:
    if debug_level >= 1 : print('No such row ! We will create the FIRST row')


connection.commit()
connection.close()



#
#
# t4  = time.time()
# file_append_with_text(get_API_bittrex_data_log,  str(date)+  '       market INSERT in tables took    ' + str(round((t4-t3)*1000,2)) + ' ms' )


