#  Created by Bogdan Trif on 10-01-2018 , 12:45 PM.

import time, os
from platform import system

import pymysql
import requests  # to make GET request
from conf.sensitive import *
from includes.DB_functions import *
from includes.app_functions import *
import logging.config

t1  = time.time()

###     URL of the API
URL_cmc1 = 'https://api.coinmarketcap.com/v1/ticker/?convert=EUR&limit=400'

###     Markets to MONITOR      ###
cmc_markets = ['BTC', 'ETH', 'XRP', 'ADA', 'XLM', 'NEO', 'XEM', 'XMR', 'LSK', 'QTUM',
                        'OMG', 'STRAT', 'SC', 'XVG', 'RDD', 'PART', 'LTC', 'ETC', 'ZEC', 'WAVES' ]


###     LOGGING     ###
config = init_logging( config_file= 'conf/logging.json' , log_type= 'json' )
logging.config.dictConfig(config['logging'])
logger = logging.getLogger('get_API_coinmarketcap_markets_data')


###### Select data from table using SQL query.      ########
# prepQuery  = 'SELECT price_EUR, price_BTC, vol_24h_EUR, market_cap_EUR, date, id FROM `market_name` ORDER BY ID DESC LIMIT 1;'

#####        Connection  to coinmarketcap DB        #####
connection = pymysql.connect( host= localhost, port=PORT_NR, user=USER, passwd=PASSWD, db=cmc )
cur = connection.cursor()

logger.debug('connection :' +str(connection) +'    cursor :' +str( cur) )

date = time.strftime("%Y-%m-%d %H:%M:%S",   time.localtime() )



def get_API_coin_market_cap_markets(URL) :
    req  = requests.get(URL )
    MARKETS = req.json()
    return MARKETS


def select_markets_metrics_for_DB(ITEM):
    Items = {}
    Items['market']    = ITEM["symbol"]
    Items['date'] = time.strftime(date_pattern, time.localtime( int(ITEM["last_updated"])))

    Items['price_USD'] = format( float(ITEM["price_usd"]), '.4f')
    Items['price_EUR'] = format( float(ITEM["price_eur"]), '.4f')
    Items['price_BTC'] = format(float(ITEM["price_btc"]), '.8f')
    Items['vol_24h_EUR'] = int(float(ITEM["24h_volume_eur"]))
    Items['market_cap_EUR'] = int(float(ITEM["market_cap_eur"]))

    Items['price_ch_1h'] = format(float(ITEM["percent_change_1h"]), '.2f')
    Items['price_ch_24h'] = format(float(ITEM["percent_change_24h"]), '.2f')
    Items['price_ch_7d'] = format(float(ITEM["percent_change_7d"]), '.2f')

    Items['available_supply'] = int(float(ITEM["available_supply"]))
    Items['rank'] = int(ITEM["rank"])

    return Items

################        END DEF         ################

MARKETS = get_API_coin_market_cap_markets(URL_cmc1)

t2  = time.time()
logger.info('getExternalContent( URL  ) took ' + str(round((t2-t1)*1000,2)) + ' ms' )

logger.debug('MARKETS : ' + str(len(MARKETS)) +'     ' + str(MARKETS) )



if __name__ == '__main__':
    QB = QueryBuilder()

    for cnt, item in enumerate(MARKETS) :
        ITEM = select_markets_metrics_for_DB(item)
        market = ITEM["market"]
        if market in cmc_markets :
            ITEM.pop('market')
            logger.info('ITEM : ' + str(ITEM) )


            try :
                prepQuery = QB.select_query( market, 'ORDER BY ID DESC LIMIT 1', *['price_EUR', 'price_BTC', 'vol_24h_EUR', 'market_cap_EUR', 'date', 'id']  )
                logger.debug('prepQuery : \t'+ str(prepQuery))
                prepResult = cur.execute(prepQuery.replace('market_name' , market))
                logger.debug('prepResult : \t'+ str(prepResult))

                row = cur.fetchone()

                if (prepResult > 0) :
                    prev_price_EUR =  float(row[0])
                    prev_price_BTC = float(row[1])
                    prev_vol24h =  int( row[2] )
                    prev_market_cap_EUR =  int( row[3] )

                    logger.debug('row values : \t' + str(row) )

                    epoch = int(time.mktime(time.strptime(str(row[4]), date_pattern)))
                    epoch_last_updated = int(convert_time_standard_format_to_epoch(ITEM['date']))

                    logger.debug('epoch = ' + str(epoch) + ' ;   epoch_last_updated = ' + str(epoch_last_updated)  )

                    ###     Changes, Percentages
                    EUR_ch =  percent( prev_price_EUR, ITEM['price_EUR'], 2  )
                    BTC_ch =  percent( prev_price_BTC, ITEM['price_BTC'], 2  )
                    vol_24h_ch =  percent( prev_vol24h, ITEM['vol_24h_EUR'], 2  )
                    market_cap_ch =  percent( prev_market_cap_EUR, ITEM['market_cap_EUR'], 2  )

                    Changes = {'EUR_ch' :EUR_ch , 'BTC_ch':BTC_ch , 'vol_24h_ch' :vol_24h_ch , 'market_cap_ch' : market_cap_ch }

                    if epoch != epoch_last_updated :
                        logger.info('---' * 25 )

                        logger.info(Colors.bg.iceberg +'#' +str(cnt+1) +'  ' +str(  market) + Colors.fg.blue + ',  last_updated=' +str(ITEM['date'])+ ',   price_EUR=' +str(ITEM['price_EUR'])+',   price_BTC=' +str(ITEM['price_BTC']) +
                                ',   vol_24h_EUR= ' +str(ITEM['vol_24h_EUR'])+',  market_cap_EUR=' +str( ITEM['market_cap_EUR'])+
                                ',   price_ch_1h=' +str(ITEM['price_ch_1h'])+ ',  available_supply=' +str(ITEM['available_supply'])+',  rank=' +str( ITEM['rank']) + Colors.disable )

                        ###     Update current values with Percentages :
                        ITEM.update(Changes)
                        logger.debug('ITEM & Changes : ' + str(ITEM) )
                        insert_all_Items_query = QB.insert_query(market, **ITEM )
                        logger.debug('insert_all_Items_query : ' + str(insert_all_Items_query))

                        insert_Result = cur.execute( insert_all_Items_query)
                        logger.debug('insert_Result : ' + str(insert_Result) )
                        connection.commit()

                    else :
                        logger.warning(Colors.bg.yellow + 'Sorry, but past_epoch == current_epoch. NO NEWER DATA ! ' + Colors.disable )
                        break

            except:
                logger.warning('No such table ! We will create the table')
                #### ====== CREATE IF TABLE DOES NOT EXIST !!!!
                logger.warning('market ' + str(market) + ' was not found in TABLES_SET !!!!')
                createTableQuery = TABLES['create_coin_market_cap_table']
                logger.warning('createTableQuery : \t' + str(createTableQuery) )
                createTableResult = cur.execute( TABLES['create_coin_market_cap_table'].replace('table_name', market ) )
                logger.warning('createTableResult : \t' + str(createTableResult) )


                insert_initial_market_query = QB.insert_query(market, **ITEM )
                logger.warning('insert_initial_market_query : ' + str(insert_initial_market_query))

                initial_insert_Result = cur.execute( insert_initial_market_query )
                logger.warning('initial_insert_Result : ' + str(initial_insert_Result))


    connection.commit()
    connection.close()


t3  = time.time()
if system() == 'Linux' :
    logger.warning('market INSERT in tables took    ' + str(round((t3-t2),2)) + ' s' + ' ,     LOAD : ' + str(os.getloadavg() )  )

