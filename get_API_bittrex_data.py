#  Created by Bogdan Trif on 10-01-2018 , 12:45 PM.

import time
import operator
import pymysql
import requests  # to make GET request
from conf.db_conn import *
from includes.DB_functions import *
from includes.app_functions import *
from os import remove, path

t1  = time.time()

debug_level = 1

connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB1 )
cur = connection.cursor()

date = time.strftime("%Y-%m-%d %H:%M:%S",   time.localtime() )
# print( 'Current Date   --->   ', date ,'\n'  )

URL = 'https://bittrex.com/api/v1.1/public/getmarketsummaries'

### Current Trading Market :
current_market_query = 'SELECT market, time_in FROM `_trade_now`;'
query_current_market_result = cur.execute( current_market_query )
if query_current_market_result > 0 :
    row_res = cur.fetchone()
    current_market = row_res[0]
if query_current_market_result == 0 :
    current_market = None
print('current market = ', current_market)

MARKETS = get_bittrex_Content(URL)

t2  = time.time()
file_append_with_text(API_bittrex_data_log,  str(date)+  '       getExternalContent( URL  ) took    ' + str(round((t2-t1)*1000,2)) + ' ms' )

###   Read valid Markets from file :
valid_MARKETS = set( read_file_line_by_line(valid_markets_file) )

t3  = time.time()
file_append_with_text(API_bittrex_data_log, str(date)+  '       valid_MARKETS took    ' + str(round((t3-t2)*1000,2)) + ' ms' )

if debug_level >= 1 :     print(len( valid_MARKETS) ,valid_MARKETS,'\n')


Strongest_Markets = dict()
cnt = 1


#### First we delete the _volumes table in order to update it after :
delete_old_volumes_data = 'DELETE FROM _volumes WHERE market = %s;'
delete_old_variations_data = 'DELETE FROM _variations WHERE market = %s;'


#######    Preparation QUERIES        #############
## prep Query for getting last minute items and compare them with the current ones  ###
prepQuery  = 'SELECT last_price, volume, buy_vs_sell, id FROM `market_name` ORDER BY ID DESC LIMIT 1;'

####  pre Query TO GET  the values with 60 minutes (1 hour) behind #####
prep_VolumeQuery_1h  = 'SELECT volume, last_price, buy_vs_sell, id FROM `market_name` WHERE ID = %s;'

####    pre Query TO GET  if the market is already in _VOLUMES TABLE or not     #####
prep_Query_Volumes_market  = 'SELECT market, status, time_in, volume FROM `_volumes` WHERE market = %s;'

####  pre Query TO GET  _history table values with 1 day offset behind #####
prep_HistoryQuery  = 'SELECT market, time, vol_ch_1h, id  FROM `_history` WHERE time >= DATE_SUB(NOW(), INTERVAL 1 DAY) AND market = %s;'




def _update_variations_table( market, debug_level ):
    BVS, PR_CH = [], []
    last_update = time.strftime("%Y-%m-%d %H:%M:%S",   time.localtime() )

    METRICS = { 'id':0, 'date' : 1,  'last_price':2, 'last_price_ch' :3, 'buy_vs_sell' :4, 'buy_vs_sell_ch' :5,
                'volume':6, 'vol_ch':7, 'buy_orders':8, 'sell_orders':9, 'vol_buy':10, 'vol_sell':11   }
    BvS_fields = ( 'BvS_1m', 'BvS_2m', 'BvS_5m', 'BvS_10m', 'BvS_15m', 'BvS_30m', 'BvS_1h', 'BvS_2h', 'BvS_4h', 'BvS_8h' )
    PR_CH_fields = ( 'pr_1m', 'pr_2m', 'pr_5m', 'pr_10m', 'pr_15m', 'pr_30m', 'pr_1h', 'pr_2h', 'pr_4h', 'pr_8h' )

    prep_variations_query = 'SELECT market, time_in FROM `_variations` WHERE market = %s;'
    prep_variations_res = cur.execute( prep_variations_query, (market) )

    ### Logs
    if debug_level >= 1 :
        file_append_with_text(API_bittrex_data_log,  str(last_update)+'   _update_variations_table = ' + str(market) +'   last_update =  ' + str(last_update) )
    if debug_level >= 2 :
        file_append_with_text(API_bittrex_data_log,  str(last_update)+  '   prep_variations_query =  ' + str(prep_variations_query)   )
        file_append_with_text(API_bittrex_data_log,  str(last_update)+  '   prep_variations_res =  ' + str(prep_variations_res)   )

    query_insert = 'INSERT INTO `_variations` (market, time_in) VALUES (%s, %s); '
    query_update = "UPDATE  `_variations` SET time_update='" +str(last_update)+"'"



    if prep_variations_res == 0 :
        # print('_variations_query_insert : ', query_insert)
        variations_insert_result  = cur.execute( query_insert, (market, last_update ) )
        connection.commit()

        # print('query_update : ', query_update )
        variations_update_result_1 =  cur.execute(query_update  )

         ### Logs
        if debug_level >= 2 :
            file_append_with_text(API_bittrex_data_log,  str(last_update)+  '   _variations_insert_result  =  ' + str(variations_insert_result)   )
            file_append_with_text(API_bittrex_data_log,  str(last_update)+  '   +variations_update_result_1 =  ' + str(variations_update_result_1)   )



    if prep_variations_res > 0 :
        row = cur.fetchone()
        time_in = row[1]
        delta = compute_dime_diff( str(time_in), last_update )

        index = binary_search(delta, Times )
        # print('delta : ' ,delta ,'            index = ', index )

        ### Logs
        if debug_level >= 3 :
            file_append_with_text(API_bittrex_data_log,  str(last_update)+  '   delta  =  ' + str(delta)  +',    index  =  ' + str(index)   )


        # query_insert += str(BvS_fields[:index+1]).lstrip('(').rstrip(')')
    # query_insert += str(PR_CH_fields[:index+1]).lstrip('(').rstrip(')')
    # query_insert += ', time_update ) VALUES '

        last_time = Times[index]
        print('last_time :' ,  last_time )
        dataset = get_complete_bittrex_dataset(market, last_time+1, 0  )
        # print(dataset)
        vol_buy = [ float( i[METRICS['vol_buy'] ] )  for i in dataset  if i[ METRICS['vol_buy'] ] != None ]
        print(' length :  ',len(vol_buy))
        vol_sell = [ float (i[METRICS['vol_sell'] ] )   for i in dataset if i[ METRICS['vol_sell'] ] != None  ]
        last_price = [ float (i[METRICS['last_price'] ] )   for i in dataset if i[ METRICS['last_price'] ] != None  ]

        # print('vol_buy : ', vol_buy )
        # print('vol_sell : ', vol_sell )
        index = min(9, index)           # 2018-05-22, LIMIT for a max of 8 hours, even if we have longer times for CURRENT MARKET
        for ind in range( index+1 ) :
            vol_buy_period = sum(vol_buy[ : Times[ind]])
            vol_sell_period = sum(vol_sell[ : Times[ind]])
            buy_vs_sell_diff_period = round(vol_buy_period - vol_sell_period, 4 )
            # print('ind =', ind,'      ', Times[ind] ,'    vol_buy_period = ',   vol_buy_period ,'    ' , vol_buy[ : Times[ind] ]   )
            print('ind =', ind,'      ', Times[ind] ,'    vol_sell_period = ',   vol_sell_period  ,'      length : ' ,len(vol_sell[ : Times[ind]] ) , '\n' , vol_sell[ : Times[ind]]   )
            print('ind =', ind , '         last_price :        index0 = ',  last_price[Times[0]] , '        last_index = ',Times[ind] ,'     ' ,last_price[Times[ind]]  )
            last_price_ch = round((last_price[0] - last_price[Times[ind]])/last_price[Times[ind]]*100 , 2 )

            print('buy_vs_sell_diff_period =', buy_vs_sell_diff_period ,  '        last_price_ch  =  ',  last_price_ch   )

            query_update += ', '+str(BvS_fields[ind])+'=' +str(buy_vs_sell_diff_period)
            query_update += ', '+str(PR_CH_fields[ind])+'=' +str(last_price_ch)

            BVS.append(buy_vs_sell_diff_period)
            PR_CH.append(last_price_ch)
        print('BVS : ', tuple(BVS) )
        print('PR_CH : ', PR_CH, '\n' )

        query_update += " WHERE market = '" + str(market)+"';"


        print('_variations query_update  : ', query_update  )

        final_update_res_2 = cur.execute(query_update  )
        ### Logs
        if debug_level >= 2 :
            file_append_with_text(API_bittrex_data_log,  str(last_update)+  '   _variations query_update =  ' + str(query_update)   )
            file_append_with_text(API_bittrex_data_log,  str(last_update)+  '   final_update_res_2 =  ' + str(final_update_res_2)   )


    # connection.commit()






###         MAIN        ####

for ITEM in MARKETS :
    market = ITEM["MarketName"]


    if market in valid_MARKETS :
        print('\nmarket = ', market )

        lastPrice = format(  ITEM["Last"] ,'.8f')
        # print('\nlastPrice : ', lastPrice, type(lastPrice))
        volume = round( ITEM["BaseVolume"] , 4 )
        bid = format( ITEM["Bid"] , '.8f')
        ask = format(  ITEM["Ask"] , '.8f' )

        openBuyOrders = int(ITEM["OpenBuyOrders"])
        openSellOrders = int(ITEM["OpenSellOrders"])

        if ( openBuyOrders > 0 and openSellOrders > 0) :
            buyVsSell = round( openBuyOrders/openSellOrders, 4 )
        else :
            openSellOrders = 0

        ### Logs
        if debug_level >= 3 :
            # print('\n',str(cnt) +'.     ',  market, '      lastPrice =' ,  lastPrice , type(lastPrice)   , '     bid = ' , bid, type(bid),  '      ask =' , ask, type(ask) ,  '       volume =  ' , volume , type(volume) ,'     openBuyOrders = ' , openBuyOrders, type(openBuyOrders) ,  '     ' , openSellOrders , '     ' , buyVsSell )
            file_append_with_text(API_bittrex_data_log, '\n'+ str(date)+  '   market =  ' + str(market) + ' ,    lastPrice = ' + str(lastPrice) + ' ,   bid = ' + str(bid) + ' ,   ask = ' + str(ask)  )
            file_append_with_text(API_bittrex_data_log,  str(date)+  '   market =  ' + str(market) + ' ,    volume = ' + str(volume) + ' ,   openBuyOrders = ' + str(openBuyOrders) + ' ,   openSellOrders = ' + str(openSellOrders)+ ' ,   buyVsSell = ' + str(buyVsSell)  )

        cnt +=1


        ###### Select data from table using SQL query.  ####
        try :
            prepResult = cur.execute(prepQuery.replace('market_name' , market))
            # print(prepResult)


            if (prepResult > 0) :
                row = cur.fetchone()
                prevPrice =  float(row[0])
                prevVol = float(row[1])
                prevBuyVsSell =  float( row[2] )
                last_id = int(row[3])

                ### Logs
                if debug_level >= 3 :
                    file_append_with_text(API_bittrex_data_log,  str(date)+ '   market =  '+ str(market) + ' ,    last_id = ' + str(last_id) + ' ,   prevPrice = ' + str(prevPrice) + '  ,  prevVol = ' + str(prevVol) + '    prevBuyVsSell = ' + str(prevBuyVsSell) )


                priceCh = round( (( float(lastPrice) - prevPrice ) / prevPrice) * 100 , 2)
                volCh = round( (( float(volume) - prevVol) / prevVol) * 100 , 2)
                buyVsSellCh = round( (( buyVsSell - prevBuyVsSell) / prevBuyVsSell) * 100 , 2 )

                #### Insert into the table the row with priceCh volCh, buyVsSellCh calculated :
                second_insert_Result = cur.execute( QUERIES['second_insert_query'].replace('table_name', market),
                                                 (date, lastPrice, priceCh, buyVsSell, buyVsSellCh, volume, volCh, bid, ask, openBuyOrders, openSellOrders) )

                 #########################################
                 #####                                                              #####
                 #####        _VOLUME, _HISTORY  TABLES        #####
                 #####                                                              #####
                 #########################################

                #    We select only the BTC markets  with SECURE VOLUME :
                if ( market.startswith('BTC-') and market not in EXCLUDE_MARKETS ) :
                    if volume < 5 :
                        delete_volumes_markets1 = cur.execute(delete_old_volumes_data, (market) )
                        delete_variations_markets1 = cur.execute(delete_old_variations_data, (market) )

                        if debug_level >= 2 :
                            print('delete_volume_markets1  : ', delete_volumes_markets1  )
                            print('delete_variations_markets1  : ', delete_variations_markets1  )

                    if volume >= 5  :
                        connection.commit()     #  !!  VERY IMPORTANT ! WE MUST UPDATE THE ROW so that we can fill REAL BUY & SELL VOL

                        #### Put the BTC markets in a dictionary and then in the    :
                        if volCh > 1  :   # buyVsSellCh > 2 and priceCh > 0 and
                            Strongest_Markets[market] = ( volCh, priceCh, buyVsSellCh, volume, buyVsSell )


                        ####        We check values 60 minutes ago to put them in the _VOLUMES TABLE table in DB:       ########
                        try :
                            if debug_level >= 2 : print('prepQuery_1h = ', prep_VolumeQuery_1h)
                            prepResult_1h = cur.execute(prep_VolumeQuery_1h.replace('market_name', market), (last_id - 59))
                            if debug_level >= 2 : print('prepResult_1h  : ', prepResult_1h  )
                            row_1h = cur.fetchone()
                            if debug_level >= 2 : print('row_1h :  ', row_1h )

                            if prepResult_1h > 0  :
                                prevVol_1h = float(row_1h[0])
                                prevPrice_1h =  float(row_1h[1])
                                prevBuyVsSell_1h =  float( row_1h[2] )
                                last_id_1h = int(row_1h[3])

                                volCh_1h = round( (( float(volume) - prevVol_1h) / prevVol_1h) * 100 , 2)
                                priceCh_1h = round( (( float(lastPrice) - prevPrice_1h ) / prevPrice_1h) * 100 , 2)
                                buyVsSellCh_1h = round( (( buyVsSell - prevBuyVsSell_1h) / prevBuyVsSell_1h) * 100 , 2 )

                                if debug_level >= 2 :
                                    print( 'prevVol_1h = ', prevVol_1h ,'     prevPrice_1h = ' , prevPrice_1h ,  '    prevBuyVsSell_1h = ', prevBuyVsSell_1h,  '    last_id_1h = ', last_id_1h  )
                                    print( 'last_id_1h = ', last_id_1h ,'     volCh_1h = ' , volCh_1h ,  '    priceCh_1h = ', priceCh_1h,  '    buyVsSellCh_1h = ', buyVsSellCh_1h  )

                                if volCh_1h >= 5 or market == current_market :
                                    ### Logs
                                    if debug_level >= 1 :
                                        file_append_with_text(API_bittrex_data_log, str(date)+  '   == market_name :   ' + str(market) +' ,   volCh_1h = ' + str(volCh_1h)  )

                                    ## UPDATE  WATCH COIN in their market table    ####

                                    ### Get the REAL VOLUME (buy & sell )
                                    buy_vol, sell_vol = get_market_real_volume( market , 1 )

                                    ### Update WATCH COIN markets the fields vol_buy & vol_sell in the markets monitored  by _volumes ####
                                    prep_Update_Watch_markets = 'UPDATE `market_name` SET vol_buy=%s, vol_sell=%s WHERE id=%s;'
                                    update_Watch_markets = cur.execute(prep_Update_Watch_markets.replace('market_name', market), (buy_vol, sell_vol, last_id+1) )

                                    ### Logs
                                    if debug_level >= 2 :
                                        file_append_with_text(API_bittrex_data_log,  str(date)+  '   update_Watch_markets =  ' + str(update_Watch_markets) )

                                    ####  END UPDATE  BTC-ADA types in their market table   ####

                                    ####################################
                                    ####                                                        ####
                                    ####        Update _VOLUMES TABLE        ####
                                    ####                                                        ####
                                    ####################################
                                    prep_Result_Volumes_market  = cur.execute( prep_Query_Volumes_market , (market) )

                                    ###  Logs
                                    if debug_level >= 2 :
                                        # file_append_with_text(API_bittrex_data_log,  str(date)+  '   prep_Query_Volumes_market =  ' + str(prep_Query_Volumes_market) )
                                        file_append_with_text(API_bittrex_data_log,  str(date)+  '   prep_Result_Volumes_market =  ' + str(prep_Result_Volumes_market) )


                                    if prep_Result_Volumes_market == 0 :    # If we have NEGATIVE response we insert a new market :
                                        insert_markets_Volumes = cur.execute( QUERIES['insert_volumes'] ,
                                             (market, date, date, volCh_1h, volCh, priceCh_1h, priceCh, buyVsSellCh_1h, buyVsSellCh, volume, buy_vol, sell_vol, lastPrice, buyVsSell, 'new') )
                                        ### Logs
                                        if debug_level >= 2 :
                                            # print('insert_markets_Volumes  : ', insert_markets_Volumes  )
                                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   insert_markets_Volumes =  ' + str(insert_markets_Volumes) )

                                    elif prep_Result_Volumes_market != 0 :
                                        update_markets_Volumes = cur.execute( QUERIES['update_volumes'] ,
                                         (date, volCh_1h, volCh, priceCh_1h, priceCh, buyVsSellCh_1h, buyVsSellCh, volume, buy_vol, sell_vol, lastPrice, buyVsSell, 'watch', market) )
                                        ### Logs
                                        if debug_level >= 2 :
                                            # print('update_markets_Volumes  : ', update_markets_Volumes  )
                                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   update_markets_Volumes =  ' + str(update_markets_Volumes) )

                                    ######################################
                                    ####                                                            ####
                                    ####        Update _VARIATIONS TABLE        ####
                                    ####                                                            ####
                                    ######################################

                                    UPDATE_VARIATIONS = _update_variations_table(market, 0 )


                                    ####        DELETE _VOLUMES  old records :      ####
                                if volCh_1h < 5 and market != current_market :
                                    delete_volumes_markets2 = cur.execute(delete_old_volumes_data, (market) )
                                    delete_variations_markets2 = cur.execute(delete_old_variations_data, (market) )
                                    ### Logs
                                    if debug_level >= 3 :
                                        file_append_with_text(API_bittrex_data_log,  str(date)+ '    ' + str(market) + '   delete_volume_markets2 =  ' + str(delete_volumes_markets2) )
                                        file_append_with_text(API_bittrex_data_log,  str(date)+ '    ' + str(market) + '   delete_variations_markets2 =  ' + str(delete_variations_markets2) )

                                    ### END Update _VOLUMES TABLE ####


                                    ###############################
                                    ####                                               ####
                                    ####         _HISTORY TABLE            ####
                                    ####                                               ####
                                    ###############################

                                    prep_History_Result_24h = cur.execute(prep_HistoryQuery, (market) )
                                    ### Logs
                                    if debug_level >= 4 :
                                        file_append_with_text(API_bittrex_data_log,  str(date)+  '   prep_History_Result_24h =  ' + str(prep_History_Result_24h) )


                                    if prep_History_Result_24h == 0 :
                                        ### Logs
                                        if debug_level >= 2 :
                                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   We do not have this market in the last 24h !   ' )
                                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   We will add this to the _history TABLE !!!  ' )

                                        #### Here we insert in the _HISTORY TABLE the volumes with vol_ch_1h > 5    ######
                                        insert_History = cur.execute( QUERIES['history_insert'],
                                                      (date, market, volCh_1h, volCh, priceCh_1h, priceCh, buyVsSellCh_1h, buyVsSellCh, volume, lastPrice, buyVsSell, 'vol' ) )
                                        ### Logs
                                        if debug_level >= 2 :
                                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   insert_History =  ' + str(insert_History) )

                                    if prep_History_Result_24h != 0 :
                                        row_24h = cur.fetchone()
                                        ### Logs
                                        if debug_level >= 4 :
                                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   There is already history within 24 hours for the market : =  ' + str(market) )
                                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   row_24h =  ' + str(row_24h) )

                                    #########  END _HISTORY TABLE    ###########


                        except IndexError :
                            print(' There are no 60 records behind !!!!!! ')
                            file_append_with_text(API_bittrex_data_log,  str(date)+  '   There are no 60 records behind !!!!!!  '  )



        except Exception :
            if debug_level >= 2 :               print('No such table ! We will create the table')
            #### ====== CREATE IF TABLE DOES NOT EXIST !!!!
            ### Logs
            if debug_level >= 2 : file_append_with_text(API_bittrex_data_log,  str(date)+ '   market ' + str(market) +'  was not found in TABLES_SET !!!!' )

            createTableQuery = TABLES['create_market_name']
            ### Logs
            if debug_level >= 2 : file_append_with_text(API_bittrex_data_log,  str(date)+ '   market ' + str(market) +'  createTableQuery : '+str(createTableQuery) )
            createTableResult = cur.execute( TABLES['create_market_name'].replace('table_name', market ) )
            ### Logs
            if debug_level >= 2 : file_append_with_text(API_bittrex_data_log,  str(date)+ '   market ' + str(market) +'  createTableResult : '+str(createTableResult) )



            if debug_level >= 3 : print(QUERIES['initial_insert_query'].replace('table_name', market))
            initial_insert_Result = cur.execute( QUERIES['initial_insert_query'].replace('table_name', market),
                                                 (date, lastPrice, buyVsSell, volume, bid, ask, openBuyOrders, openSellOrders) )
            connection.commit()



connection.commit()
# connection.rollback()
connection.close()


t4  = time.time()
file_append_with_text(API_bittrex_data_log,  str(date)+  '       market INSERT in tables took    ' + str(round((t4-t3)*1000,2)) + ' ms' )
file_append_with_text(API_bittrex_data_log,  str(date)+  '       current_market :   ' + str(current_market)  )

###     STRONGEST   MARKETS     #####
# We empy the file if we don't have valid markets
# if len(Strongest_Markets) == 0 :
#     open(strongest_markets_file, 'w').close()

if len(Strongest_Markets) > 0 :
    Strongest_Markets = sorted( Strongest_Markets.items(), key=operator.itemgetter(1) )

    file_append_with_text(API_bittrex_data_log,  '_ _ '*15+'   Strongest Markets :'+'_ _ '*15 )
    file_append_with_text(API_bittrex_data_log,  ' = vol_Ch, price_Ch, buy_Vs_Sell_Ch, volume, buy_Vs_Sell =' )
    file_append_with_text(API_bittrex_data_log,    str(Strongest_Markets) )
    # file_append_with_text(API_bittrex_data_log,    str(Strongest_Markets[-20:][::-1][4:8]) )
    # file_append_with_text(API_bittrex_data_log,    str(Strongest_Markets[-20:][::-1][8:12]) )
    # file_append_with_text(API_bittrex_data_log,    str(Strongest_Markets[-20:][::-1][12:16]) )
    # file_append_with_text(API_bittrex_data_log,    str(Strongest_Markets[-20:][::-1][16:20]) )


###     END     STRONGEST   MARKETS     #####


    ##### WRITE  to the Strongest Markets FILE   ######
    #######         Write to file Strongest Markets      ######

    # SM = Strongest_Markets[::-1]           #       All the results

    # if debug_level >= 1 : print('\nSM: ', len(SM), SM ,'\n' )



    # outF = open(strongest_markets_file , 'w')
    # outF.write( ' = vol_Ch, price_Ch, buy_Vs_Sell_Ch, volume, buy_Vs_Sell =\n' )
    # for  cnt, candidate in enumerate(SM) :
    #     market = candidate[0]
    #     values = candidate[1]
    #     outF.write( market +',' + str(values[0])+','+ str(values[1])+','+ str(values[2])+','+ str(values[3])+','+ str(values[4]) +'\n' )
    #     if debug_level >= 1 :      print( str(cnt+1)+'.     ',  market,'    ',  values )
    #
    # outF.close()


file_append_with_text(API_bittrex_data_log, '---'*30 )
if debug_level >= 1 : print( '\nCurrent Date   --->   ', date ,'\n'  )
