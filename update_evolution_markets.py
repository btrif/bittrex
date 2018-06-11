#  Created by Bogdan Trif on 25-01-2018 , 7:10 PM.

import time
import pymysql
from conf.db_conn import *
from includes.DB_functions import *
from includes.app_functions import *

t1  = time.time()

debug_level = 0
date = time.strftime("%Y-%m-%d %H:%M:%S",   time.localtime() )

connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB1 )
cur = connection.cursor()

update_evolution_table_log =  'tmp/update_evolution_table.log'
valid_MARKETS = 'tmp/valid_Markets.txt'

t2  = time.time()
file_append_with_text(update_evolution_table_log,  str(date)+  '       update _evolution MARKETS read_file_line_by_line  took    ' + str(round((t2-t1)*1000,2)) + ' ms' )


def update_evolution_market(market):

    prep_MarketQuery  = 'SELECT id, last_price, buy_vs_sell, volume FROM `market_name` ORDER BY ID DESC LIMIT 1;'
    prep_MarketResult = cur.execute(prep_MarketQuery.replace('market_name', market))
    if prep_MarketResult == 1 :
        row = cur.fetchone()
        # if debug_level >= 2 : print('row :  ', row )

        last_id =  row[0]
        last_price =  float(row[1])
        last_BuyVsSell =  float( row[2] )
        last_Vol = float(row[3])



        ######### Updating / CREATING the rows MARKETS to the  _evolution table         #################
        prep_evolutionSelect = "select market, pr_ch_1m from _evolution where market = 'market_name' ;"
        # if debug_level >= 3 :        print('prep_evolutionSelect --->', prep_evolutionSelect )
        try :
            prep_evolutionResult = cur.execute(prep_evolutionSelect.replace('market_name', market))
            # if debug_level >= 3 :             print('prep_evolutionResult --> ', prep_evolutionResult )

            #####    If there is no row in evolution table, insert one
            if prep_evolutionResult == 0 :
                # if debug_level >= 3 :
                #     print('there is no such row in the evolution table. THIS WILL BE CREATED RIGHT NOW !' )
                createRowQuery = QUERIES['evolution_initial_insert']
                # if debug_level >= 3 :                     print(' createRowQuery --> ', createRowQuery)
                createRowResult = cur.execute( createRowQuery , (market ) )
                # if debug_level >= 3 :                     print('=== createRowResult  ', createRowResult)
                # connection.commit()



            #####   If there is a table UPDATE the existing values in the row market :
            elif prep_evolutionResult == 1 :
                # if debug_level >= 3 :               print('We will update all the values up to the corresponding times')

                first_market_query = cur.execute(QUERIES['first_market_row'].replace('market_name' , market) )
                market_first_id = cur.fetchone()[0]
                # if debug_level >= 3 :                         print('market_first_id = ', market_first_id )

                last_market_query = cur.execute(QUERIES['last_market_row'].replace('market_name' , market) )
                market_last_id = cur.fetchone()[0]
                # if debug_level >= 3 :                         print('market_last_id = ', market_last_id   )

                max_time_diff = market_last_id - market_first_id

                #####    DO THE UPDATES :
                i = 0
                PR_CH, BvS_CH = [], []
                upd_ch_Str = ''
                while  Times[i] <= max_time_diff :
                    # if debug_level >= 3 : print(Times[i],'    ', Periods[i])
                    value_behind = market_last_id-Times[i]
                    # if debug_level >= 3 : print('value of the price / BuyvsSell  behind = ', value_behind )

                    #### FIRST SELECT OLD VALUES :      ###############
                    price_and_buy_vs_sell_select = cur.execute( QUERIES['custom_price_and_buy_vs_sell_market'].replace('market_name', market), value_behind )
                        ########        PRICE & BUY_VS_SELL CHANGE      #########
                    # if debug_level >= 4 :   print('     price_and_buy_vs_sell_select ----------->', price_and_buy_vs_sell_select )
                    rowa = cur.fetchone()
                    previous_price, previous_buy_vs_sell = float(rowa[0]), float(rowa[1])
                    # if debug_level >= 3 :   print('previous_price, previous_buy_vs_sell = ', previous_price, previous_buy_vs_sell  )

                    priceChange = round( (( float(last_price) - previous_price ) / previous_price) * 100 , 2)
                    buy_vs_sellChange = round( (( float(last_BuyVsSell) - previous_buy_vs_sell ) / previous_buy_vs_sell) * 100 , 2)

                    # if debug_level >= 3 :
                    #     print('previous_price = ', previous_price, '      the most recent price = ', last_price , '    priceChange = ' , priceChange )
                    #     print('previous_buy_vs_sell = ', previous_buy_vs_sell, '      the most recent buy_vs_sell = ', last_BuyVsSell , '    buy_vs_sellChange = ' , buy_vs_sellChange )

                    PR_CH.append(priceChange)
                    BvS_CH.append(buy_vs_sellChange)
                    upd_ch_Str += 'pr_ch_'+str(Periods[i])+' = %s,'


                    i+=1

                # if debug_level >= 3 :    print('PR_CH = ', PR_CH ,'    BvS_CH =  ', BvS_CH)

                ########## CONSTRUCTING THE UPDATE QUERY , WE WILL UPDATE ALL Fields ONLY ONCE !  Like in example bellow :
                ## UPDATE `_evolution` SET pr_ch_2m=1.23, pr_ch_5m=-0.78  WHERE market='BTC-1ST';

                upd_BvS_Str = upd_ch_Str.replace('pr_ch_', 'BvsS_ch_')
                upd_BvS_Str = upd_BvS_Str.rstrip(',')
                update_Str = upd_ch_Str + upd_BvS_Str
                # print('update_Str : ', update_Str, '\n')
                update_evolution_Query = "UPDATE `_evolution` SET " + update_Str + " WHERE market='market_name';"

                # if debug_level >= 3 :                       print('update_evolution_Query : ', update_evolution_Query)
                VALUES_tuple = tuple(PR_CH) + tuple(BvS_CH)

                # if debug_level >= 3 :                print('VALUES tuple :::: ', VALUES_tuple )
                update_evolution_market = cur.execute(update_evolution_Query.replace('market_name', market) , VALUES_tuple   )






        except :
            # if debug_level >= 3 :
            print('To complete afterwards')





with open(valid_MARKETS, 'r') as f :
    for cnt, line in enumerate(f) :
        market = line.rstrip('\n')
        if debug_level >= 1 : print(str(cnt+1),'.    ', market )

        update_evolution_market(market)

        #
        #
        # prep_MarketQuery  = 'SELECT id, last_price, buy_vs_sell, volume FROM `market_name` ORDER BY ID DESC LIMIT 1;'
        # prep_MarketResult = cur.execute(prep_MarketQuery.replace('market_name', market))
        # if prep_MarketResult == 1 :
        #     row = cur.fetchone()
        #     if debug_level >= 2 : print('row :  ', row )
        #
        #     last_id =  row[0]
        #     last_price =  float(row[1])
        #     last_BuyVsSell =  float( row[2] )
        #     last_Vol = float(row[3])
        #
        #
        #
        #     ######### Updating / CREATING the rows MARKETS to the  _evolution table         #################
        #     prep_evolutionSelect = "select market, pr_ch_1m from _evolution where market = 'market_name' ;"
        #     if debug_level >= 3 :        print('prep_evolutionSelect --->', prep_evolutionSelect )
        #     try :
        #         prep_evolutionResult = cur.execute(prep_evolutionSelect.replace('market_name', market))
        #         if debug_level >= 3 :             print('prep_evolutionResult --> ', prep_evolutionResult )
        #
        #         #####    If there is no row in evolution table, insert one
        #         if prep_evolutionResult == 0 :
        #             if debug_level >= 3 :
        #                 print('there is no such row in the evolution table. THIS WILL BE CREATED RIGHT NOW !' )
        #             createRowQuery = QUERIES['evolution_initial_insert']
        #             if debug_level >= 3 :
        #                 print(' createRowQuery --> ', createRowQuery)
        #             createRowResult = cur.execute( createRowQuery , (market ) )
        #             if debug_level >= 3 :
        #                 print('=== createRowResult  ', createRowResult)
        #             connection.commit()
        #
        #
        #
        #         #####   If there is a table UPDATE the existing values in the row market :
        #         elif prep_evolutionResult == 1 :
        #             if debug_level >= 3 :
        #                 print('We will update all the values up to the corresponding times')
        #
        #             first_market_query = cur.execute(QUERIES['first_market_row'].replace('market_name' , market) )
        #             market_first_id = cur.fetchone()[0]
        #             if debug_level >= 3 :                         print('market_first_id = ', market_first_id )
        #
        #             last_market_query = cur.execute(QUERIES['last_market_row'].replace('market_name' , market) )
        #             market_last_id = cur.fetchone()[0]
        #             if debug_level >= 3 :                         print('market_last_id = ', market_last_id   )
        #
        #             max_time_diff = market_last_id - market_first_id
        #
        #             #####    DO THE UPDATES :
        #             i = 0
        #             PR_CH, BvS_CH = [], []
        #             upd_ch_Str = ''
        #             while  Times[i] <= max_time_diff :
        #                 if debug_level >= 3 : print(Times[i],'    ', Periods[i])
        #                 value_behind = market_last_id-Times[i]
        #                 if debug_level >= 3 : print('value of the price / BuyvsSell  behind = ', value_behind )
        #
        #                 #### FIRST SELECT OLD VALUES :      ###############
        #                 price_and_buy_vs_sell_select = cur.execute( QUERIES['custom_price_and_buy_vs_sell_market'].replace('market_name', market), value_behind )
        #                     ########        PRICE & BUY_VS_SELL CHANGE      #########
        #                 if debug_level >= 4 :   print('     price_and_buy_vs_sell_select ----------->', price_and_buy_vs_sell_select )
        #                 rowa = cur.fetchone()
        #                 previous_price, previous_buy_vs_sell = float(rowa[0]), float(rowa[1])
        #                 if debug_level >= 3 :   print('previous_price, previous_buy_vs_sell = ', previous_price, previous_buy_vs_sell  )
        #
        #                 priceChange = round( (( float(last_price) - previous_price ) / previous_price) * 100 , 2)
        #                 buy_vs_sellChange = round( (( float(last_BuyVsSell) - previous_buy_vs_sell ) / previous_buy_vs_sell) * 100 , 2)
        #
        #                 if debug_level >= 3 :
        #                     print('previous_price = ', previous_price, '      the most recent price = ', last_price , '    priceChange = ' , priceChange )
        #                     print('previous_buy_vs_sell = ', previous_buy_vs_sell, '      the most recent buy_vs_sell = ', last_BuyVsSell , '    buy_vs_sellChange = ' , buy_vs_sellChange )
        #
        #                 PR_CH.append(priceChange)
        #                 BvS_CH.append(buy_vs_sellChange)
        #                 upd_ch_Str += 'pr_ch_'+str(Periods[i])+' = %s,'
        #
        #
        #                 i+=1
        #
        #             if debug_level >= 3 :    print('PR_CH = ', PR_CH ,'    BvS_CH =  ', BvS_CH)
        #
        #             ########## CONSTRUCTING THE UPDATE QUERY , WE WILL UPDATE ALL Fields ONLY ONCE !  Like in example bellow :
        #             ## UPDATE `_evolution` SET pr_ch_2m=1.23, pr_ch_5m=-0.78  WHERE market='BTC-1ST';
        #
        #             upd_BvS_Str = upd_ch_Str.replace('pr_ch_', 'BvsS_ch_')
        #             upd_BvS_Str = upd_BvS_Str.rstrip(',')
        #             update_Str = upd_ch_Str + upd_BvS_Str
        #             # print('update_Str : ', update_Str, '\n')
        #             update_evolution_Query = "UPDATE `_evolution` SET " + update_Str + " WHERE market='market_name';"
        #
        #             if debug_level >= 3 :                       print('update_evolution_Query : ', update_evolution_Query)
        #             VALUES_tuple = tuple(PR_CH) + tuple(BvS_CH)
        #
        #             if debug_level >= 3 :                print('VALUES tuple :::: ', VALUES_tuple )
        #             update_evolution_market = cur.execute(update_evolution_Query.replace('market_name', market) , VALUES_tuple   )
        #
        #
        #
        #
        #
        #
        #     except :
        #         if debug_level >= 3 :
        #             print('To complete afterwards')

connection.commit()


t3  = time.time()
file_append_with_text(update_evolution_table_log,  str(date)+  '       _evolution table UPDATE took    ' + str(round((t3-t2)*1000,2)) + ' ms' )
file_append_with_text(update_evolution_table_log, '---'*30 )

connection.close()

