#  Created by Bogdan Trif on 30-01-2018 , 11:28 PM.



import time, os
from platform import system

import pymysql
import requests  # to make GET request
from conf.sensitive import *
from includes.DB_functions import *
from includes.app_functions import *


time.sleep(3)
cur_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

debug_level = 3

###     URL of the API
URL_global = 'https://api.coinmarketcap.com/v1/global/?convert=EUR'


###     LOGGING     ###
config = init_logging( config_file= 'conf/logging.json' , log_type= 'json' )
logging.config.dictConfig(config['logging'])
logger = logging.getLogger('get_API_coinmarketcap_markets_data')


#####        Connection  to coinmarketcap DB        #####
connection = pymysql.connect( host= localhost, port=PORT_NR, user=USER, passwd=PASSWD, db=cmc )
cur = connection.cursor()


class Computations():
    ''':Description: Class to make only needed computations for price_ch, volume_ch, buy_vs_sell_ch
    '''
    def __init__(self, Last_Values, Previous_Values):
        self.Last_Values = Last_Values
        self.Previous_Values = Previous_Values
        self.Names = ['Price_ch', 'Vol_ch', 'BvS_ch' ]
        self.Price_Vol_ch = self.get_values_changes()


    def get_values_changes(self) :
        ''':Description: constructs an array of changes up to the given period of time which is maximum

        :param Last_Values: lst,  Last_Values is an array containing the data taken from the API  in the form [last_Price, volume, buyVsSell ]
        :param Previous_Values: 2d array ; in the form [ [last_Price, volume, buyVsSell ]_5m, [last_Price, volume, buyVsSell ]_15m, ...  ]
        :return: 2D array, in the form [[ pr_ch_5m, vol_ch_5m, , BvS_ch_5m ], [ pr_ch_15m, vol_ch_15m, , BvS_ch_15m], ... ]
        '''

        Price_Vol_BvS_ch =[]
        for i in range(len(self.Previous_Values)) :
            logger.debug( str(Periods[i]) +'  :  Last_Values : ' +str( self.Last_Values[i]) + '    Previous_values : ' +str( self.Previous_Values[i]) )
            perc = [ percent( p, self.Last_Values[i], 2) for p in self.Previous_Values[i] ]
            logger.debug(str(self.Names[i])+ ' percentages : ' + str(perc) )
            Price_Vol_BvS_ch.append(perc)

        return Price_Vol_BvS_ch



class DatabaseCollection() :
    ''':Description: Class to Collect OLD Items from a SINGLE TABLE. It needs current values obtained
        from DataPreparation Class in order to replace missing values from Db when getting the
            old values array of [ old_prices 5m, 15m, 30m ...,  ] ,[ old_volumes 5m, 15m, 30m ...,  ],[ old_buy_vs_sell 5m, 15m, 30m ...,  ]
    '''
    def __init__(self, table_name ):
        self.QB = QueryBuilder()
        self.table_name = table_name


        ###  Method Calls
        self.last_id=  self.get_last_index()
        logger.debug('self.last_id  : ' + str(self.last_id) )

        self.max_index_diff = self.compute_max_index_behind()
        logger.debug('self.max_index_diff  : ' + str(self.max_index_diff) )

        self.Old_Values = self.get_old_values()
        logger.debug('self.Old_Values  : ' +str( self.Old_Values) )


    def get_last_index(self):
        '''                :return: last_id from a table                    '''
        last_index_query = self.QB.select_query(self.table_name, 'ORDER BY ID DESC LIMIT 1', *['id']  )
        try :
            result = cur.execute(last_index_query)
            print('result : ', result)
            last_id = cur.fetchone()[0]
            print('last_id : ', last_id )
            return last_id    # diff index return

        except :
            logger.warning('Sorry no such entry in the table. This means that there is no record in the table :  ' + str(self.table_name) )


    def compute_max_index_behind(self ):
        ''':Description: Computes the maximum index behind the last record by taking the first index in the table.
         We need this information to know how much time we can go back to extract values.        '''
        # 'select id from `market_name` ORDER BY ID LIMIT 1;'
        if self.get_last_index() != None :
            first_id_query = self.QB.select_query(self.table_name, 'ORDER BY ID LIMIT 1', *['id'] )
            # logger.debug('query : ' +str(first_id_query) )
            first_id_result = cur.execute(first_id_query)
            first_id = cur.fetchone()[0]
            logger.debug('first_id : ' + str(first_id))


            # logger.debug('first_id : ' +str( first_id) + '     self.last_id = ' +str( self.last_id )+ '     indeces behind = ', +str(self.last_id - first_id +1) )
            return self.last_id - first_id +1     # diff index return


    def get_old_values(self ):
        ''':Description: Method to get the old values FROM DATABASE.
        :param cur_values: lst, array with [last_price, last_vol, last_buy_vs_sell ]
        :return: lst of list, three arrays of int with values in the form [ [ old_prices ... ], [old_volumes] ,[old_buy_vs_sells] ]         '''
        Prices, Vols, =[], []
        i=0
        for i in range(len(Times)) :
            if Times[i] <= self.max_index_diff :
                index = self.last_id - Times[i]+1
                logger.debug(str(i) +'    Time_index : ' + str(Times[i]) +  '     Period : '+ str(Periods[i]) + '   index = ' +str( index) )
                # 'SELECT id, last_price, volume, buy_vs_sell from `market_name` WHERE ID = %s;'
                Select_result = 0
                while Select_result != 1 :
                    try :
                        Sel_query = self.QB.select_query(self.table_name, 'WHERE ID='+str(index)  ,*['market_cap_EUR', 'volume_24h', 'id' ] )
                        logger.debug('Sel_query : ' + str(Sel_query) )
                        Select_result = cur.execute(Sel_query)
                        logger.debug('Select_result : ' + str(type(Select_result)) +str( Select_result)  )
                        Vals = cur.fetchone()
                        logger.debug(str(Periods[i]) +' : old values : ' + str(Vals) )
                        Prices.append(Vals[0])
                        Vols.append(Vals[1])


                    except :
                        # print('\n' + Colors.fg.purple+' SORRY ! No id with this value = ' +str(index) + Colors.reset )
                        index +=1
                        logger.debug('Select_result = ' + str(Select_result) + '     We try index =' + Colors.fg.purple+ str(index) + Colors.reset)


        logger.debug('Prices : ' + str( Prices) )
        logger.debug('Vols : '+ str(Vols ) )


        return Prices, Vols

    def get_custom_values_behind(self, period_behind ):
        ''':Description: finds the old values corresponding to period behind. E.g. find the values of 5m, 15m, 30m, 1h, 4h, 8h, 1d, ...
        :param period_behind: string, of the form 5m, 15m, 30m, 1h, 4h, 8h, 1d, ...
        :return: tuple (Price, Vol, BvS),  of its corresponding index if it is found                               '''
        try :
            index_behind = Periods.index(period_behind)
            # print('index_behind = ', index_behind)
            return self.Old_Values[0][index_behind], self.Old_Values[1][index_behind]
        except ValueError :
            logger.warning(Colors.bg.yellow + 'The period ' + period_behind +' is not among ' + str(Periods) +' !!!. Try to use one of these !' +Colors.reset  )
            return -10, -10, -10        # return to avoid errors




class CoinMarketCapDatabaseOps( ):
    ''':Description: Class used only for bittrex database specific operations.
        It aggregates and prepares all the parameters in a single dictionary of **kwargs    '''
    def __init__(self, table_name, cur_vals, other_vals ):
        self.table_name = self.market_name = table_name
        self.cur_vals = cur_vals
        self.percentages = self.other_vals =  other_vals
        # self.kwargs_btc_ada = self.assemble_main_markets_args()
        # self.kwargs_evolution = self.assemble_evolution_markets_args()


    def assemble_market_cap_args(self):       ###     '_market_cap' TABLE
        print('self.cur_vals : ', self.cur_vals ,'   self.percentages : ', self.percentages)
        names = ['market_cap_ch', 'volume_24h_ch' ]
        changes =  { names[i] : self.percentages[i][0] for i in range(len(self.percentages ))  }
        print('cur_vals : ', self.cur_vals  )
        print('changes  : ', changes )

        _kwargs = { **self.cur_vals, **changes   }
        print('kwargs : ', _kwargs )
        return _kwargs



    def assemble_evolution_markets_args(self):      ###     _evolution TABLE
        print('assemble_evolution_markets_args  :   cur _vals = ' + str( self.cur_vals)  )

        PR , VOL  = self.other_vals[0], self.other_vals[1]
        pr_0, vol_0 = self.cur_vals['market_cap_EUR'], self.cur_vals['volume_24h']
        print(' pr_0, vol_0 :  ', pr_0, vol_0)

        # we insert the current values at the beginning of the list
        PR.insert(0, pr_0) ; VOL.insert(0, vol_0 )
        print('PR , VOL  :  ' + str(PR) + str(  VOL)    )

        # take only as much name keys as needed
        Price_per = Price_periods[:len(PR)]
        Vol_per = Vol_periods[:len(VOL)]

        print(' Price_periods , Vol_periods : ', Price_per, Vol_per )

        # make a dictionary of key:values. Like {'pr_0' : 0.02, 'pr_5m: 0.4, ... } ; {'vol_0' : 62.23, 'vol_5m: 64.45, ... }, ...
        Prices = { Price_per[i] : PR[i] for i in range(len(PR))  }
        Volumes = { Vol_per[i] : VOL[i] for i in range(len(VOL))  }

        __kwargs = { **Prices, **Volumes, **{'time' : cur_datetime} }
        logger.debug('assemble_evolution_markets_args __kwargs : ' + str( __kwargs) )
        return __kwargs


    def assemble_percentages_markets_args(self):        ###     _percentages TABLE
        # print('assemble_evolution_markets_args  :   cur _vals = ' + str( self.cur_vals)  )

        PR , VOL   = self.percentages[0], self.percentages[1]   # only vals from 5 min up

        # logger.debug('percentages :  PR, VOL :  ' + str(PR) + str( VOL) )


        # take only as needed name keys as needed
        Price_per = Price_periods[1:len(PR)+1]
        Vol_per = Vol_periods[1:len(VOL)+1]

        # print(' Price_periods , Vol_periods, BvS_periods : ', Price_per, Vol_per, BvS_per )

        # make a dictionary of key:values. Like {'pr_0' : 0.02, 'pr_5m: 0.4, ... } ; {'vol_0' : 62.23, 'vol_5m: 64.45, ... }, ...
        Prices_ch = { Price_per[i] : PR[i] for i in range(len(PR))  }
        Volumes_ch = { Vol_per[i] : VOL[i] for i in range(len(VOL))  }

        logger.debug('Prices_ch, Volumes_ch : ' + str(Prices_ch) + str(Volumes_ch) )
        __kwargs = { **Prices_ch, **Volumes_ch, **{'time' : cur_datetime} }
        # print('percentages __kwargs : ', __kwargs )
        logger.debug('assemble_evolution_markets_args __kwargs : ' + str( __kwargs) )
        return __kwargs


class DatabaseOperations( ):
    ''':Description: Class to do INSERT, UPDATE Operations on a SINGLE TABLE   '''
    def __init__(self, market, kwargs ):
        self.QB = QueryBuilder()
        # print('self.DC = ', len(self.DC.MARKETS), self.DC.MARKETS )
        self.table_name = market
        self.kwargs = kwargs
        logger.debug('DatabaseOperations table_name  & **kwargs : ' + str( self.table_name) + '  ' +str(self.kwargs ) )


    def update_row_in_evolution(self):
        try :
            update_query = self.QB.update_query('_evolution' , 'market' , self.table_name, **self.kwargs )
            logger.debug('update_evolution_query : ' + str( update_query) )
            update_res = cur.execute(update_query)
            # print('update__evolution_res  : ' + str(update_res))

            if update_res ==0 :
                insert_query = self.QB.insert_query('_evolution', **{'market': self.table_name, 'time' : cur_datetime } )
                logger.debug('insert evolution query : ' +  str(insert_query))
                ins_res = cur.execute(insert_query)
                # print('insert evolution result : ' +  str(ins_res))

        except  :
            logger.error('There was an error at update__evolution_res !')


    def update_row_in_percentages(self):
        logger.debug('all_percentages :   '+ str(self.kwargs)  )
        try :
            update_percentages_query = self.QB.update_query('_percentages' , 'market' , self.table_name, **self.kwargs )
            logger.debug('update_percentages_query : ' + str( update_percentages_query) )
            update_perc_res = cur.execute(update_percentages_query)
            # print('update_perc_res  : ' + str(update_perc_res))

            if update_perc_res == 0 :
                insert_perc_query = self.QB.insert_query('_percentages', **{'market': self.table_name, 'time' : cur_datetime } )
                logger.debug('insert _percentages query : ' +  str(insert_perc_query))
                ins_vol_res = cur.execute(insert_perc_query)
                # print('insert _volumes result : ' +  str(ins_vol_res))

        except  :
            logger.error('There was an error at update_percentages_res !')




def select_Total_Market_Cap_metrics_for_DB(DATA):
    ITEMS = {}

    ITEMS['market_cap_EUR'] = int(DATA['total_market_cap_eur'])
    ITEMS['market_cap_USD'] = int(DATA['total_market_cap_usd'])
    ITEMS['volume_24h'] = int(DATA['total_24h_volume_eur'])
    ITEMS['BTC_dominance'] = format( DATA["bitcoin_percentage_of_market_cap"], '.2f' )
    ITEMS['date'] = time.strftime(date_pattern, time.localtime(DATA['last_updated']))

    return ITEMS



################        END DEF         ################



req  = requests.get(URL_global )
DATA = req.json()


logger.debug('DATA : ' + str(DATA) )


ITEM = select_Total_Market_Cap_metrics_for_DB(DATA)
logger.debug('Current_Vals : ' + str(ITEM) )


# total_market_cap_eur = int(DATA['total_market_cap_eur'])
# total_market_cap_usd = int(DATA['total_market_cap_usd'])
# total_24h_volume_eur = int(DATA['total_24h_volume_eur'])
# BTC_dominance = format( DATA["bitcoin_percentage_of_market_cap"], '.2f' )

logger.info('last_updated : ' + str(ITEM['date']) + ' ,  market_cap_EUR=' + str(ITEM['market_cap_EUR']) + '  , volume_24h= '+ str(ITEM['volume_24h'])  +
            ',    market_cap_USD=' + str(ITEM['market_cap_USD']) + ',   BTC_dominance=' + str(ITEM['BTC_dominance']) )


QB = QueryBuilder()

####    Compare the past date from DB to the last date taken from  coinmarketcap.com
###     This needs to be done to not insert redundant data, as coinmarketcap.com does NOT always updates the API

# prepQuery  = 'SELECT market_cap_EUR, volume_24h, date, id FROM `_market_cap` ORDER BY ID DESC LIMIT 1;'
past_date_query = QB.select_query('_market_cap' ,'ORDER BY ID DESC LIMIT 1', 'date')
logger.debug('past date_query : ' + str(past_date_query))
past_date_result = cur.execute(past_date_query)
logger.debug('past date_result : ' + str(past_date_result))

past_date = cur.fetchone()[0]
logger.debug('past_date : ' + str(past_date))

past_epoch = int(time.mktime(time.strptime(str(past_date), date_pattern)))
logger.debug('past_epoch : ' + str(past_epoch))

current_epoch = int(convert_time_standard_format_to_epoch(ITEM['date']))
logger.debug('current_epoch : ' + str(current_epoch))

if past_epoch != current_epoch :

    DC = DatabaseCollection('_market_cap')

    ### 1.      Last values
    Values_5m = DC.get_custom_values_behind('5m')
    logger.debug('Values_5m  : ' + str(Values_5m) )

    ### 2.      Get Old Values for Price, Volume, buy_vs_sell @ 5m, 15m, 30m, 1h, 4h, 8h, 1d, ....
    All_Old_Values = DC.Old_Values
    logger.debug(Colors.fg.green + 'Pr_Vol_Old_Values  : ' + str(All_Old_Values) + Colors.disable )

    ### 3.  Percentages ALL
    Current_Vals = ITEM['market_cap_EUR'] , ITEM['volume_24h']
    All_Percentages = Computations(Current_Vals, All_Old_Values).Price_Vol_ch
    logger.debug('All_Percentages  : ' + str(All_Percentages) )


    ### 4.     Gather All the **kwargs to INSERT into the _market_cap TABLE
    kwargs_market_cap = CoinMarketCapDatabaseOps('_market_cap', ITEM, All_Percentages ).assemble_market_cap_args()
    logger.debug('kwargs_market_cap : ' + str(kwargs_market_cap ))

    total_cap_query = QB.insert_query('_market_cap', **kwargs_market_cap )
    logger.debug('total_cap_query : ' + str(total_cap_query))
    total_cap_result = cur.execute(total_cap_query)
    logger.debug('total_cap_result : ' + str(total_cap_result))

    connection.commit()
    connection.close()

    #####        Connection  to BITTREX DB        #####
    connection = pymysql.connect( host= localhost, port=PORT_NR, user=USER, passwd=PASSWD, db=bittrex )
    cur = connection.cursor()

    # ### 5.    _EVOLUTION TABLE      ###
    #
    # kwargs_evolution = CoinMarketCapDatabaseOps('_market_cap', ITEM, All_Old_Values ).assemble_evolution_markets_args()
    # DO = DatabaseOperations('_market_cap', kwargs_evolution)
    # DO.update_row_in_evolution()

    ### 6.     _PERCENTAGES TABLE      ###
    kwargs_percentages = CoinMarketCapDatabaseOps('_market_cap', ITEM, All_Percentages ).assemble_percentages_markets_args()
    DO = DatabaseOperations('_market_cap', kwargs_percentages)
    DO.update_row_in_percentages()

    connection.commit()
    connection.close()

    #
    #
    # try :
    #     if debug_level >= 2 : print('\nprepQuery : ', prepQuery)
    #     prepResult = cur.execute(prepQuery)
    #     if debug_level >= 2 : print('prepResult : ', prepResult)
    #     row = cur.fetchone()
    #
    #
    #
    #     if (prepResult > 0) :
    #
    #         prev_total_market_cap =  int(row[0])
    #         prev_total_24h_volume = int(row[1])
    #
    #         if debug_level >= 2 : print('  row = ',row, '    ',  prev_total_market_cap , prev_total_24h_volume    )
    #
    #         total_market_cap_Ch = round( (( total_market_cap_eur - prev_total_market_cap ) / prev_total_market_cap) * 100 , 2)
    #         total_24h_volume_Ch = round( (( total_24h_volume_eur - prev_total_24h_volume) / prev_total_24h_volume) * 100 , 2)
    #
    #         if debug_level >= 2 : print('  total_market_cap , prev_total_24h_volume : \t', total_24h_volume_eur ,'       ' ,prev_total_24h_volume )
    #
    #
    #         if debug_level >= 2 : print('epoch : ', epoch)
    #         if debug_level >= 1 : print( '  epoch != DATA[last_updated]    ' , epoch != DATA['last_updated'] ,'        epoch =   ' ,epoch , '    DATA[] =' , DATA['last_updated']  )
    #
    #         ##### We insert the data only if it is new ! We prevent to accumulate DOUBLE DATA !!!!
    #         if epoch != int(DATA['last_updated']) :
    #
    #             insert_query  = 'INSERT INTO `_market_cap` (date, market_cap_EUR, market_cap_USD, market_cap_ch, volume_24h, volume_24h_ch, BTC_dominance) ' \
    #                             'VALUES (%s, %s, %s, %s, %s, %s, %s);'
    #             if debug_level >= 2 : print('insert_query : \t', insert_query)
    #             insert_result = cur.execute( insert_query, (date, total_market_cap_eur, total_market_cap_usd, total_market_cap_Ch, total_24h_volume_eur, total_24h_volume_Ch, BTC_dominance) )



else :
    logger.warning(Colors.bg.yellow + 'Sorry, but past_epoch == current_epoch. NO NEWER DATA ! ' + Colors.disable )
    # initial_insert_query =  'INSERT INTO `_market_cap` (date, market_cap_EUR, market_cap_USD, volume_24h, BTC_dominance) VALUES (%s, %s, %s, %s, %s);'
    # if debug_level >= 2 : print('initial_insert_query : ', initial_insert_query )
    # initial_insert_result = cur.execute(initial_insert_query, ( date, total_market_cap_eur, total_market_cap_usd, total_24h_volume_eur, BTC_dominance ))
    # if debug_level >= 2 : print('initial_insert_result : \t', initial_insert_result)








#
#
# t4  = time.time()
# file_append_with_text(get_API_bittrex_data_log,  str(date)+  '       market INSERT in tables took    ' + str(round((t4-t3)*1000,2)) + ' ms' )


