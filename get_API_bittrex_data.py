#  Created by Bogdan Trif on 22-08-2018 , 10:36 AM.

import time, os
from platform import system
import operator
import pymysql
import requests  # to make GET request
from conf.sensitive import *
from includes.DB_functions import *
from includes.app_functions import *
from collections import OrderedDict
import logging.config

from os import path

t1  = time.time()


###     LOGGING     ###
config = init_logging( config_file= 'conf/logging.json' , log_type= 'json' )
logging.config.dictConfig(config['logging'])
logger = logging.getLogger('get_API_bittrex_data')



connection = pymysql.connect( host= localhost, port=3306, user=USER, passwd=PASSWD, db=bittrex )
cur = connection.cursor()
# print('connection :', connection ,'    cursor :', cur)
logger.debug('connection :' +str(connection) +'    cursor :' +str( cur) )


cur_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


URL = 'https://bittrex.com/api/v1.1/public/getmarketsummaries'
logger.debug('markets file : ' + str(bittrex_markets_file) )


# current_market = get_current_market()
# logger.debug('current market = ' + str(current_market) )



# file_append_with_text(API_bittrex_data_log,  str(cur_datetime)+  '       getExternalContent( URL  ) took    ' + str(round((t2-t1)*1000,2)) + ' ms' +' ,     LOAD : ' + str(os.getloadavg() ) )

###   Read valid Markets from file :
# valid_MARKETS = set( read_file_line_by_line(valid_markets_file) )








#### First we delete the _volumes table in order to update it after :
delete_old_volumes_data = 'DELETE FROM _volumes WHERE market = %s;'
delete_old_variations_data = 'DELETE FROM _variations WHERE market = %s;'


# #######    Preparation QUERIES        #############
# ## prep Query for getting last minute items and compare them with the current ones  ###
# prepQuery  = 'SELECT last_price, volume, buy_vs_sell, id FROM `market_name` ORDER BY ID DESC LIMIT 1;'
#
# ####  pre Query TO GET  the values with 60 minutes (1 hour) behind #####
# prep_VolumeQuery_1h  = 'SELECT last_price, volume, buy_vs_sell, id FROM `market_name` WHERE ID = %s;'
#
# ####    pre Query TO GET  if the market is already in _VOLUMES TABLE or not     #####
# prep_Query_Volumes_market  = 'SELECT market, status, time_in, volume FROM `_volumes` WHERE market = %s;'
#
# ####  pre Query TO GET  _HISTORY table values with 1 day offset behind #####
# prep_HistoryQuery  = 'SELECT market, time, vol_ch_1h, id  FROM `_history` WHERE time >= DATE_SUB(NOW(), INTERVAL 1 DAY) AND market = %s;'




# DA = DataAquisition( URL  )
# print('all Markets : ', len(DA.raw_markets), DA.raw_markets )
# DA.get_API_Content()


class CustomizedMarkets(object) :
    def __init__(self, markets_file ):
        # super(CustomizedMarkets, self).__init__()
        self.markets_file = markets_file
        self.defined_Markets = set(self.read_file_line_by_line(self.markets_file))


    def read_file_line_by_line( self , filename) :
        ###   Read valid Markets from file :
        ''':Description : Read a filename line by line and the put the elements found
        in the form of strings into a LIST '''
        L = []
        with open( filename, 'r') as f :
            for line in f :
                l = line.rstrip('\n')
                # print(l, type(l) )
                L.append(l)
        f.close()
        return L


# BVM = CustomizedMarkets( markets_file)
# print('BVM Valid Markets : ', len(BVM.defined_Markets), BVM.defined_Markets )
# DA = DataAquisition(URL)
# print('DA All Markets : ', len(DA.raw_markets), DA.raw_markets )

class DataPreparation(DataAquisition, CustomizedMarkets):
    ''':Description: Class which inherits the properties of other classes and aggregates data.
    Roles : 1.   take the data from Bittrex API and put in a dataset.
               2.   see which markets are defined and suitable;
                :Methods of the class:
                3.  detect when new markets are added;
                4.  select what items will be taken from the dataset
                5.  deliver a new dataset which contains only usable data.
    '''

    def __init__( self, URL, markets_file ) :
        # super(DataInsertion, self).__init__(URL )
        # super( DataInsertion, self ).__init__()
        DataAquisition.__init__(self, URL)
        CustomizedMarkets.__init__(self, markets_file)
        logger.info('self.defined_Markets : ' + str(len(self.defined_Markets)) + '  ' + str(self.defined_Markets))
        self.markets_dataset = self.collect_API_items()
        # logger.debug('markets_dataset : ' + str(self.markets_dataset))
        self.new_Markets = self.detect_new_markets()
        self.db_markets_dataset = {  k:v for k,v in self.markets_dataset.items() if k not in self.new_Markets }
        logger.info(Colors.fg.green + str(len(self.db_markets_dataset)) + str(self.db_markets_dataset) + Colors.reset )


    def detect_new_markets( self ):
        ''' :Description: check the existing tables in the bittrex DB and compares with the ones
            defined in the defined_Markets. If new ones are added,
            then the method creates the corresponding table to bittrex DB'''
        existing_tables_result = cur.execute( QUERIES['query_existing_tables'])     #, ('bittrex', 'BTC-ADAs') )

        if  len(self.defined_Markets) != existing_tables_result :
            logger.info('existing_tables_result = ' + str( existing_tables_result) + '    length defined_Markets=   ' + str(len(self.defined_Markets)))
            markets_in_db = { i[0].upper() for i in cur.fetchall() }
            logger.info('markets_in_db : ' + str(len(markets_in_db)) + str(markets_in_db)  )
            New_markets =  self.defined_Markets.difference(markets_in_db)
            if len(New_markets) > 0 :
                logger.warning('New_markets : ' + Colors.bg.yellow + str( New_markets ) + Colors.reset )
                New_markets_data = {  k:v for k,v in self.markets_dataset.items() if k in New_markets}
                logger.warning(Colors.bg.yellow + str(New_markets_data) + Colors.reset)
                return New_markets_data

        return []


    def select_API_bittrex_items(self, ITEM):
        market = ITEM["MarketName"]
        lastPrice = format(  ITEM["Last"] ,'.8f')
        volume = round( ITEM["BaseVolume"] , 4 )
        bid = format( ITEM["Bid"] , '.8f')
        ask = format(  ITEM["Ask"] , '.8f' )
        openBuyOrders = int(ITEM["OpenBuyOrders"])
        openSellOrders = int(ITEM["OpenSellOrders"])
        buyVsSell = round( openBuyOrders/openSellOrders, 4 )

        return market, lastPrice, volume, bid, ask, openBuyOrders, openSellOrders, buyVsSell


    def collect_API_items(self):
        DATASET = OrderedDict()
        defined_Markets_dataset = [market for market in self.raw_markets if market['MarketName'] in self.defined_Markets]
        logger.info('(collect_API_items ) defined_Markets_dataset : ' + str(len(defined_Markets_dataset)) + str(defined_Markets_dataset))
        for  cnt, ITEM in enumerate(defined_Markets_dataset) :
            cur_datetime = time.strftime("%Y-%m-%d %H:%M:%S",   time.localtime() )
            market, lastPrice, volume, bid, ask, openBuyOrders, openSellOrders, buyVsSell = self.select_API_bittrex_items(ITEM)
            DATASET[str(market)] = { 'last_price': lastPrice , 'volume': volume ,'bid': bid , 'ask': ask , 'buy_ord': openBuyOrders ,'sell_ord': openSellOrders , 'BvS': buyVsSell  }
            # print(str(cnt+1)+'.     market=', market ,'    lastPrice=', lastPrice , '    volume=', volume ,'    bid=', bid , '    ask=', ask , '    openBuyOrders=', openBuyOrders ,'    openSellOrders=', openSellOrders , '    buyVsSell=', buyVsSell  )
        # print('DATASET : ',DATASET  )
        return DATASET


class Computations():
    ''':Description: Class to make only needed computations for price_ch, volume_ch, buy_vs_sell_ch
    '''
    def __init__(self, Last_Values, Previous_Values):
        self.Last_Values = Last_Values
        self.Previous_Values = Previous_Values
        self.Names = ['Price_ch', 'Vol_ch', 'BvS_ch' ]
        self.Price_Vol_BvS_ch = self.get_values_changes()


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



class DatabaseCollection(DataPreparation) :
    ''':Description: Class to Collect OLD Items from a SINGLE TABLE. It needs current values obtained
        from DataPreparation Class in order to replace missing values from Db when getting the
            old values array of [ old_prices 5m, 15m, 30m ...,  ] ,[ old_volumes 5m, 15m, 30m ...,  ],[ old_buy_vs_sell 5m, 15m, 30m ...,  ]
    '''
    def __init__(self, table_name ):
        self.QB = QueryBuilder()
        self.table_name = table_name

        # super().__init__(URL, markets_file)

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
            # print('result : ', result)
            last_id = cur.fetchone()[0]
            # print('last_id : ', last_id )
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
        Prices, Vols, BvSs =[], [], []
        i=0
        for i in range(len(Times)) :
            if Times[i] <= self.max_index_diff :
                index = self.last_id - Times[i]+1
                logger.debug(str(i) +'    Time_index : ' + str(Times[i]) +  '     Period : '+ str(Periods[i]) + '   index = ' +str( index) )
                # 'SELECT id, last_price, volume, buy_vs_sell from `market_name` WHERE ID = %s;'
                Select_result = 0
                while Select_result != 1 :
                    try :
                        Sel_query = self.QB.select_query(self.table_name, 'WHERE ID='+str(index)  ,*['last_price', 'volume', 'BvS' , 'id' ] )
                        logger.debug('Sel_query : ' + str(Sel_query) )
                        Select_result = cur.execute(Sel_query)
                        logger.debug('Select_result : ' + str(type(Select_result)) +str( Select_result)  )
                        Vals = cur.fetchone()
                        logger.debug(str(Periods[i]) +' : old values : ' + str(Vals) )
                        Prices.append(Vals[0])
                        Vols.append(Vals[1])
                        BvSs.append(Vals[2])

                    except :
                        # print('\n' + Colors.fg.purple+' SORRY ! No id with this value = ' +str(index) + Colors.reset )
                        index +=1
                        logger.debug('Select_result = ' + str(Select_result) + '     We try index =' + Colors.fg.purple+ str(index) + Colors.reset)


        logger.debug('Prices : ' + str( Prices) )
        logger.debug('Vols : '+ str(Vols ) )
        logger.debug('BvSs : '+ str(BvSs ) )

        return Prices, Vols, BvSs

    def get_custom_values_behind(self, period_behind ):
        ''':Description: finds the old values corresponding to period behind. E.g. find the values of 5m, 15m, 30m, 1h, 4h, 8h, 1d, ...
        :param period_behind: string, of the form 5m, 15m, 30m, 1h, 4h, 8h, 1d, ...
        :return: tuple (Price, Vol, BvS),  of its corresponding index if it is found                               '''
        try :
            index_behind = Periods.index(period_behind)
            # print('index_behind = ', index_behind)
            return self.Old_Values[0][index_behind], self.Old_Values[1][index_behind], self.Old_Values[2][index_behind]
        except ValueError :
            logger.warning(Colors.bg.yellow + 'The period ' + period_behind +' is not among ' + str(Periods) +' !!!. Try to use one of these !' +Colors.reset  )
            return -10, -10, -10        # return to avoid errors




class DatabaseOperations( ):
    ''':Description: Class to do INSERT, UPDATE Operations on a SINGLE TABLE   '''
    def __init__(self, market, kwargs ):
        self.QB = QueryBuilder()
        # print('self.DC = ', len(self.DC.MARKETS), self.DC.MARKETS )
        self.table_name = market
        self.kwargs = kwargs
        logger.debug('DatabaseOperations table_name  & **kwargs : ' +str( self.table_name) + '  ' +str(self.kwargs ) )


    def create_market_table( self ):
        createTableResult = cur.execute( TABLES['create_market_name'].replace('table_name', self.table_name ) )
        logger.warning(Colors.fg.cyan + 'createTableResult : '+ str(createTableResult) )
        logger.warning(Colors.fg.cyan+ 'table ' +Colors.bg.red + str(self.table_name) + Colors.fg.cyan + ' has been created' + Colors.reset  )
        QueryBuilder.insert_query(self.table_name )


    def insert( self, commit ):
        query_insert = self.QB.insert_query(self.table_name, **self.kwargs  )
        try :
            insert_res = cur.execute(query_insert)
            logger.debug(Colors.bg.green + 'insert_result : ' + str( insert_res ) + Colors.reset )
            if commit == True : connection.commit()

        except :
            logger.error(Colors.bg.red + 'Sorry ! Insertion is not possible for ' + str(self.table_name) +'   with **kwargs : ' +str( self.kwargs) +Colors.reset )
            connection.rollback()

    def update_row_in_evolution(self):
        try :
            update_query = self.QB.update_query('_evolution' , 'market' , self.table_name, **self.kwargs )
            logger.debug('update_evolution_query : ', update_query )
            update_res = cur.execute(update_query)
            # print('update__evolution_res  : ' + str(update_res))

            if update_res ==0 :
                insert_query = self.QB.insert_query('_evolution', **{'market': self.table_name, 'time' : cur_datetime } )
                logger.debug('insert evolution query : ' +  str(insert_query))
                ins_res = cur.execute(insert_query)
                # print('insert evolution result : ' +  str(ins_res))

        except  :
            logger.error('There was an error at update__evolution_res !')


    def update_row_in_volumes(self):
        try :
            update_volumes_query = self.QB.update_query('_volumes' , 'market' , self.table_name, **self.kwargs )
            logger.debug('update_volumes_query : ', update_volumes_query )
            update_vol_res = cur.execute(update_volumes_query)
            # print('update_volumes_res  : ' + str(update_vol_res))
            if update_vol_res ==0 :
                insert_vol_query = self.QB.insert_query('_volumes', **{'market': self.table_name, 'time' : cur_datetime } )
                logger.debug('insert _volumes query : ' +  str(insert_vol_query))
                ins_vol_res = cur.execute(insert_vol_query)
                # print('insert _volumes result : ' +  str(ins_vol_res))

        except  :
            logger.error('There was an error at update_volumes_res !')





class BittrexDatabaseOps( ):
    ''':Description: Class used only for bittrex database specific operations.
        It aggregates and prepares all the parameters in a single dictionary of **kwargs    '''
    def __init__(self, table_name, cur_vals, other_vals ):
        self.table_name = self.market_name = table_name
        self.cur_vals = cur_vals
        self.percentages = self.other_vals =  other_vals
        # self.kwargs_btc_ada = self.assemble_main_markets_args()
        # self.kwargs_evolution = self.assemble_evolution_markets_args()


    def assemble_main_markets_args(self):
        names = ['price_ch', 'vol_ch', 'BvS_ch' ]
        changes =  { names[i] : self.percentages[i][0] for i in range(len(self.percentages ))  }
        # print('cur_vals : ', self.cur_vals  )
        # print('changes  : ', changes )

        _kwargs = { **self.cur_vals, **changes, **{'date' : cur_datetime}  }
        # print('kwargs : ', _kwargs )
        return _kwargs
        # DO = DatabaseOperations(self.table_name, **_kwargs)
        # print('DO :', DO)
        # DO.insert( True )

    def assemble_evolution_markets_args(self):
        # print('assemble_evolution_markets_args  :   cur _vals = ' + str( self.cur_vals)  )

        PR , VOL , BVS = self.other_vals[0], self.other_vals[1], self.other_vals[2]
        pr_0, vol_0, BvS_0 = self.cur_vals['last_price'], self.cur_vals['volume'], self.cur_vals['BvS']
        # print(' pr_0, vol_0, BvS_0 :  ', pr_0, vol_0, BvS_0)

        # we insert the current values at the beginning of the list
        PR.insert(0, pr_0) ; VOL.insert(0, vol_0 ) ; BVS.insert(0, BvS_0 )
        # print('PR , VOL , BVS :  ' + str(PR) + str(  VOL) + str(BVS)   )

        # take only as needed name keys as needed
        Price_per = Price_periods[:len(PR)]
        Vol_per = Vol_periods[:len(VOL)]
        BvS_per = BvS_periods[:len(BVS)]
        # print(' Price_periods , Vol_periods, BvS_periods : ', Price_per, Vol_per, BvS_per )

        # make a dictionary of key:values. Like {'pr_0' : 0.02, 'pr_5m: 0.4, ... } ; {'vol_0' : 62.23, 'vol_5m: 64.45, ... }, ...
        Prices = { Price_per[i] : PR[i] for i in range(len(PR))  }
        Volumes = { Vol_per[i] : VOL[i] for i in range(len(VOL))  }
        BvSs = { BvS_per[i] : BVS[i] for i in range(len(BVS))  }
        __kwargs = { **Prices, **Volumes, **BvSs, **{'time' : cur_datetime} }
        logger.debug('assemble_evolution_markets_args __kwargs : ' + str( __kwargs) )
        return __kwargs


    def assemble_volumes_markets_args(self):
        logger.info('All_Percentages : ' + str(  self.percentages)  )
        if len(self.percentages[0]) >= 4:  percentages_1h = { 'pr_ch_1h' : self.percentages[0][3], 'vol_ch_1h' : self.percentages[1][3], 'BvS_ch_1h' : self.percentages[2][3] }
        else :  percentages_1h = { 'pr_ch_1h' : -10, 'vol_ch_1h' : -10, 'BvS_ch_1h' : -10 }
        percentages_5m = { 'pr_ch_5m' : self.percentages[0][0], 'vol_ch_5m' : self.percentages[1][0], 'BvS_ch_5m' : self.percentages[2][0] }
        current_vals = { 'last_price' : self.cur_vals['last_price'] ,'volume' : self.cur_vals['volume'] ,'BvS' : self.cur_vals['BvS'] }
        _kwargs_volumes = { **percentages_1h, **percentages_5m, **current_vals , **{'time' : cur_datetime}}
        logger.debug('_kwargs_volumes : ' +str( _kwargs_volumes)  )
        return _kwargs_volumes



t2  = time.time()
if system() == 'Linux' :
    logger.warning(' getExternalContent( URL  ) took    ' + str(round((t2-t1)*1000,2)) + ' ms' + ' ,     LOAD : ' + str(os.getloadavg() ) )

if __name__ == '__main__' :
    ###     1.      Take the Data from Bittrex API, the chosen markets BTC-ADA
    DP = DataPreparation(URL, bittrex_markets_file)

    # logger.debug('DP.MARKETS dataset : ' + str(DP.markets_dataset) )

    cnt =0
    for market, Vals in DP.db_markets_dataset.items() :
        cnt+=1
        logger.info('---'*20 )
        logger.warning(Colors.bg.iceberg +'#'+str(cnt) +'    market : ' + str(market) + Colors.fg.blue + '     Vals : '+ str(Vals) + Colors.disable )

        ###     BTC-ADA     MARKETS / Individual    TABLES      ###
        #     2.   Prepare the data for insertion in the main market -->
        # the 5m values from API bittrex, Price_Vol_BvS changes & real_volume

        DC = DatabaseCollection(market)
        Values_5m = DC.get_custom_values_behind('5m')
        logger.debug('Values_5m  : ' + str(Values_5m) )

        ###     Get Old Values for Price, Volume, buy_vs_sell @ 5m, 15m, 30m, 1h, 4h, 8h, 1d, ....
        All_Old_Values = DC.Old_Values
        logger.debug(Colors.fg.green + 'Pr_Vol_BvS_Old_Values  : ' + str(All_Old_Values) + Colors.disable )

        ###  Last Values taken from the BITTREX API
        Current_Vals = [Vals['last_price'], Vals['volume'], Vals['BvS']]
        logger.info(Colors.fg.cyan + 'Current_Vals  : ' + str(Current_Vals) + Colors.disable)

        ###     2a.     Compute the percent variations for Price, Volume & Buy_vs_Sell
        All_Percentages = Computations(Current_Vals, All_Old_Values).Price_Vol_BvS_ch
        # print('CALC All_Percentages : ', All_Percentages )

        ###     REAL VOLUME     ###
        real_volume = get_market_real_volume(market, 5 )

        ###     2b.     Gather All the **kwargs to INSERT into the BTC-ADA markets
        kwargs_btc_ada = BittrexDatabaseOps(market, Vals, All_Percentages).assemble_main_markets_args()
        kwargs_btc_ada.update(real_volume)      #   Update kwargs_btc_ada with real_volume dictionary
        logger.info('main_market_args : ' +str( kwargs_btc_ada) )

        ###     2c.     Markets 'BTC-ADA' INSERTS           ###
        DO = DatabaseOperations(market, kwargs_btc_ada)
        DO.insert( False )

        ###     _EVOLUTION TABLE        ###

        kwargs_evolution = BittrexDatabaseOps(market, Vals, All_Old_Values ).assemble_evolution_markets_args()
        DO = DatabaseOperations(market, kwargs_evolution)
        DO.update_row_in_evolution()

        ###     _VOLUMES TABLE          ###

        kwargs_volumes = BittrexDatabaseOps(market, Vals, All_Percentages).assemble_volumes_markets_args()
        kwargs_volumes.update(real_volume)
        DO = DatabaseOperations(market, kwargs_volumes)
        DO.update_row_in_volumes()



    ###############################
    ###      3.      New Markets Case       ###
    ###############################
    if len(DP.new_Markets) > 0 :
        for market, Vals in DP.new_Markets.items() :
            cnt+=1
            logger.info('---'*20 )
            logger.info(Colors.bg.iceberg +'#'+str(cnt) +'    market : ' + str(market) + Colors.fg.blue + '     Vals : '+ str(Vals) + Colors.disable )

            main_market_args = { **Vals, **{'date' : cur_datetime} }
            logger.info('main_market_args : ' +str(main_market_args) )
            DO = DatabaseOperations(market, main_market_args)
            DO.create_market_table()

            DO.insert(True)



    connection.commit()
    connection.close()

t3  = time.time()
if system() == 'Linux' :
    logger.warning('    market INSERT in tables took    ' + str(round((t3-t2),2)) + ' s' + ' ,     LOAD : ' + str(os.getloadavg() )  )
