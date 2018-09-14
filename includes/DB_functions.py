#  Created by Bogdan Trif on 13-01-2018 , 10:11 PM.

TABLES, QUERIES = {}, {}


TABLES['create_market_name'] = (
    'CREATE TABLE IF NOT EXISTS `table_name` ('
      '`id` int(11) NOT NULL AUTO_INCREMENT,'
      '`date` datetime DEFAULT NULL,'
      '`last_price` decimal(16,8) DEFAULT NULL,'
      '`price_ch` decimal(7,2) DEFAULT NULL,'
      '`volume` decimal(20,4) DEFAULT NULL,'
      '`vol_ch` decimal(7,2) DEFAULT NULL,'
      '`BvS` decimal(7,4) DEFAULT NULL,'
      '`BvS_ch` decimal(7,2) DEFAULT NULL,'
      '`bid` decimal(16,8) DEFAULT NULL,'
      '`ask` decimal(16,8) DEFAULT NULL,'
      '`buy_ord` int(10) DEFAULT NULL,'
      '`sell_ord` int(10) DEFAULT NULL,'
      '`vol_buy` decimal(11,4) DEFAULT NULL,'
      '`vol_sell` decimal(11,4) DEFAULT NULL,'
      'PRIMARY KEY (`id`),'
      'KEY `date` (`date`)'
       ') ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;')


TABLES['create_coin_market_cap_table'] = (
    'CREATE TABLE IF NOT EXISTS `table_name` ('
    '`id` int(11) NOT NULL AUTO_INCREMENT,'
    '`date` datetime DEFAULT NULL,'    
    '`price_USD` decimal(11,4) DEFAULT NULL,'    
    '`price_EUR` decimal(11,4) DEFAULT NULL,'    
    '`EUR_ch` decimal(7,2) DEFAULT NULL,'    
    '`price_BTC` decimal(11,8) DEFAULT NULL,'    
    '`BTC_ch` decimal(7,2) DEFAULT NULL,'    
    '`vol_24h_EUR` bigint(14) DEFAULT NULL,'    
    '`vol_24h_ch` decimal(7,2) DEFAULT NULL,'    
    '`market_cap_EUR` bigint(14) DEFAULT NULL,'    
    '`market_cap_ch` decimal(7,2) DEFAULT NULL,'
    '`price_ch_1h` decimal(7,2) DEFAULT NULL,'    
    '`price_ch_24h` decimal(7,2) DEFAULT NULL,'    
    '`price_ch_7d` decimal(7,2) DEFAULT NULL,'
    '`available_supply` bigint(14) DEFAULT NULL,'
    '`rank` smallint(3) DEFAULT NULL,'    
    'PRIMARY KEY (`id`)'    
    ') ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;')



QUERIES['check_if_table_exist'] =  'SHOW TABLES LIKE "table_name" ; '
QUERIES['show_tables'] =  'SHOW TABLES; '

QUERIES['query_existing_tables'] = (
    "SELECT TABLE_NAME FROM information_schema.TABLES WHERE "
    "(TABLE_SCHEMA = 'bittrex') AND (TABLE_NAME LIKE '%-%');")


class QueryBuilder():
    @classmethod
    def update_query(cls, table_name, where_field, where_arg , **kwargs ):
        ''':Description: function which updates a table name
        :param table_name: str, table_name
        :param kwargs: keys and values in the form : BTC_balance=BTC_balance, time_update=t1, status='SELL_PROCESS'
            or as a dictionary
        :OBSERVATION: last of the **kwargs must be the WHERE condition argument.
            Example : func_call : update_table('BTC-ADA' , 'id', 1234 ,**{volume :100  })  translates to :
            UPDATE `BTC-ADA` SET volume=100 WHERE id=1234;
        '''

        update_query = "UPDATE `" + str(table_name) +"` SET "
        for field, value in kwargs.items():
    #         print(field , " =", value)
            if type(value) == str :
                value = "'" + str(value) + "'"
            update_query += field +'='+str(value)+", "

        update_query = update_query.rstrip(' ,')
        update_query += " WHERE "+ str(where_field)+"='" + str(where_arg)+"';"
#         print('update_query: ', update_query)

        return update_query

    @classmethod
    def insert_query(cls, table_name,  **kwargs ):
        ''':Description: function which insert into a table name
            :param table_name: str, table_name
            :param kwargs: keys and values in the form : of a dictionary or as time_update=t1, status='SELL_PROCESS'
                '''
        insert_query = "INSERT INTO `" + str(table_name) +"` "
        K, V = '('  , "( "
        for field, value in kwargs.items():
    #         print(field , " =", value)
            K+= field+', '
            if type(value) == str :
                V += "'" + str(value) + "', "
            else :
                V += str(value) +', '

        K, V = K.rstrip(' ,') , V.rstrip(' ,')
        K += ') VALUES '
        V += ');'
        # print('keys :', K, '   ')
    #     print('values : ', V)

        insert_query += K + V
        # print('insert_query: ', insert_query)

        return insert_query


    @classmethod
    def select_query(cls, table_name, clause, *args ):
        ''':Description: function which updates a table name
           :param kwargs: what fields to select     '''
        select_query = "SELECT "
        for field in args:
    #         print(field , " =", value)
            select_query += field +", "

        select_query = select_query.rstrip(' ,')

        select_query += " FROM `" +str(table_name) +"` "
        select_query += str(clause)+";"
#         print('select_query: ', select_query)

        return select_query

    @classmethod
    def table_check(cls, database, table_name):
        ''':Description: checks the existance of a table
                WAYS to do it :
        1.      322ms:       show tables like 'BTC-ADA';
        2.      691ms:      select 1 from BTC-ADA limit 1;
        3.      319ms:      SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'bittrex') AND
                                (TABLE_NAME = 'BTC-ADA');
        '''
         # 'SELECT count(*) FROM information_schema.tables WHERE table_schema = %s  AND table_name = %s LIMIT 1;'
        check_query = "SELECT count(*) FROM information_schema.tables WHERE table_schema = '"
        check_query += str(database)+"' AND TABLE_NAME = '" + str(table_name) +"';"
        print('çheck_query :', check_query )
        return check_query





QUERIES['initial_insert_query'] = (
        'INSERT INTO `table_name` ('
        'date, last_price, buy_vs_sell, volume, bid, ask, buy_orders, sell_orders'
        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s);' )


QUERIES['second_insert_query'] = (
    'INSERT INTO `table_name` ('
    'date, last_price, last_price_ch, buy_vs_sell, buy_vs_sell_ch, volume, vol_ch,'
    'bid, ask, buy_orders, sell_orders'
    ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' )

######     _VOLUMES TABLE       ##########

QUERIES['insert_volumes'] = (
    'INSERT INTO `_volumes` ('
    'market, time_in, last_update, vol_ch_1h, vol_ch_1m, price_ch_1h, price_ch_1m, BvS_ch_1h, BvS_ch_1m,'
    'volume, vol_buy, vol_sell, last_price, buy_vs_sell, status'
    ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' )

QUERIES['update_volumes'] = (
    'UPDATE `_volumes` SET '
    'last_update=%s, vol_ch_1h=%s, vol_ch_1m=%s, price_ch_1h=%s, price_ch_1m=%s, BvS_ch_1h=%s, BvS_ch_1m=%s, '
    'volume=%s, vol_buy=%s, vol_sell=%s, last_price=%s, buy_vs_sell=%s, status=%s '
    'WHERE market=%s;' )



######     _HISTORY TABLE

QUERIES['history_insert'] = (
    'INSERT INTO `_history` ('
    'time, market, vol_ch_1h, vol_ch_1m, price_ch_1h, price_ch_1m, BvS_ch_1h, BvS_ch_1m,'
    'volume, last_price, buy_vs_sell, type'
    ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' )


#####   _EVOLUTION TABLE

QUERIES['evolution_initial_insert'] = (
    'INSERT INTO `_evolution` ('
    'market, time'    
    ') VALUES (%s, %s);')

QUERIES['first_market_row'] = (
    'select id from `market_name` ORDER BY ID LIMIT 1;'
)
QUERIES['last_market_row'] = (
    'select id from `market_name` ORDER BY ID DESC LIMIT 1;'
)

QUERIES['custom_price_and_buy_vs_sell_market'] = (
    'SELECT id, last_price, volume, buy_vs_sell from `market_name` WHERE ID = %s;'
)

###### COIN MARKET CAP TABLE

QUERIES['initial_insert_market_cap'] = (
        'INSERT INTO `table_name` ('
        'date, price_USD, price_EUR, price_BTC, vol_24h_EUR, market_cap_EUR, price_ch_1h, price_ch_24h, price_ch_7d, available_supply, rank'
        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' )

QUERIES['second_insert_market_cap'] = (
        'INSERT INTO `table_name` ('
        'date, price_USD, price_EUR, EUR_ch, price_BTC, BTC_ch, vol_24h_EUR, vol_24h_ch, market_cap_EUR, market_cap_ch,'
        'price_ch_1h, price_ch_24h, price_ch_7d, available_supply, rank'
        ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);' )

#### _TRANZACTIONS TABLE


QUERIES['tranzactions_buy_order'] = (
        'INSERT INTO `_tranzactions` ('
        'market, BUY_time, price_IN, BTC_IN, quantity'
        ') VALUES (%s, %s, %s, %s, %s);' )

QUERIES['tranzactions_sell_order'] = (
		'UPDATE _tranzactions SET '
        'SELL_time=%s, price_OUT=%s, BTC_OUT=%s, ROI=%s '
        'WHERE id=%s;' )

### _TRADE_NOW TABLE        #####

QUERIES['trade_now_table_insert'] = (
                'INSERT INTO `_trade_now` '
                '(market, time_in, time_update, status, coin_balance, BTC_balance, order_type, time_order, uuid) ' 
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);' )


QUERIES['trade_now_table_update'] = (
                'UPDATE `_trade_now` SET '
                'time_update=%s, status=%s, coin_balance=%s, BTC_balance=%s, order_type=%s, time_order=%s, uuid=%s ' 
                'WHERE market=%s;' )


####        _VARIATIONS TABLE       ####
QUERIES['insert_variations'] = (
        'INSERT INTO `_variations` '
        '(market, time_in, BvS_1m, pr_1m) '
        'VALUES (%s, %s, %s, %s);' )



