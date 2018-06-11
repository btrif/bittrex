#  Created by Bogdan Trif on 22-05-2018 , 6:07 PM.


from includes.DB_functions import *
from includes.app_functions import *


### Initiate DB Connections :
connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB1 )
cur = connection.cursor()


#############     get CLOSED & OPENED      ORDERS      #################

def get_Closed_Orders(market_name, order_type, offset ) :
    ''' :Description: Gets the latest COMPLETED ORDERS NEWER than the offset specified time :
    :Steps: Interrogates the BITTREX API about CLOSED (Completed) Orders
    :Remark: It also takes into account the commission 0.25% of the market to return CORRECT BTC spendings
    :param market_name: active trading market
    :param order_type: string, BUY or SELL
    :param offset: int, in HOURS, how many hours behind
    :return: lst, ARRAY of tuples                       '''

    ARR = []        # Array to hold multiple closed orders in form of tuples
    gCO = API.getorderhistory(market_name, offset )
    for I in gCO :
        closed_time, quantity , quantity_remaining = convert_time_UTC_bittrex_format_to_local( I['Closed']) ,  I['Quantity'],  I['QuantityRemaining']
        actual_rate, BTC_sum, tip, commission = I['PricePerUnit'], I['Price'], I['OrderType'], I['Commission']
        if convert_time_standard_format_to_epoch(closed_time) > time.time() - offset*3600 :    # Newer than an offset hours :
            if tip.__contains__(order_type) :
                if tip.__contains__('BUY') :         BTC_sum += commission
                if tip.__contains__('SELL') :         BTC_sum -= commission

                ARR.append( ( market_name, closed_time, actual_rate, round(BTC_sum, 8) , round( quantity-quantity_remaining, 8 ), commission ,tip ) )
                print('closed_time :', closed_time ,'    actual_rate =', actual_rate, '    BTC_sum =', round(BTC_sum, 8), '   quantity=', quantity, '    Commission =', commission, '   order_type =', tip )
    print(str(market_name)+' :  <'+str(offset)+' hours , '  + str(order_type)+'  Closed Orders : ', ARR )
    # print('get_Closed_Orders ARR : ', ARR)
    return ARR



def get_Opened_Orders( market_name, order_type, offset ) :
    ''' :Description: Gets the latest OPENED ORDERS NEWER than the offset specified time :
        :Steps: Interrogates the BITTREX API about  OPENED Orders
        :param current_market: active trading market
        :param order_type: string, BUY or SELL
        :param offset: int, in HOURS, how many hours behind
        :return: lst, ARRAY of tuples                       '''

    gOO = API.getopenorders(market_name)
    ARR = []
    if len(gOO) > 0  :
        # print('\nWe already have some ORDERS. We must check if they are OLD or NEW ')
        print('Orders :', gOO )
        for order in gOO :
            opened_time, OrderType = convert_time_bittrex_format_to_epoch( order['Opened'] ), order['OrderType']
            uuid, price_offer, coin_quantity = order['OrderUuid'], order['Limit'], order['Quantity']
            if OrderType.__contains__(order_type) :             # we filter the Order Type
                ### If the ORDER is OLD this means we actually have no recent ORDER OPENED
                if  opened_time > convert_utc_time_to_epoch() - 3600 * offset   :
                    print('price_offer= ', price_offer, '     uuid : ', uuid, '      opened_time : ', opened_time, '      ', convert_epoch_to_standard_UTC(opened_time), '    OrderType :', OrderType)
                    ARR.append((market_name ,convert_epoch_to_standard_local_time(opened_time), price_offer, coin_quantity, uuid ) )
    print(str(market_name)+' :  <'+str(offset)+' hours ,  '  + str(order_type)+' Opened_Orders : ', ARR )
    print('get_Opened_Orders ARR : ', ARR)
    return ARR


###########     _tranzactions_table     OPERATIONS          ############


def insert_into_tranzactions_table(current_market, offset, connection, cur ):
    ''':Description: INSERT into the _tranzaction table     '''
    compute_price = compute_median_price(current_market, 'BUY', offset)
    if compute_price != None :
        market, closed_time, Median_Price, BTC_Total, Quant_Total = compute_median_price(current_market, 'BUY', offset)

    ### Insert new entry into the _tranzactions table in DB, THE Computed RESULT :
    ## Here we take the median price, we INSERT only the MEDIAN TRANZACTION :
    # INSERT into the _tranzactions TABLE
    #     print('cursor = ', cur)
        buy_insert = cur.execute( QUERIES['tranzactions_buy_order'] , ( current_market, closed_time, format(Median_Price, '.8f'), BTC_Total, Quant_Total )  )
        connection.commit()

def update_tranzations_table(current_market, offset ) :
    ''':Description: UPDATE the _tranzaction table
     offset - how many hours behind to search for the COMPLETED SELL ORDERS'''
    ### First pre-SELECT the BUY Data from _tranzactions table :
    pre_select_tranz = 'SELECT id, market, price_IN, BTC_IN, quantity from _tranzactions WHERE market=%s ORDER by id DESC LIMIT 1;'

    select_tranz_Result = cur.execute(pre_select_tranz, (current_market) )
    print('select_tranz_Result : ', select_tranz_Result)
    if select_tranz_Result > 0 :
        row = cur.fetchone()
        last_ID, BTC_IN = row[0], float(row[3])
        print(' last_ID = ', last_ID, '    , BTC_IN = ',BTC_IN)

        ### Get the Completed SELL Orders from the BITTREX API
        market, closed_time, Median_Price, BTC_OUT, Quant_Total = compute_median_price(current_market, 'SELL', offset )
        print('market =  ', market, '   , closed_time=',closed_time, ' , Median_Price = ',Median_Price, ', BTC_OUT = ',BTC_OUT, '  , Quant_Total= ' ,Quant_Total  )

        ROI = round( ( float(BTC_OUT) - BTC_IN)*100/BTC_IN , 2 )

        sell_update = cur.execute(QUERIES['tranzactions_sell_order'] , (closed_time, format(Median_Price, '.8f'), BTC_OUT, ROI, last_ID ) )
        print('sell_update : ',  sell_update )

        connection.commit()


def compute_median_price(current_market, order_type, offset) :
    ''':Description: Gets all the orders, BUY or SELL and computes the median price.
    Depends on the function get_Closed_Orders

    :param current_market:
    :param order_type: SELL or BUY
    :param offset: int, how many hours behind
    :return: tuple                                              '''
    ARR = get_Closed_Orders(current_market, order_type, offset )
    if len(ARR) > 0:
        Quant_Total, BTC_Total = 0, 0
        for Order in ARR :
            closed_time, actual_rate, BTC_sum, quantity, commission ,tip = Order[1], Order[2], Order[3], Order[4], Order[5], Order[6]
            Quant_Total += quantity
            BTC_Total += BTC_sum
            print('closed_time :', closed_time , '   quantity=', quantity, '    actual_rate =', actual_rate, '    BTC_sum =', BTC_sum,'    Commission =', commission,'    tip =', tip )

        # print('Quant_Total : ', Quant_Total, '    BTC_Total : ', BTC_Total )
        Median_Price = BTC_Total / Quant_Total
        # BTC_Total = round(BTC_Total *(1-0.0025), 8 )
        if order_type.__contains__('BUY') :   Median_Price -= 0.0025 *Median_Price
        if order_type.__contains__('SELL') :   Median_Price += 0.0025 *Median_Price
        print('\nQuant_Total = ', Quant_Total ,'       BTC_Total = ', BTC_Total, '     Median_Price =', round(Median_Price,8) )

        return ( current_market, closed_time, round(Median_Price,8) , BTC_Total, Quant_Total )



def get_market_Balance(current_market, balance_type ) :
    ''':Description: Because of Bittrex Way to return previously unused markets to NONE, I am forced to write this function
    :param: balance_type : Balance, Available                                    '''
    currency = current_market.split('-')[1]
    currency_Balance = API.getbalance(currency)[balance_type]
    # print(str(balance_type)+' ' + str(currency) + ' Balance : ', currency_Balance)
    if currency_Balance == None :         return 0
    else :     return currency_Balance


############            DB OPERATIONS       ##############

def update_table(table_name, market_name, commit ,**kwargs ):
    ''':Description: function which updates a table name
    :param table_name: str, table_name
    :param market_name: str, market_name
    :param kwargs: keys and values in the form : BTC_balance=BTC_balance, time_update=t1, status='SELL_PROCESS'       '''
    update_query = "UPDATE `" + str(table_name) +"` SET "
    for field, value in kwargs.items():
        # print(field , " =", value)
        if type(value) == str :
            value = "'" + str(value) + "'"
        update_query += field +'='+str(value)+", "

    update_query = update_query.rstrip(' ,')
    # print(update_query)
    update_query += " WHERE market='" + str(market_name)+"';"
    print('update_query: ', update_query)

    update_result = cur.execute( update_query  )
    print('update_result :', update_result)
    if commit == True :   connection.commit()
    return update_result

def insert_into_table(table_name, market_name, commit , **kwargs ):
    ''':Description: function which insert into a table name
    :param table_name: str, table_name
    :param market_name: str, market_name
    :param kwargs: keys and values in the form : BTC_balance=BTC_balance, time_update=t1, status='SELL_PROCESS'       '''
    insert_query = "INSERT INTO `" + str(table_name) +"` "
    K, V = '(market, '  , "('" + str(market_name) + "', "
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
#     print('keys :', K)
#     print('values : ', V)

    insert_query += K + V
    print('insert_query: ', insert_query)

    insert_result = cur.execute( insert_query  )
    print('insert_result :', insert_result)
    if commit == True :     connection.commit()
    return insert_result


#######         BUY & SELL  REQUESTS ( PROCESSING , Process )     ############


def initiate_buy(bid, ask, BTC_balance, current_market ):
    ''' Make the request to buy . Depends upon the get_buy_price function
        :Returns: nothing, the scope is to buy                          '''
    my_offer_buy_price = get_buy_price(bid, ask)
    BTC_balance = float(BTC_balance)
    # print( ' my_offer_buy_price: ', my_offer_buy_price , type(my_offer_buy_price) )
    # print( ' BTC_balance: ', BTC_balance , type(BTC_balance) )
    quan_tity = (BTC_balance / my_offer_buy_price )
    quantity = (quan_tity - quan_tity*0.25/100) // 0.0001 /10000
    print('current_market = ' ,current_market,  '\nbid  = ' , bid , 'ask  = ' , ask ,  ' \tquantity =  ', quantity, '     my_offer_buy_price', my_offer_buy_price )
    buy_order = API.buylimit( current_market, quantity, my_offer_buy_price )
    print(' buy_order ---> ', buy_order)

def initiate_sell( bid, ask, current_market, market_Balance ,policy_type ):
    ''' :Description: makes a SELL REQUEST.
        :Steps : - First seeks the LAST BUY completed orders NEWER than 24 hours.
                    - If there are many orders computes a MEDIAN of the buy price
    :param bid:
    :param ask:
    :param current_market:
    :param policy:                              '''

    ### Get the latest COMPLETED ORDERS and get the MEDIAN Price :
    # We want the Completed BUY orders no older than 12 hours
    market, closed_time, Median_Price, BTC_IN, Quant_Total = compute_median_price(current_market, 'BUY', 12 )
    ### If we establish that We sell on percent, then compute the SELLING price using the PERCENT INCREASE
    if type(policy_type) == float :
        SELL_price = round( Median_Price * ( 1+ policy_type / 100 ), 8 )

        print('market :', market , 'SELL Price = ', SELL_price, '    BTC = ', BTC_IN, '     Quant_Total = ', Quant_Total )

        sell_order = API.selllimit( current_market, market_Balance, SELL_price )
        if 'uuid' in sell_order:
            print('sell_order ---> ', sell_order)
            return '   SELL REQUEST made at the price  = ' + str(SELL_price) +'   uuid : ' + sell_order['uuid']
        else :
            print('sell_order ---> ', sell_order)
            return str(sell_order) + ' ;    market Balance = ' +str(market_Balance)

    # 2018-03-27, 19:10  , This will be completed AFTERWARDS as it requires MORE COMPLEX ALGORITHMS
    # if policy_type == 'DYNAMIC' :





#####    ESTABLISH   SELL & BUY      PRICES     ######


def get_buy_price(bid, ask):
    ''':Description: Function to get a nice price from the market.
    Example : If the ask is 0.03462700 and the bid is 0.03450000 the function will establish a sum in between
    :param bid: float, the bid price,
    :param ask: float, the ask price
    :return: float
    '''
#     print(round(ask-bid,8))
    return round(bid +(ask - bid)/10, 8)

def get_sell_price(bid, ask):
    ''':Description: Function to get the SELL  price from the market. '''
#     print(round(ask-bid,8))
    return round(ask -(ask - bid)/4, 8)


#########           Get Total Book Order in BTC     ######### Not used for now

def get_order_book_total_BTC( market, percent_limit ) :

    SELL, BUY = 0, 0
    API = BITTREX(api_key, api_secret)
    ticker = API.getticker(market)
    Last = ticker['Last']
    # print('Last = ', Last, type(Last) )
    orderbook = API.getorderbook(market , 'both')
    # print(' len(test_order_book) = ', len(orderbook ) ,'\norder_book : ', orderbook  )


    for K in orderbook['buy'] :
        rate, quantity = K['Rate'], K['Quantity']
        if rate >= Last*( 1 - percent_limit/100 ) :
            BUY += rate * quantity
            # print(K, '    buy rate = ',  rate, '     quantity = ',  quantity , '     Last - percent = ', Last*( 1-percent_limit/100)  )

    for Q in orderbook['sell'] :
        rate, quantity = Q['Rate'], Q['Quantity']
        if rate <= (Last*( 1+percent_limit/100) ) :
            # print(K, '     sell rate = ',  rate, '     quantity = ',  quantity , '     Last + percent = ', Last*( 1+percent_limit/100)  )
            SELL += rate * quantity
    print('\nmarket : ', market)
    print('Last = ', format(Last,'.8f' ) ,'\nLow ' +str(percent_limit)+' % range = ', format(Last*( 1-percent_limit/100),'.8f' ), '    High ' +str(percent_limit)+' % range = ', format(Last*( 1+percent_limit/100),'.8f' )   )
    print('Order Book at ' + str(percent_limit) , '%  :\nTOTAL  BUY = ', round(BUY, 2) ,' BTC' , '\nTOTAL  SELL = ', round(SELL, 2) ,  ' BTC')
    return round(BUY,2), round(SELL,2)


################      SPECIAL FUNCTIONS          #############


def get_metric_variation( market, metric ,period , offset ) :
    ''':Description: computes the price variation sequentially to detect if the market is on STEADY PRICE
    It applies only to metric changes like : last_price_ch, buy_vs_sell_ch, vol_ch
    :param market:
    :param metric:
    :param period: int,
    :param offset:
    :return:  tuple of  two metrics, both must be considered '''

    METRICS = { 'id':0, 'date' : 1,  'last_price':2, 'last_price_ch' :3, 'buy_vs_sell' :4, 'buy_vs_sell_ch' :5,
                                                            'volume':6, 'vol_ch':7, 'buy_orders':8, 'sell_orders':9, 'vol_buy':10, 'vol_sell':11  }
    dataset = get_complete_bittrex_dataset( market, period , offset )
    # for i in dataset :
    #     print('time = ' , i[1] , '         price = ' , i[2]  )

    metric_change = [  float(  i[ METRICS[metric] ]  ) for i in dataset if i[ METRICS[metric] ] != None ][::-1]
    timp = [ i[1] for i in dataset ][::-1]
    print('\nStart time :', timp[0], '     End time :', timp[-1] )
    # print('\ndataset :', dataset)
    print('\nprice :', metric_change)
    Min, Max = min(metric_change) , max(metric_change)
    print('min ', Min  , '       max ',  Max  )
    abs_var = round( (Max-Min) , 4  )
    print( 'Absolute variation = ',  abs_var )

    MEtric_var = metric_change[0]              # Price_var calculation, We should not have big variations
    for i in range(1, len(metric_change)) :
        MEtric_var += metric_change[i]
        # print( 'time  =' , timp[i] , '      price_ch =' ,price_ch[i]  ,'        MEtric_var = ', MEtric_var )
    if abs_var > 6 :  return abs_var
    print('Price_var  = ', round(MEtric_var, 2)  )

    #### Also includes last hour metric :
    MEtric_var_1 , MEtric_var_2 = round( sum(metric_change),2), round(sum(metric_change[-60::]),2)
    print('Price_var_'+str(len(metric_change)/60)+'h  = ' , MEtric_var_1 , '  ;     Price_var_'+str(len(metric_change[-60::])/60)+'h  = '  ,  MEtric_var_2  )

    return MEtric_var_1, MEtric_var_2