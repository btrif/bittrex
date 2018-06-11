#  Created by Bogdan Trif on 06-03-2018 , 7:31 PM.

from includes.DB_functions import *
from includes.plot_functions import  *
from trade_agent import *

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db=DB1 )
# cur = connection.cursor()

# API = BITTREX(api_key, api_secret)

#####################################
#####################################


current_market = 'BTC-ZEC'





# BTC_balance = API.getbalance('BTC')['Available']


# look_for_market_opportunities(connection, cur )


# update_tranzations_table(current_market, 29 )
# insert_into_tranzactions_table(current_market, 28, connection, cur )


# bid_ask = API.getticker(current_market)
# bid, ask = bid_ask['Bid'], bid_ask['Ask']
#
# gBP = get_buy_price(bid, ask)
# compute_median_price(current_market, 'BUY', 2)

#################################################
##############      BUY / SELL ORDERS      #############

# gOO_BUY = get_Opened_Orders(current_market, 'BUY' , 36 )
# print( len(gOO_BUY), '    uuid : ' ,gOO_BUY[0][1], '            ', gOO_BUY )

gOO_SELL = get_Opened_Orders(current_market, 'SELL' , 1136 )
print( len(gOO_SELL), '     first values ' ,gOO_SELL[0][1:3], '            ', gOO_SELL )

# gCO_BUY = get_Closed_Orders(current_market, 'BUY' , 36 )
# print( len(gCO_BUY), '    time_completed : ' ,gCO_BUY[0][1], '            ', gCO_BUY )

# gCO_SELL = get_Closed_Orders(current_market, 'SELL' , 36 )
# print( len(gCO_SELL), '    time_completed : ' ,gCO_SELL[0][1], '            ', gCO_SELL )

##############    END  BUY / SELL ORDERS      #############
####################################################






# get_market_Balance(current_market, 'Balance')

t1 = Now_t(time.time())
# update_table('_trade_now',current_market ,True ,BTC_balance=BTC_balance, time_update=t1, status='SELL_FINISHED' , coin_balance=coin_balance )

# insert_into_table('_trade_now', current_market , False ,coin_balance=coin_balance, time_update=t1, status='SELL_PROCESS' )

# get_metric_variation( current_market, 'last_price_ch', 3,  1  )
# get_price_variation( market, 'last_price', 1,  0.5) # ) , ['last_price_ch', 'last_price', 'volume'] )

# market_Balance = API.getbalance(current_market.split('-')[1])['Balance']
# if market_Balance != None :
#     print( ( market_Balance > 0 )  )

# uuid = [ u[1] for u in gOO_SELL ]
# print('uuid = ', uuid[0],   uuid )


# GPV = get_price_variation(current_market, 'last_price', 1, 3.5 )


# 2018-03-27, 19:10  , This will be completed AFTERWARDS as it requires MORE COMPLEX ALGORITHMS
# if policy_type == 'DYNAMIC' :







# look_for_market_opportunities(connection, cur)
# get_complete_bittrex_dataset( current_market, 2 , 0 )

# get_metric_variation( current_market, 'last_price_ch' , 3 , 0 )


###########             PLOT            #############
# print( get_price_variation_with_plot(current_market, 'price_ch', 1, 1 ,['last_price', 'last_price_ch', 'volume', 'buy_vs_sell'] ) )



# diff = compute_dime_diff('2018-05-10 09:36:14', date )
# print('time diff : ', diff)

# priceCh = 0.34
# buy_vol, sell_vol = get_market_real_volume(current_market, 1)

# insert_variations_table = cur.execute(QUERIES['insert_variations'] ,( current_market, date, (buy_vol-sell_vol), priceCh ) )
# print('insert_variations_table : ', insert_variations_table)

connection.commit()
connection.close()

