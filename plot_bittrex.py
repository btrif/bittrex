#  Created by Bogdan Trif on 30-10-2018 , 5:03 PM.

from conf.sensitive import *
from includes.API_functions import *

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import pymysql
import numpy as np


connection = pymysql.connect( host= btrif, port=33066, user=USER, passwd=PASSWD, db = bittrex )
cur = connection.cursor()


def get_complete_bittrex_dataset( market, period , offset  ):
    ''' Gets the complete dataset from bittrex exchange
    :param market: string, market to consider
    :param period: int, MINUTES for which the graph is plotted
    :param offset: int, offset, how many MINUTES BEHIND will be the last time
    :return: complete dataset, list of tuples       '''


    # print('market name = ', market)
    METRICS = { 'id':0, 'date' : 1,  'last_price':2, 'last_price_ch' :3, 'buy_vs_sell' :4, 'buy_vs_sell_ch' :5,
                'volume':6, 'vol_ch':7, 'buy_orders':8, 'sell_orders':9, 'vol_buy':10, 'vol_sell':11   }
    # minutes = period *60
    prep_CountQuery = 'SELECT  count(*) FROM  `market_name` ;'

    prep_CountResult = cur.execute(prep_CountQuery.replace('market_name', market) )
    # print('prep_CountResult : ', prep_CountResult )
    count_rows = cur.fetchone()
    # print('count_rows |: ',  count_rows[0] )
    if count_rows[0] - offset >= period :     # If we can go back in time by this amount :
        prep_MarketQuery  = 'SELECT  id, date, last_price, price_ch, BvS, BvS_ch, volume, vol_ch, buy_ord, sell_ord, ' \
                                    'vol_buy, vol_sell FROM `market_name` ORDER BY ID DESC LIMIT %s OFFSET %s;'
        prep_MarketResult = cur.execute(prep_MarketQuery.replace('market_name', market), ( period, offset ) )

    else : return []

    row = cur.fetchall()
    # print('prep_MarketQuery : ', prep_MarketQuery)
    # print('row = ', row)
    # dataset = [ float( I[METRICS[metric]] ) for I in row if I[METRICS[metric]] != None ]
    # print('dataset : ', dataset)
    connection.close()
    return row


# X = get_complete_bittrex_dataset('BTC-DASH', 2, 1  )
# for cnt, rec in enumerate(X) :
#     print(str(cnt+1),'.      ', rec )





def plot_bittrex_metrics_market( market, period, hours_behind, *args ) :
    # plot_market_metric(  source_exchange , cur_market , 'buy_vs_sell_ch', period , slice, 1  )

    colors = ['mediumvioletred', 'm', 'seagreen', 'teal', 'royalblue',  'dodgerblue', 'coral', 'salmon', 'yellowgreen' ]
    markers = [  '.' , ',' , '.' , ',' , '.' , ','   ]
    METRICS = { 'id':0, 'date' : 1,  'last_price':2, 'price_ch' :3, 'BvS' :4, 'BvS_ch' :5,
                'volume':6, 'vol_ch':7, 'buy_ord':8, 'sell_ord':9, 'vol_buy':10, 'vol_sell':11  }

    just = datetime.datetime.now() - datetime.timedelta(hours = offset*12 )
    just = just.strftime("%Y-%m-%d %H.%M")           # Data si ora including si lag hours behind
    # print('just : ', just)
    # minutes = period*12

    dataset = get_complete_bittrex_dataset ( market, period*12+1, hours_behind*12  )
    # print('length dataset : ' , len(dataset), '\ndataset : \n' , dataset, '\n')

    offset_time = dataset[0][1]
    # print('offset_time : ', offset_time )
    t = np.arange( 0, len(dataset) )
    timp = [ offset_time - datetime.timedelta(minutes= i*5 ) for i in range(0, len(dataset) )]

    # print(' time offset test :  ',  offset_time - datetime.timedelta(minutes= hours_behind*60  )  )
    # print('timp : ', timp )
    # print('lengths : dataset,  t ', len(dataset), len(t))

    fig = plt.figure(1, figsize=( 20, 12 ) )


    # plt.title('period= '+ str(period)+ 'h', y = -4 )


    for cnt, metric in enumerate( args ) :
        # print( str(cnt+1) ,'.         metric = ' , metric )

        # METRICS[metric]
        data = [  i[METRICS[metric]]  for i in dataset  ]
        # print('data : ',len( data) , data )

        ax1 = plt.subplot( len(args) , 1 , cnt+1 )
        ax1.grid(which='both')

        # print('  metric ---> ', metric )
        ax1.set_ylabel( metric )


        if cnt == 0 :         plt.title('exchange:  bittrex'+',   market: '+market + ',    period= '+ str(period)+ 'h'+',  offset = '+ str(hours_behind) +'h' )

        plt.gcf().autofmt_xdate()
        myFmt = mdates.DateFormatter('%d-%m %H:%M')
        plt.gca().xaxis.set_major_formatter(myFmt)

        plt.plot( timp, data, color= colors[cnt] , marker = markers[cnt] , linestyle='-' , linewidth = 1.5 , markersize=5, label=metric )
        plt.subplots_adjust( left=0.0475, bottom=0.08, right=0.99 , top=0.98, wspace=0.2, hspace=0.1 )

        ax1.set_xlabel('Time')
        plt.legend(loc = 0)

    # plt.tight_layout()
    # ax2 = ax1.twiny()
    # ax2.set_xlabel("x")
    # ax2.set_xlim(  len(dataset), 0  )

    plt.savefig('img/'+str(just) +' ' + str(period)  + 'h  ' +str(market) +' offset_'+ str(hours_behind) +'h' + '.png' )

    # plt.show()


#########       END DEF   #############


if __name__ == '__main__':

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur_market = 'BTC-OMG'

    print('\ncurrent market : ', cur_market,'       current time :  ' , now , '\n' )

    source_exchange = 'bittrex'
    period = 24
    offset = 0

    plot_bittrex_metrics_market(  cur_market, period, offset, *['BvS',  'volume' , 'last_price'] )
