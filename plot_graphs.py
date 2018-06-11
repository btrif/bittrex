#  Created by Bogdan Trif on 06-02-2018 , 9:54 PM.


import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from includes.app_functions import *
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import datetime

# from scipy.signal import savgol_filter


def plot_market_metric(  source_exchange, market , metric, period, slice, subplot  ) :
    ''':Description: this plots a market by doing the following operations :
        -   gets the moving average of the market
        - makes the Savitzy-Golay filter and plots it    '''

    mov_avg = get_moving_average(source_exchange, market, metric, period+1, slice )
    # print(market ,':  \t', type(mov_avg), len(mov_avg), mov_avg )
    t2 = np.arange( 0, len(mov_avg) )
    # timp = [datetime.datetime.now() - datetime.timedelta(minutes=i) for i in range(0, len(mov_avg) )]
    # print('time : ', t)
    fig = plt.figure(1, figsize=( 18, 12 ))     # Here we plot 2 figures !

    ax = plt.subplot(2, 1, subplot )

    # plt.xlim(len(t), 0)

    plt.grid(which='both')
    plt.plot( t2, mov_avg, color = 'aquamarine')

    #### Constructed method  SAVITZKY - GOLAY
    window_size, order = 11, 1
    SV_31_5 = Savitzky_Golay(mov_avg, window_size=window_size, order=order) # window size 31, polynomial order 5
    plt.plot( t2, SV_31_5, color='mediumblue', label = 'Sav_Gol ' + str(window_size)+' ' + str(order), linewidth = 1 )


    window_size, order = 61, 1
    SV_61_5 = Savitzky_Golay(mov_avg, window_size=window_size, order=order) # window size 61, polynomial order 5
    plt.plot(t2, SV_61_5, color='red', label = 'Sav_Gol ' + str(window_size)+' ' + str(order), linewidth = 1 )

    #### Min & Max points
    min1_max1 = find_local_min_or_max(SV_31_5)
    # print(' min1_max1 =  ', min1_max1 )
    # plt.plot( t[min1_max1], SV_31_5[min1_max1], color = "b", marker = "3", markersize=2 )      #plot the dots
    plt.plot( t2[min1_max1], SV_31_5[min1_max1], "bD" , markersize=3 )      #plot the dots

    ################## second #################

    min2_max2 = find_local_min_or_max(SV_61_5)
    # print(' min2_max2 =  ', min2_max2  )
    plt.plot( t2[min2_max2], SV_61_5[min2_max2], "r>" , markersize=9 )       #plot the dots

    plt.plot(t2, mov_avg, color='y', marker='.' ,linestyle='-.', alpha=0.8, ms=4, label='mov_avg')
    plt.title('exchange: '+str(source_exchange)+',   market: '+market + ',    period= '+ str(period)+ 'h,    slice= '+ str(slice) +' min' )

    plt.xlabel('Time')
    ##### beautify the x-labels
    # plt.gcf().autofmt_xdate()
    # myFmt = mdates.DateFormatter('%H:%M')
    # plt.gca().xaxis.set_major_formatter(myFmt)

    plt.xlim(0, (period)*60 )

    plt.ylabel(metric)
    plt.legend(loc = 0)

    # plt.legend(bbox_to_anchor=(0.12, 0.2))


def plot_dataset( source_exchange, market , metric, period, subplot, color, marker ) :

    dataset = get_dataset( source_exchange , market, metric, period )
    # print(dataset)
    t = np.arange( 0, len(dataset) )

    timp = [datetime.datetime.now() - datetime.timedelta(minutes=i) for i in range(0, len(dataset) )]
    # print('lengths : dataset, t ', len(dataset), len(t))
    ax = plt.subplot(5, 1, subplot )
    plt.grid(which='both')

    plt.title('period= '+ str(period)+ 'h' )
    plt.xlabel('Time')
    plt.ylabel(metric)
    # plt.xlim(0, (period)*60)


    window_size, order = 61, 5
    SV_61_5 = Savitzky_Golay(np.array( dataset), window_size=window_size, order=order) # window size 61, polynomial order 5

    # plt.plot(t, SV_61_5, color='orange', label = 'Sav_Gol ' + str(window_size)+' ' + str(order), linewidth = 2 )

    ##### beautify the x-labels
    plt.gcf().autofmt_xdate()
    myFmt = mdates.DateFormatter('%d-%m %H:%M')
    plt.gca().xaxis.set_major_formatter(myFmt)

    plt.plot( timp, dataset, color= color, marker = marker , linestyle='-' , linewidth = 2 , markersize=5, label=metric )      #plot the dots
    plt.subplots_adjust(hspace = .001)

    plt.legend(loc = 0)
    plt.tight_layout()


def plot_simple_dataset( source_exchange, market, metric, period, subplot, color  ) :

    dataset = get_dataset(source_exchange , market, metric , period)
    print(str(metric)+' dataset : \n' ,dataset)

    t = np.arange( 0, len(dataset) )

    timp = [datetime.datetime.now() - datetime.timedelta(minutes=i) for i in range(0, len(dataset) )]
    # print('lengths : dataset, t ', len(dataset), len(t))
    fig = plt.figure(1, figsize=( 18, 11 ))
    ax1 = plt.subplot( 2 , 1 , subplot )
    ax1.grid(which='both')


    plt.title('period= '+ str(period)+ 'h', y = -4 )
    ax1.set_xlabel('Time')
    ax1.set_xlabel(metric)

    # plt.xlim(0, (period)*60)


    # window_size, order = 61, 5
    # SV_61_5 = Savitzky_Golay(np.array( dataset), window_size=window_size, order=order) # window size 61, polynomial order 5

    # plt.plot(t, SV_61_5, color='orange', label = 'Sav_Gol ' + str(window_size)+' ' + str(order), linewidth = 2 )

    ##### beautify the x-labels
    plt.gcf().autofmt_xdate()
    myFmt = mdates.DateFormatter('%d-%m %H:%M')
    plt.gca().xaxis.set_major_formatter(myFmt)

    plt.plot( timp, dataset, color= color, marker = '.' , linestyle='-' , linewidth = 2 , markersize=5, label=metric )      #plot the dots

    plt.legend(loc = 0)
    # plt.tight_layout()

    ax2 = ax1.twiny()
    ax2.set_xlabel("x")
    ax2.set_xlim(  len(dataset), 0  )





def plot_bittrex_metrics_market( market, period, hours_behind, *args ) :
    # plot_market_metric(  source_exchange , cur_market , 'buy_vs_sell_ch', period , slice, 1  )

    colors = ['mediumvioletred', 'm', 'seagreen', 'teal', 'royalblue',  'dodgerblue', 'coral', 'salmon', 'yellowgreen' ]
    markers = [  '.' , ',' , '.' , ',' , '.' , ','   ]
    METRICS = { 'id':0, 'date' : 1,  'last_price':2, 'last_price_ch' :3, 'buy_vs_sell' :4, 'buy_vs_sell_ch' :5,
                'volume':6, 'vol_ch':7, 'buy_orders':8, 'sell_orders':9, 'vol_buy':10, 'vol_sell':11  }

    just = datetime.datetime.now() - datetime.timedelta(minutes = offset*60 )
    just = just.strftime("%Y-%m-%d %H.%M")           # Data si ora including si lag hours behind
    # print('just : ', just)
    minutes = period*60

    dataset = get_complete_bittrex_dataset ( market, period*60, hours_behind*60 )
    # print(str(dataset)+' dataset : \n' ,dataset, '\n')

    t = np.arange( 0, len(dataset) )
    timp = [datetime.datetime.now() - datetime.timedelta(minutes=i+hours_behind*60) for i in range(0, len(dataset) )]
    # print('lengths : dataset, t ', len(dataset), len(t))

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
        plt.subplots_adjust( left=0.0475, bottom=0.07, right=0.99 , top=0.98, wspace=0.2, hspace=0.1 )

        ax1.set_xlabel('Time')
        plt.legend(loc = 0)

    # plt.tight_layout()
    # ax2 = ax1.twiny()
    # ax2.set_xlabel("x")
    # ax2.set_xlim(  len(dataset), 0  )

    plt.savefig('img/'+str(just) +' ' + str(period)  + 'h  ' +str(market) +' offset_'+ str(hours_behind) +'h' + '.png' )

    plt.show()




#########       END DEF   #############

now = datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S")

### We plot the current market
# try :
#     with open(active_trade_market_file, 'r') as f :
#         for line in f :
#             cur_market = line
#             print('\ncur_market = ', cur_market, type (cur_market),'      ' ,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") , '\n'  )
# except :
#     FileNotFoundError
#     print('File Not Found.   We will choose a custom market')


cur_market = 'BTC-REP'


print('\ncurrent market : ', cur_market,'      ' ,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'\n' )

source_exchange = 'bittrex'

period = 8
offset = 6

slice= 60



plot_bittrex_metrics_market( cur_market, period, offset, 'buy_vs_sell', 'buy_vs_sell_ch', 'volume' , 'vol_ch',  'last_price', 'last_price_ch'  )

# plot_market_metric(  source_exchange, cur_market , 'volume', period, slice, 1  )
# plt.show()



# plot_all_metrics_market(source_exchange, cur_market, period, slice)



# fig = plt.figure(1, figsize=( 18, 12 ))     # Here we plot 2 figures !
# plot_dataset(  source_exchange , cur_market , 'buy_vs_sell_ch', period,  1, 'm' , ','  )
# plot_dataset(  source_exchange , cur_market , 'volume', period,  2, 'coral' , ','  )
# plot_dataset( source_exchange, cur_market , 'vol_ch', period, 3, 'salmon' ,'.')
# plot_dataset(  source_exchange , cur_market , 'last_price', period ,  4, 'teal', '.'  )
# plot_dataset( source_exchange, cur_market , 'last_price_ch', period, 5 , 'forestgreen', '.' )
# plt.savefig('img/'+str(now) +'  ' +str(market) +'.png' )
# plt.show()

# data = get_dataset(source_exchange,cur_market, 'vol_ch', period )
# for i in  range( len(data) ) :
#     if abs( data[i]  ) > 1 :
#         print(i, '.   ', data[i] )



# plt.show()
# plot_simple_dataset( source_exchange, cur_market , 'buy_vs_sell_ch', period )








