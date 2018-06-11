#  Created by Bogdan Trif on 22-03-2018 , 5:58 PM.

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from .app_functions import *
from random import randint

def plot_dataset( dataset, timeset ) :

        fig = plt.figure(1, figsize=( 16, 6 ))

        # print('lengths : dataset, t ', len(dataset), len(t))

        ax = plt.subplot(1, 1, 1 )
        plt.grid(which='both')

        plt.title('period= '+ str(period)+ 'h' )
        plt.xlabel('Time')
        plt.ylabel(metric)
        # plt.xlim(0, (period)*60)


        # window_size, order = 21, 0
        # SV_61_5 = Savitzky_Golay(np.array( price_ch), window_size=window_size, order=order) # window size 61, polynomial order 5
        #
        # plt.plot(timp, SV_61_5, color='orange', label = 'Sav_Gol ' + str(window_size)+' ' + str(order), linewidth = 2 )

        ##### beautify the x-labels
        plt.gcf().autofmt_xdate()
        myFmt = mdates.DateFormatter('%d-%m %H:%M')
        plt.gca().xaxis.set_major_formatter(myFmt)

        plt.plot( timp, price_ch,  linestyle='-' , linewidth = 2 , markersize=5, label=metric )      #plot the dots
        # plt.subplots_adjust(hspace = .001)

        plt.legend(loc = 0)
        plt.tight_layout()
        plt.show()



def get_price_variation_with_plot( market, metric ,period , offset, plot_args ) :
    dataset = get_complete_bittrex_dataset( market, period*60 , offset*60 )
    # for i in dataset :
    #     print('time = ' , i[1] , '         price = ' , i[2]  )

    price_ch = [  float(i[3]) for i in dataset ][::-1]
    timp = [ i[1] for i in dataset ][::-1]
    print('\nStart time :', timp[0], '     End time :', timp[-1] )
    # print('\ndataset :', dataset)
    print('\nprice :', price_ch)
    Min, Max = min(price_ch) , max(price_ch)
    print('min ', Min  , '       max ',  Max  )
    abs_var = round( (Max-Min) , 4  )
    print( 'Absolute variation = ',  abs_var )

    Prc_var = price_ch[0]              # Price_var calculation, We should not have big variations
    for i in range(1, len(price_ch)) :
        Prc_var += price_ch[i]
        # print( 'time  =' , timp[i] , '      price_ch =' ,price_ch[i]  ,'        Prc_var = ', Prc_var )

    print( 'price variation = ', Prc_var )

    colors = ['mediumvioletred', 'm', 'seagreen', 'teal', 'royalblue',  'dodgerblue', 'coral', 'salmon', 'yellowgreen' ]
    markers = [  '.' , ',' , '.' , ',' , '.' , ','   ]
    METRICS = { 'id':0, 'date' : 1,  'last_price':2, 'last_price_ch' :3, 'buy_vs_sell' :4, 'buy_vs_sell_ch' :5,
                'volume':6, 'vol_ch':7, 'buy_orders':8, 'sell_orders':9, 'vol_buy':10, 'vol_sell':11  }



    ########   PLOT     #########
    def plot_dataset( dataset, timeset, color ) :
        # print('lengths : dataset, t ', len(dataset), len(t))
        plt.grid(which='both')

        plt.title('market : ' + market + ',  period= '+ str(period)+ 'h' )
        plt.xlabel('Time')
        plt.ylabel(metric)
        # plt.xlim(0, (period)*60)

        ##### beautify the x-labels
        plt.gcf().autofmt_xdate()
        myFmt = mdates.DateFormatter('%d-%m %H:%M')
        plt.gca().xaxis.set_major_formatter(myFmt)

        plt.plot( timp, dataset,  linestyle='-' , linewidth = 2, color=color  ,markersize=5, label=metric )      #plot the dots
        # plt.subplots_adjust(hspace = .001)
        plt.tight_layout()
        plt.legend(loc = 0)


    if len(plot_args) >0 :
        fig = plt.figure(1, figsize=( 18, 3.5*len(plot_args) ))

        for cnt, metric in enumerate(plot_args) :
            metric_set = [  float(i[ METRICS[metric] ]) for i in dataset ][::-1]
            ax = plt.subplot( len(plot_args) , 1, cnt+1 )
            plot_dataset( metric_set, timp, color=colors[randint(0,8) ] )
        plt.show()

    #######   END PLOT      ########

    if abs_var > 6 :  return abs_var

    return Prc_var

