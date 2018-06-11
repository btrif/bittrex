#  Created by Bogdan Trif on 21-01-2018 , 1:02 PM.

from conf.db_conn import *
from includes.API_functions import *


import pymysql
import numpy as np
from math import floor, ceil
from pathlib import Path
from os import remove, path


############        VARIABLES      ##############

Times = [1, 2, 5, 10, 15, 30, 60, 120, 240, 480, 720, 1440, 2880, 4320, 10080, 20160, 43200, 86400, 129600 ]
Periods = ['1m', '2m', '5m', '10m', '15m', '30m','1h', '2h', '4h', '8h', '12h', '1d', '2d', '3d', '1w', '2w', '4w','8w', '12w']
date_pattern = '%Y-%m-%d %H:%M:%S'

###         Markets which present risk, controlled markets, pump & dump markets ...and so on ...
EXCLUDE_MARKETS = { 'BTC-NBT' }


#################       URL'S       #################



##############      SPECIAL   FILES         ################
strongest_markets_file = 'tmp/Strongest_Markets.txt'
active_trade_market_file = 'tmp/ACTIVE_Trade_Market.txt'
temp_trade_market_file = 'tmp/TEMPORARY_Trade_Market.txt'
valid_markets_file = 'tmp/valid_Markets.txt'
bittrex_DB_markets_tables_file = 'tmp/bittrex_DB_markets_tables.txt'


##############      LOGS         ################
API_bittrex_data_log =  'log/API_bittrex_data.log'
DB_backup_log = 'log/DB_backup.log'
trade_agent_log_file = 'log/trade_agent.log'

##############      API         ################
API = BITTREX(api_key, api_secret)




###############     FUNCTIONS   ###############

def get_bittrex_Content(URL) :
    ''' :Description:   Gets the Bittrex Content by connecting to the Bittrex API
        :Returns: an JSON (JavaScript Object Notation) object in the form of a DICTIONARY '''
    req  = requests.get(URL )
    html = req.json()
    # print( type(html), html )

    RSLT = html['result']
    # print(len(RSLT) , RSLT)
    return RSLT

def read_file_line_by_line(filename) :
    ''':Description : Read a filename line by line and the put the elements found
    in the form of strings into a LIST '''
    L = []
    with open(filename, 'r') as f :
        for line in f :
            l = line.rstrip('\n')
            # print(l, type(l) )
            L.append(l)
    f.close()
    return L

def file_append_with_text(filename, text):
    ''':Description: Opens a file as APPENDABLE and appends a new line with the specified text to it.
    :Usage: Primarily used here to write to LOGS
    :Example: file_append_with_text(API_bittrex_data_log,  ' some custom text here ' )
    :param filename: filename to write to
    :param text: text which will be appended on a new line
    :return: nothing, as the purpose is only to write !                      '''
    with open(filename, "a") as myfile:
        myfile.write(text + '\n')

def is_non_empty_file( fpath ):
    ''' Returns FALSE if :
        - There IS NO FILE
        - There IS AN EMPTY FILE
        Returns TRUE if : There IS A NON-EMPY FILE !     '''
    # return os.stat( fpath ).st_size == 0
    return path.isfile(fpath) and path.getsize(fpath) > 0


def write_active_market_to_file( active_trade_market, active_trade_market_file ) :
    ''':Description: Used by the trade_agent, writes the current active market to a file  for later usage'''
    ### Write the current market to the file for later usage :
    with open(active_trade_market_file, "w") as myfile:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        myfile.write(active_trade_market+';'+str(now) )
        myfile.close()


def get_current_market_from_file(active_trade_market_file) :
    '''Function to take the current market from the current_market_file in order to monitor its progress
    :param current_market_file: Current_Market.txt
    :return: current market & date , type string            '''
    # make sure that there is active_trade_market_file and has a market written in it :
    if is_non_empty_file(active_trade_market_file) :
        with open(active_trade_market_file, 'r') as f :
            for line in f :
                cur_market = line
        print('\ncur_market = ', cur_market.split(';') )
        f.close()
        return cur_market.split(';')
    return None



#########################################################
############          MATHEMATICAL FUNCTIONS         #############


def Savitzky_Golay(y, window_size, order, deriv=0, rate=1):
    """Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    3     The Savitzky-Golay filter removes high frequency noise from data.
    4     It has the advantage of preserving the original shape and
    5     features of the signal better than other types of filtering
    6     approaches, such as moving averages techniques.
    7     Parameters
    8     ----------
    9     y : array_like, shape (N,)
    10         the values of the time history of the signal.
    11     window_size : int
    12         the length of the window. Must be an odd integer number.
    13     order : int
    14         the order of the polynomial used in the filtering.
    15         Must be less then `window_size` - 1.
    16     deriv: int
    17         the order of the derivative to compute (default = 0 means only smoothing)
    18     Returns
    19     -------
    20     ys : ndarray, shape (N)
    21         the smoothed signal (or it's n-th derivative).
    22     Notes
    23     -----
    24     The Savitzky-Golay is a type of low-pass filter, particularly
    25     suited for smoothing noisy data. The main idea behind this
    26     approach is to make for each point a least-square fit with a
    27     polynomial of high order over a odd-sized window centered at
    28     the point.
    29     Examples
    30     --------
    31     t = np.linspace(-4, 4, 500)
    32     y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    33     ysg = savitzky_golay(y, window_size=31, order=4)
    34     import matplotlib.pyplot as plt
    35     plt.plot(t, y, label='Noisy signal')
    36     plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    37     plt.plot(t, ysg, 'r', label='Filtered signal')
    38     plt.legend()
    39     plt.show()
    40     References
    41     ----------
    42     .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
    43        Data by Simplified Least Squares Procedures. Analytical
    44        Chemistry, 1964, 36 (8), pp 1627-1639.
    45     .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
    46        W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
    47        Cambridge University Press ISBN-13: 9780521880688
"""

    from math import factorial

    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))

    return np.convolve( m[::-1], y, mode='valid')


def get_complete_bittrex_dataset( market, period , offset ):
    ''' Gets the complete dataset from bittrex exchange
    :param market: string, market to consider
    :param period: int, MINUTES for which the graph is plotted
    :param offset: int, offset, how many MINUTES BEHIND will be the last time
    :return: complete dataset, list of tuples       '''

    connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db = DB1 )
    cur = connection.cursor()

    # print('market name = ', market)
    METRICS = { 'id':0, 'date' : 1,  'last_price':2, 'last_price_ch' :3, 'buy_vs_sell' :4, 'buy_vs_sell_ch' :5,
                'volume':6, 'vol_ch':7, 'buy_orders':8, 'sell_orders':9, 'vol_buy':10, 'vol_sell':11   }
    # minutes = period *60
    prep_CountQuery = 'SELECT  count(*) FROM  `market_name` ;' #.replace('column', metric)
    prep_CountResult = cur.execute(prep_CountQuery.replace('market_name', market) )
    # print('prep_CountResult : ', prep_CountResult )
    count_rows = cur.fetchone()
    # print('count_rows |: ',  count_rows[0] )
    if count_rows[0] - offset >= period :     # If we can go back in time by this amount :
        prep_MarketQuery  = 'SELECT  id, date, last_price, last_price_ch, buy_vs_sell, buy_vs_sell_ch, volume, vol_ch, buy_orders, sell_orders, ' \
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


def get_dataset( source_exchange , market, metric, period ):
    ''':Description: Gets a dataset by interogating a specific DB. First it gets all data and then
    it select only needed metric

    :param source_exchange: DB
    :param market: actual market, like BTC-ADA
    :param metric: last_price, price_ch, volume, ....
    :param period: int, how many hours
    :return: lst,  array of floats
    '''
    if source_exchange == 'bittrex' : db = DB1
    elif source_exchange == 'marketcap' : db = DB2
    else : return 'No such exchange source. No such database'

    connection = pymysql.connect( host= HOST, port=3306, user=USER, passwd=PASSWD, db = db )
    cur = connection.cursor()

    # print('market name = ', market)

    if source_exchange == 'bittrex' :
        METRICS = { 'id':0, 'last_price':1, 'last_price_ch' :2, 'buy_vs_sell' :3, 'buy_vs_sell_ch' :4, 'volume':5, 'vol_ch':6, 'buy_orders':7, 'sell_orders':8 }
        minutes = period *60
        prep_CountQuery = 'SELECT  count(*) FROM  `market_name` ;' #.replace('column', metric)
        prep_CountResult = cur.execute(prep_CountQuery.replace('market_name', market) )
        # print('prep_CountResult : ', prep_CountResult )
        count_rows = cur.fetchone()
        # print('count_rows |: ',  count_rows[0] )
        if count_rows[0] >= minutes :
            prep_MarketQuery  = 'SELECT  id, last_price, last_price_ch, buy_vs_sell, buy_vs_sell_ch, volume, vol_ch, buy_orders, sell_orders' \
                                        ' FROM `market_name` ORDER BY ID DESC LIMIT %s;' #.replace('column', metric)
            prep_MarketResult = cur.execute(prep_MarketQuery.replace('market_name', market), ( minutes) )

        else : return []

    if source_exchange == 'marketcap' :
        period = period *12
        if market == '_market_cap' :
            prep_MarketQuery  = 'SELECT  id, column  FROM _market_cap ORDER BY ID DESC LIMIT %s;'.replace('column', metric)
            prep_MarketResult = cur.execute(prep_MarketQuery.replace(metric, metric ) , ( period) )
        else :
            prep_MarketQuery  = 'SELECT  id, column FROM `market_name` ORDER BY ID DESC LIMIT %s;'.replace('column', metric)
            prep_MarketResult = cur.execute(prep_MarketQuery.replace('market_name', market), (period) )

        # else : return 'wrong market name !'

    # if prep_MarketResult == 0 : return 'wrong market name or wrong metric !'

    # print('prep_MarketResult : ', prep_MarketResult)
    row = cur.fetchall()
    # print('prep_MarketQuery : ', prep_MarketQuery)
    # print('row = ', row)
    dataset = [ float( I[METRICS[metric]] ) for I in row if I[METRICS[metric]] != None ]
    # print('dataset : ', dataset)
    connection.close()
    return dataset


    # if prep_MarketResult == 0 : return 'wrong market name or wrong metric !'

    # print('prep_MarketResult : ', prep_MarketResult)
    row = cur.fetchall()
    # print('prep_MarketQuery : ', prep_MarketQuery)
    # print('row = ', row)
    dataset = [ float( I[METRICS[metric]] ) for I in row if I[METRICS[metric]] != None ]
    # print('dataset : ', dataset)
    connection.close()
    return dataset





def find_local_min_and_max(dataset):
    ''':Description : findd the local minimum and maximum of a dataset returning two separate arrays of INDEXES
    Returns INDEXES of the min or max values'''
    minimum = (np.diff(np.sign(np.diff(dataset))) > 0).nonzero()[0] + 1 # local min
    maximum = (np.diff(np.sign(np.diff(dataset))) < 0).nonzero()[0] + 1 # local max
    return minimum, maximum


def find_local_min_or_max(dataset):
    ''':Description : find the local minimum and maximum of a dataset returning a single array od INDEXES
    Returns INDEXES of the min or max values'''
    min_max = np.diff(np.sign(np.diff(dataset))).nonzero()[0] + 1   # local min+max
    return min_max


def get_moving_average( source_exchange, market , metric, period, slice  ) :
    ''':Description: computes the moving average of an array
    :==parameters==:
    :source_exchange: the source from where the data is taken: bittrex, coinmarketcap
    :market: market name.
            :Example: : BTC-LSK, USDT-BTC on bittrex.
            On coinmarketcap DB : ADA, XRP, _market_cap

    :metric: which values to use : Example : buy_vs_sell_ch, volume_ch, price_ch, ...
    :period: the period on which the data set is taken. Example 2 hours
    :slice: the desired moving average frame. Example : slice= 60 will take the average of 60 minutes
                and compute the average
    '''
    B = get_dataset('bittrex', market, metric, period )[::-1]
    if len(B) >0  :
        # print( market ,' : ', len(B) ,B, '\n')
        length = len(B)

        AVG = []
        for i in range(0, length-slice +1 ):
            average = round(sum(B[i:slice+i])/slice, 8 )
            AVG.append(average)
            # L = B[i:slice+i]
            # print(str(i) + '.    slice : ', average  ,len(L), L)
        return np.array(AVG)

    else :
        return []


def binary_search(n, List):        # VERY FAST ALGORITHM
    ''':Description: Search for an element in the list and returns the index of it. If it not finds it returns
        the index of the element closest to its left, the smaller number.
    :param: **n**- integer, the number to find
                **List** - lst type, the list to search for
    :returns:   int, the index of the element
    '''
    left = 0
    right = len(List) -1

    while left <= right:
        midpoint = (left+right)//2
        if List[midpoint] == n: return midpoint
        elif List[midpoint] > n: right = midpoint-1
        else: left = midpoint+1
    if n > List[midpoint]: return midpoint
    else: return (midpoint-1)



######## TIME & DATE CONVERSIONS #########


def get_timezone_diff():
    ''' This function is not affected by daylight time savings changes on Mars & October
        returns in seconds. Example: 3600, 7200, 10800, ...'''
    utc, local = datetime.datetime.utcnow(), datetime.datetime.today()
    delta = local - utc
#     print('utc time : ',utc, '     local time : ',local)
#     print( 'Timeshift in seconds : ' , delta.total_seconds() )
    return int(delta.total_seconds())

# get_timezone_diff()

def convert_time_bittrex_format_to_epoch(bittrex_time):
    bittrex_time = bittrex_time.split('.')[0]
    bt = datetime.datetime.strptime( bittrex_time, "%Y-%m-%dT%H:%M:%S")
    # bt = datetime.datetime.strptime( bittrex_time, "%Y-%m-%dT%H:%M:%S.%f")
    # print(bt)
    return time.mktime(bt.timetuple())

def convert_utc_time_to_epoch():
    ut = datetime.datetime.utcnow()
    # print(ut)
    return time.mktime(ut.timetuple())

def convert_epoch_to_standard_UTC(epoch):
    ''':Description: Takes an epoch time of the form 1521528914  and transforms it to standard  UNIVERSAL time     '''
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))

def convert_epoch_to_standard_local_time(epoch):
    ''':Description: Takes an epoch time of the form 1521528914  and transforms it to standard time LOCAL time     '''
    epoch += get_timezone_diff()
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))

def Now_t(epoch) :
    ''':Description: Shortened, Takes an epoch time of the form 1521528914  and transforms it to standard time     '''
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))

def convert_time_standard_format_to_epoch(standard_time):
    standard_time = standard_time.split('.')[0]
    st = datetime.datetime.strptime( standard_time, "%Y-%m-%d %H:%M:%S")
#     print(bt)
    return time.mktime(st.timetuple())

def convert_time_UTC_bittrex_format_to_local(bittrex_time):
    ''' :Usage: convert_time_UTC_bittrex_format_to_local('2018-03-23T08:45:03.71')
    :param bittrex_time: returns in standard time
    :return:                                            '''
    bittrex_time = bittrex_time.split('.')[0]
    bt = datetime.datetime.strptime( bittrex_time, "%Y-%m-%dT%H:%M:%S")
    local_epoch = time.mktime(bt.timetuple()) + get_timezone_diff()
#     print('local epoch :', local_epoch)
    local_time_standard_form = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(local_epoch) )
    # print( 'local_time_standard_form :', local_time_standard_form  )
    return local_time_standard_form

# convert_time_UTC_bittrex_format_to_local('2018-03-23T08:45:03.71')

def compute_dime_diff(time_in, last_update ) :
    ''':Description: Computes time difference between two dates in minutes. time_in must precede last_update (should be first date) '''
    t1 = datetime.datetime.strptime( time_in, "%Y-%m-%d %H:%M:%S")
    t2 = datetime.datetime.strptime( last_update, "%Y-%m-%d %H:%M:%S")
    mins = int( (t2-t1).total_seconds() // 60 )
    # print(t1, t2, '  total seconds' , (t2-t1).total_seconds() , mins )
    return mins


######################


def get_market_real_volume(market, mins) :
    '''Get the REAL VOLUME of the market by investigating the past tranzactions of the market
    to the period specified as argument
    :return: BUY, SELL in BTC    '''
    API = BITTREX(api_key, api_secret)
    A = API.getmarkethistory(market , 1000 )
    now_epoch_utc = convert_utc_time_to_epoch()
    # print('now_utc : ', now_epoch_utc ,'       ',    datetime.datetime.utcnow() ,'\n' )
    BUY, SELL = 0, 0
    for cnt, res in enumerate(A) :
        bittrex_epoch = convert_time_bittrex_format_to_epoch( res['TimeStamp'] )
        if bittrex_epoch >= now_epoch_utc - mins*60 :
            # print( str(cnt+1) , '.         time : ', res['TimeStamp']  ,'     ' ,res['OrderType'], '    total= ',res['Total'] ,'        ', res )
            if res['OrderType'] == 'BUY' :        BUY += res['Total']
            if res['OrderType'] == 'SELL' :        SELL += res['Total']
        if bittrex_epoch < now_epoch_utc - mins*60 :     break

    BUY, SELL = round(BUY, 4), round(SELL, 4)
    # print('Real Volume in the last ' + str(mins) + ' mins  is :     BUY = ', BUY ,'      SELL = ', SELL ,'    BTC' , '      TOTAL = ', BUY+SELL ,'    BTC' ,  )

    return BUY, SELL



#####################################################


