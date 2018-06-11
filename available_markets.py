#  Created by Bogdan Trif on 20-01-2018 , 3:34 PM.

import requests     # to make GET request
from time import strftime, localtime, gmtime, sleep
from includes.app_functions import *
import re


date = strftime("%Y-%m-%d %H:%M:%S",   localtime() )
#print( 'Current Date   --->   ', date ,'\n'  )

URL = 'https://bittrex.com/api/v1.1/public/getmarketsummaries'
URL2 = 'https://bittrex.com/api/v1.1/public/getmarkets'


# filename = 'market_data.html'
# Write to file :
# with open(filename, 'w', encoding='utf-8') as file :
#     file.write(str(html))
#     print(  html )



def get_delisted_markets( URL2):
    ### We get here the external content which contain full specification. We'll read from here
    ### notices regading the delisting of certain coins
    del_MARKETS = get_bittrex_Content(URL2)
    delisted_MARKETS = set()
    for cnt, ITEM in enumerate(del_MARKETS) :
        market = ITEM["MarketName"]
        notice = str(ITEM['Notice'])
        pattern = re.compile('[Dd]elist.*|[Rr]emov.*')
        re.search(pattern, notice)
        # print(str(cnt+1)+'.     ',  market, '     Notice : ', notice ,'          pattern =', re.search(pattern, notice))

        # if str(notice).find('Delisted') != -1 :
        if re.search(pattern, notice) :
            # print(str(cnt+1)+'.     ',  market, '     Notice : ', notice ,'          pattern =', re.search(pattern, notice))
            delisted_MARKETS.add(market)

    # print('== delisted_MARKETS == ', delisted_MARKETS )
    return delisted_MARKETS


print('== delisted_MARKETS == ' , get_delisted_markets( URL2) )

def write_to_file_valid_markets(filename) :
    delisted_MARKETS = get_delisted_markets(URL2)
    #print('\n Markets to be soon DELISTED : ', delisted_MARKETS ,'\n' )
    MARKETS = get_bittrex_Content(URL)
    outF = open(filename , 'w')
    cnt=1
    for  ITEM in MARKETS :
        market = ITEM["MarketName"]
        if market not in delisted_MARKETS :
            outF.write(market +'\n')
            # print(str(cnt)+'.     ',  market)
            cnt += 1
    outF.close()


# sleep(5)
write_to_file_valid_markets(valid_markets_file)



#valid_Markets  = read_file_line_by_line(filename)
#print(len(valid_Markets), valid_Markets )