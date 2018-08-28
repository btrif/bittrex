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


class ValidMarkets():
    '''Description: Write ONLY valid markets to the file valid_markets_file.txt

    '''
    def __init__(self, URL ,URL2, filename ):
        self.URL = URL
        self.URL2 = URL2
        self.filename = filename
        self.delisted_MARKETS =  self.get_delisted_markets( )
        self.valid_markets = self.write_to_file_valid_markets( )

    def get_delisted_markets( self ):
        ### We get here the external content which contain full specification. We'll read from here
        ### notices regarding the delisting of certain coins
        del_MARKETS = get_bittrex_Content(self.URL2)
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

        print('== delisted_MARKETS == ', delisted_MARKETS  )
        print('len delisted_MARKETS : ', len(delisted_MARKETS) )


        return delisted_MARKETS



    def write_to_file_valid_markets( self ) :
        ''':Description: Method to write ONLY valid Markets (not delisted or which will be removed) in the valid market file.txt
        :param URL:
        :param filename:
        :return:
        '''
        MARKETS = get_bittrex_Content(self.URL)
        print('len total MARKETS : ', len(MARKETS) )
        outF = open(self.filename , 'w')
        cnt=0
        for  ITEM in MARKETS :
            market = ITEM["MarketName"]
            if market not in self.delisted_MARKETS :
                outF.write(market +'\n')
                # print(str(cnt+1)+'.     ',  market)
                cnt += 1

        print('len Valid Markets : ', cnt )
        outF.close()


if __name__ == '__main__':

    VM = ValidMarkets(URL, URL2, valid_markets_file )
    print('delisted Markets  : ', VM.delisted_MARKETS )


# valid_Markets  = read_file_line_by_line( valid_markets_file )
# print(len(valid_Markets), valid_Markets )