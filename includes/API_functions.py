#  Created by Bogdan Trif on 22-05-2018 , 6:17 PM.

from urllib.parse import urlencode
import hmac
import hashlib

from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urlencode

import time, datetime

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class BITTREX(object):
    ''' :Description: Makes the interaction with the bittrex API
    '''

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret
        self.public = ['getmarkets', 'getcurrencies', 'getticker', 'getmarketsummaries', 'getmarketsummary', 'getorderbook', 'getmarkethistory']
        self.market = ['buylimit', 'buymarket', 'selllimit', 'sellmarket', 'cancel', 'getopenorders']
        self.account = ['getbalances', 'getbalance', 'getdepositaddress', 'withdraw', 'getorder', 'getorderhistory', 'getwithdrawalhistory', 'getdeposithistory']
        self.base_url = 'https://bittrex.com/api/v1.1/'
        self.nonce = int(time.time())

    def query(self, method, values={}) :
        if method in self.public :             url = self.base_url+'public/'
        elif method in self.market :             url = self.base_url+'market/'
        elif method in self.account :             url = self.base_url+'account/'
        else:             return 'Something went wrong, sorry.'


        ###### if method in self.public :
        url += method + '?' + urlencode(values)
        # print('initial public url = ', url)

        if method not in self.public:
            url += '&apikey=' + self.key
            url += '&nonce=' + str(self.nonce)


            # print('Non public url2 = \t', url )

            signature = hmac.new( bytes(self.secret, 'UTF-8'), bytes(url, 'UTF-8') , hashlib.sha512).hexdigest()
            # print('signature : ', signature)
            headers = {'apisign': signature}
        else:
            headers = {}

        print('\nurl : \t', url , '\n')

        req = requests.get(url, verify=False, headers = headers )
        print('req : \t', req)

        response = req.json()
        print('response : \t', response)

        if response["result"]:
            return response["result"]
        else:
            return response["message"]

    def getmarkets(self):
        return self.query('getmarkets')

    def getcurrencies(self):
        return self.query('getcurrencies')

    def getticker(self, market):
        return self.query('getticker', {'market': market})

    def getmarketsummaries(self):
        return self.query('getmarketsummaries')

    def getmarketsummary(self, market):
        return self.query('getmarketsummary', {'market': market})

    def getorderbook(self, market, type, depth=20):
        return self.query('getorderbook', {'market': market, 'type': type, 'depth': depth})

    def getmarkethistory(self, market, count=20):
        return self.query('getmarkethistory', {'market': market, 'count': count})

    def buylimit(self, market, quantity, rate):
        return self.query('buylimit', {'market': market, 'quantity': quantity, 'rate': rate})

    def buymarket(self, market, quantity):
        return self.query('buymarket', {'market': market, 'quantity': quantity})

    def selllimit(self, market, quantity, rate):
        return self.query('selllimit', {'market': market, 'quantity': quantity, 'rate': rate})

    def sellmarket(self, market, quantity):
        return self.query('sellmarket', {'market': market, 'quantity': quantity})

    def cancel(self, uuid):
        return self.query('cancel', {'uuid': uuid})

    def getopenorders(self, market):
        return self.query('getopenorders', {'market': market})

    def getbalances(self):
        return self.query('getbalances')

    def getbalance(self, currency):
        return self.query('getbalance', {'currency': currency})

    def getdepositaddress(self, currency):
        return self.query('getdepositaddress', {'currency': currency})

    def withdraw(self, currency, quantity, address):
        return self.query('withdraw', {'currency': currency, 'quantity': quantity, 'address': address})

    def getorder(self, uuid):
        return self.query('getorder', {'uuid': uuid})

    def getorderhistory(self, market, count):
        return self.query('getorderhistory', {'market': market, 'count': count})

    def getwithdrawalhistory(self, currency, count):
        return self.query('getwithdrawalhistory', {'currency': currency, 'count': count})

    def getdeposithistory(self, currency, count):
        return self.query('getdeposithistory', {'currency': currency, 'count': count})

