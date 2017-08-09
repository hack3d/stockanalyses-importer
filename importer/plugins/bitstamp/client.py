#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#import configparser
#import pymysql.cursors
import json
import datetime
import requests
import sys


class Public(object):

    '''
        Constructor
    '''
    def __init__(self, logger, prod_server, storage):
        self.logger = logger
        self.url = prod_server['url']
        self.storage = storage


    def prepareTickdata(self,file):

        # prepare path
        file = self.storage['store_data'] + file

        with open(file) as data_file:
            data = json.load(data_file)

        # convert unix timestamp
        data['datetime'] = datetime.datetime.fromtimestamp(int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')

        self.logger.debug('High: %s, Low: %s, Open: %s, Last: %s, Bid: %s, Ask: %s, volume weighted average price: %s, Volume: %s, Timestamp: %s, Datetime: %s' % (data['high'], data['low'], data['open'], data['last'], data['bid'], data['ask'], data['vwap'], data['volume'], data['timestamp'], data['datetime']))

        return data


    def addTickdata(self, data, base, quote, exchange):
        try:
            result = False
            json_data = [{'base': str(base), 'quote': str(quote), 'exchange': str(exchange), 'high': str(data['high']),
                          'volume': str(data['volume']), 'datetime': str(data['datetime']), 'bid': str(data['bid']),
                          'ask': str(data['ask']), 'vwap': str(data['vwap']), 'low': str(data['low'])}]
            print(self.url + 'currencies/addTickdata')
            self.logger.info("POST Request to %s" % (self.url + 'currencies/addTickdata'))
            r = requests.post(self.url + 'currencies/addTickdata', data=json.dumps(json_data), headers={'Content-Type': 'application/json'})
            self.logger.info('Result of POST: %s' % r.text)
            print(r.text)

            self.logger.info("StatusCode of response: %s" % r.status_code)

            if r.status_code == 200:
                result = True

        except requests.exceptions.RequestException as e:
            self.logger.error("Error [%s]" % e)
            result = False

        except:
            e = sys.exc_info()[0]
            self.logger.error("Error [%s]" % e)
            result = False

        finally:
            return result
