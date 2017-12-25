#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import datetime
import pytz
import requests
import sys


class Public(object):

    def __init__(self, logger, prod_server):
        """
            Constructor
        """
        self.logger = logger
        self.url = prod_server['url']
        self.username = prod_server['username']
        self.password = prod_server['password']

    def prepareTickdata(self, data):
        """
        Prepare the tickdata
        :param data: 
        :return: 
        """
        # convert unix timestamp to utc time
        local_tz = pytz.timezone("UTC")
        data['datetime'] = local_tz.localize(datetime.datetime.utcfromtimestamp(int(data['timestamp']))).strftime(
            '%Y-%m-%d %H:%M:%S')

        self.logger.debug('High: %s, Low: %s, Open: %s, Last: %s, Bid: %s, Ask: %s, volume weighted average price: %s, Volume: %s, Timestamp: %s, UTC-Datetime: %s' % (data['high'], data['low'], data['open'], data['last'], data['bid'], data['ask'], data['vwap'], data['volume'], data['timestamp'], data['datetime']))

        return data

    def addTickdata(self, data):
        """
        Add the tickdata to the database via REST call
        :param data: 
        :return: 
        """
        try:
            result = False
            json_data = [{'high': str(data['high']), 'volume': str(data['volume']), 'datetime': str(data['datetime']),
                          'bid': str(data['bid']), 'ask': str(data['ask']), 'low': str(data['low']),
                          'last': str(data['last']), 'isin': str(data['isin']), 'exchange': str(data['exchange'])}]
            print(self.url + 'currencies/addTickdata')
            self.logger.info("POST Request to %s" % (self.url + 'currencies/addTickdata'))
            r = requests.post(self.url + 'currencies/addTickdata', auth=(self.username, self.password), data=json.dumps(json_data), headers={'Content-Type': 'application/json'})
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
