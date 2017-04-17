#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import pymysql.cursors
import json
import datetime


class Public(object):

    '''
        Constructor
    '''
    def __init__(self, logger):
        self.logger = logger

        # config
        config = configparser.ConfigParser()
        config.read('/opt/stockanalyses-importer/importer/config')

        prod_server = config['prod']
        storage = config['path']

        try:
            self.db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'], prod_server['database'])

        except pymysql.Error as e:
            self.logger.error("Error [%s]" % (e))


    '''
        Destructor
    '''
    def __del__(self):
        self.logger.info("Close database connection for bitstamp.")
        self.db.close()


    def prepareTickdata(self,file):

        # prepare path
        file = '/opt/stockanalyses-downloader/data/' + file

        with open(file) as data_file:
            data = json.load(data_file)

        # convert unix timestamp
        data['datetime'] = datetime.datetime.fromtimestamp(int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')

        self.logger.debug('High: %s, Low: %s, Open: %s, Last: %s, Bid: %s, Ask: %s, volume weighted average price: %s, Volume: %s, Timestamp: %s, Datetime: %s' % (data['high'], data['low'], data['open'], data['last'], data['bid'], data['ask'], data['vwap'], data['volume'], data['timestamp'], data['datetime']))

        return data


    def addTickdata(self,data, base, quote, exchange):

        try:
            # prepare cursor
            cursor_tick = self.db.cursor()

            sql = 'insert into currency_now (base_currency, quote_currency, high,volume, latest_trade, bid, ask, currency_volume, low, exchange_idexchange, insert_timestamp, insert_user) ' \
                  'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)'

            self.logger.debug(sql % (base, quote, data['high'], data['volume'], data['datetime'], data['bid'], data['ask'], data['vwap'], data['low'], exchange, 'importer'))

            cursor_tick.execute(sql, (base, quote, data['high'], data['volume'], data['datetime'], data['bid'], data['ask'], data['vwap'], data['low'], exchange, 'importer'))

            self.db.commit()
            result = True
        except pymysql.Error as e:
            self.db.rollback()
            result = False
            self.logger.error("Error [%s]" % (e))

        finally:
            return result
            cursor_tick.close()