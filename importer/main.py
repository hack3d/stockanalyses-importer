#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import logging.handlers
import pymysql.cursors
import time as t

from importer.plugins.bitstamp import client


# config
config = configparser.ConfigParser()
config.read('config')

prod_server = config['prod']
storage = config['path']


##########
# Logger
##########
LOG_FILENAME = storage['storage_logs']+'Importer.log'
# create a logger with the custom name
logger = logging.getLogger('stockanalyses.Importer')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler(LOG_FILENAME)
fh.setLevel(logging.DEBUG)
# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=11000000, backupCount=5)
#create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(handler)
#logger.addHandler(ch)


'''
    Try to get a job to import data
'''
def getJob():
    try:
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'], prod_server['database'])

        # prepare cursor
        cursor_job = db.cursor()

        sql = 'select action, id_stock, filename, timestamp from import_jq where timestamp <= now() and action in (1000) ' \
              'order by timestamp limit 1;'
        logger.debug(sql)
        cursor_job.execute(sql)

        # check if we get a result
        logger.debug('Number of jobs: %s' % cursor_job.rowcount)
        if cursor_job.rowcount > 0:
            result = cursor_job.fetchone()
        else:
            result = 0

        return result

    except pymysql.Error as e:
        logger.error("Error [%s]" % (e))

    finally:
        cursor_job.close()
        db.close()


'''
    Update the column action for a specific job
'''
def updateJob(current_action, new_action, value):
    try:
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'], prod_server['database'])

        # prepare cursor
        cursor_job = db.cursor()

        sql = "update `import_jq` set `action`= %s, `modify_timestamp` = now(), `modify_user` = 'importer' where `action` = %s and `filename` = %s;"
        logger.debug(sql % (new_action, current_action, value))
        cursor_job.execute(sql,(new_action, current_action, value))

        db.commit()

    except pymysql.Error as e:
        db.rollback()
        new_action = 0
        logger.error("Error [%s]" % (e))

    finally:
        return new_action
        db.close()

'''
    Get id from currency
'''
def getCurrency(currency):
    try:
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'], prod_server['database'])

        # prepare cursor
        cursor_cny = db.cursor()

        sql = 'select currency_id from currency where symbol_currency = %s'
        logger.debug(sql % (currency))
        cursor_cny.execute(sql, currency)

        if cursor_cny.rowcount > 0:
            tmp = cursor_cny.fetchone()
            result = tmp[0]
        else:
            result = 0

    except pymysql.Error as e:
        result = '-1'
        logger.error("Error [%s]" % (e))

    finally:
        return result
        cursor_cny.close()
        db.close()


'''
    Get id from exchange
'''
def getExchange(exchange):
    try:
        db = pymysql.connect(prod_server['servername'], prod_server['username'], prod_server['password'], prod_server['database'])

        # prepare cursor
        cursor_ehe = db.cursor()

        sql = 'select idexchange from exchange where exchange_symbol = %s'
        logger.debug(sql % (exchange))
        cursor_ehe.execute(sql, exchange)

        if cursor_ehe.rowcount > 0:
            tmp = cursor_ehe.fetchone()
            result = tmp[0]
        else:
            result = 0

    except pymysql.Error as e:
        result = '-1'
        logger.error("Error [%s]" % (e))

    finally:
        return result
        cursor_ehe.close()
        db.close()


'''
    Main
'''
def main():
    logger.info('Start StockanalysesImporter...')

    bitstamp_client = client.Public(logger)

    while True:
        logger.debug('Get a job...')
        job = getJob()
        action_tmp = '-1'
        status = False
        exchange = ''
        base = ''
        quote = ''


        if job != 0 and job[0] == 1000:
            array = job[1].split("#")
            exchange = array[0]
            base = array[1]
            quote = array[2]

            logger.info('Job action: %s, base: %s, quote: %s, exchange: %s' % (job[0], base, quote, exchange))

            action_tmp = updateJob(job[0], '1100', job[2])
            logger.info('Set action to %s' % ('1100'))

        else:
            logger.info('no job for me...')


        if action_tmp == '1100':
            id_base = getCurrency(base)
            id_quote = getCurrency(quote)
            id_exchange = getExchange(exchange)

            logger.debug('id base: %s, id_quote: %s, id exchange: %s' % (str(id_base), str(id_quote), str(id_exchange)))

            if id_base > 0 and id_quote > 0 and id_exchange > 0:
                if exchange == 'bitstamp':
                    bitstamp_data = bitstamp_client.prepareTickdata(job[2])
                    if bitstamp_client.addTickdata(bitstamp_data,id_base, id_quote, id_exchange):
                        updateJob(action_tmp, '1200', job[2])
                    else:
                        # we have to send a mail
                        updateJob(action_tmp, '1900', job[2])

        if action_tmp == '0':
            # we have to send a mail
            updateJob(action_tmp, '1900', job[2])


        t.sleep(5)
                



if __name__ == '__main__':
        main()