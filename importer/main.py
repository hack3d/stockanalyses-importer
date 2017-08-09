#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import json
import os
import logging.handlers
import pymysql.cursors
import requests
import time as t

from importer.plugins.bitstamp import client


# config
dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(dir_path + '/config')

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
        print(prod_server['url'] + 'job/importer_jobs')
        r = requests.get(prod_server['url'] + 'job/importer_jobs')
        print(r.text)

        return r.json()

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))



'''
    Update the column action for a specific job
'''
def updateJob(current_action, new_action, value):

    try:
        json_data = []
        json_data.append({'new_action': str(new_action), 'current_action': str(current_action), 'filename': str(value)})
        print(prod_server['url'] + 'job/set_importer_jobs_state')
        logger.debug("PUT Request to %s with data %s" % ((prod_server['url'] + 'job/set_importer_jobs_state'), json_data))
        r = requests.put(prod_server['url'] + 'job/set_importer_jobs_state', data=json.dumps(json_data), headers={'Content-Type': 'application/json'})

        result_text = r.text
        result_text = result_text.encode('utf-8')
        print(result_text)


    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))
        new_action = 0

    finally:
        return new_action

'''
    Get id from currency
'''
def getCurrency(currency):
    try:
        result = ''
        print(prod_server['url'] + 'currencies/' + str(currency))
        r = requests.get(prod_server['url'] + 'currencies/' + str(currency))
        print(r.text)

        rs_tmp = r.json()

        if rs_tmp['currency_id'] == "null":
            result = 0
        else:
            result = rs_tmp['currency_id']

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))
        result = '-1'

    finally:
        return result


'''
    Get id from exchange
'''
def getExchange(exchange):
    try:
        result = ''
        print(prod_server['url'] + 'exchanges/' + str(exchange))
        r = requests.get(prod_server['url'] + 'exchanges/' + str(exchange))
        print(r.text)

        rs_tmp = r.json()

        if rs_tmp['idexchange'] == "null":
            result = 0
        else:
            result = rs_tmp['idexchange']

    except requests.exceptions.RequestException as e:
        logger.error("Error [%s]" % (e))
        result = '-1'

    finally:
        return result


'''
    Main
'''
def main():
    logger.info('Start StockanalysesImporter...')

    bitstamp_client = client.Public(logger, prod_server, storage)

    while True:
        logger.debug('Get a job...')
        job = getJob()
        action_tmp = '-1'
        status = False
        exchange = ''
        base = ''
        quote = ''


        if job['action'] == 1000:
            array = job['id_stock'].split("#")
            exchange = array[0]
            base = array[1]
            quote = array[2]

            logger.info('Job action: %s, base: %s, quote: %s, exchange: %s' % (job['action'], base, quote, exchange))

            action_tmp = updateJob(job['action'], '1100', job['filename'])
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
                    logger.info("We are running on exchange: %s" % exchange)
                    bitstamp_data = bitstamp_client.prepareTickdata(job['filename'])
                    if bitstamp_client.addTickdata(bitstamp_data, id_base, id_quote, id_exchange):
                        os.remove(storage['store_data']+job['filename'])
                        updateJob(action_tmp, '1200', job['filename'])
                    else:
                        # we have to send a mail
                        updateJob(action_tmp, '1900', job['filename'])

        if action_tmp == '0':
            # we have to send a mail
            updateJob(action_tmp, '1950', job['filename'])


        t.sleep(5)
                



if __name__ == '__main__':
        main()