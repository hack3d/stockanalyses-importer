#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import json
import os
import logging.handlers
import pika
import requests

from plugins.bitstamp import client
from plugins.bitfinex.client import Bitfinex

# config
dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(dir_path + '/config')

prod_server = config['prod']
storage = config['path']

# Default handling
# Minimum database version this application can handle
database_version = 1


##########
# Logger
##########
# default logging
log_level = logging.INFO
if storage['logs_filename'] == "":
    logs_filename = 'Importer.log'
else:
    logs_filename = storage['logs_filename']

if storage['logs_max_size'] == "":
    logs_max_size = 11000000
else:
    logs_max_size = storage['logs_max_size']

if storage['logs_rotated_files'] == "":
    logs_rotated_files = 5
else:
    logs_rotated_files = storage['logs_rotated_files']

# log level
if prod_server['log_level'] == 'DEBUG':
    log_level = logging.DEBUG

if prod_server['log_level'] == 'INFO':
    log_level = logging.INFO

if prod_server['log_level'] == 'WARNING':
    log_level = logging.WARNING

if prod_server['log_level'] == 'ERROR':
    log_level = logging.ERROR

if prod_server['log_level'] == 'CRITICAL':
    log_level = logging.CRITICAL

LOG_FILENAME = storage['storage_logs'] + logs_filename
# create a logger with the custom name
logging.basicConfig(filename=LOG_FILENAME, level=log_level,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("stockanalyses.Importer")
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=int(logs_max_size),
                                               backupCount=int(logs_rotated_files))
logger.addHandler(handler)


def getDatabaseVersion():
    try:
        print(prod_server['url'] + 'dbversion')
        logger.info("URL for database version: %s", prod_server['url'] + 'dbversion')
        r = requests.get(prod_server['url'] + 'dbversion', auth=(prod_server['username'], prod_server['password']))
        print(r.text)
        logger.debug('Result database version: %s', r.text)

        return r.json()
    except requests.exceptions.RequestException as e:
        logger.error('Error [%s]', e)


def callback(ch, method, properties, body):
    body = body.decode('utf-8').replace("'", '"')
    json_message = json.loads(body)
    print(json_message)
    logger.info("ISIN: %s and exchange: %s" % (json_message['isin'], json_message['exchange']))

    # Check for which exchange we want to import the data
    # Bitstamp
    if json_message['exchange'] == 'btsp':
        logger.info('Data for exchange bitstamp.')

        bitstamp_client = client.Public(logger, prod_server)
        bitstamp_data = bitstamp_client.prepareTickdata(json_message)
        if bitstamp_client.addTickdata(bitstamp_data):
            logger.info('Bitstamp data successfully imported.')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info('RabbitMQ message acknowledged.')
        else:
            logger.warning('Bitstamp data not imported')
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)

    if json_message['exchange'] == 'btfx':
        logger.info('Data for exchange bitfinex.')

        bitfinex_client = Bitfinex(logger, prod_server)
        bitfinex_data = bitfinex_client.prepareTickdata(json_message)
        if bitfinex_client.addTickdata(bitfinex_data):
            logger.info('Bitfinex data successfully imported.')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info('RabbitMQ message acknowledged.')
        else:
            logger.warning('Bitfinex data not imported')
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)


def main():
    """
    Main
    :return: 
    """
    logger.info('Start Stockanalyses-Importer...')
    logger.info('Minimum database version: %s' % database_version)

    # we will check if the application can handle the database schema.
    dbversion_data = getDatabaseVersion()
    if dbversion_data['versions'][0]['version_number'] >= int(database_version):
        while True:
            try:
                credentials = pika.PlainCredentials(prod_server['rabbitmq_username'], prod_server['rabbitmq_password'])
                connection = pika.BlockingConnection(pika.ConnectionParameters(prod_server['rabbitmq_host'], 5672, '/',
                                                                               credentials))

                channel = connection.channel()
                channel.queue_declare(queue=prod_server['rabbitmq_queue'], durable=True)

                logger.info('Connect to queue %s' % (prod_server['rabbitmq_queue']))

                # set up subscription on the queue
                channel.basic_consume(callback, queue=prod_server['rabbitmq_queue'], no_ack=False)

                # start consuming (blocks)
                channel.start_consuming()

            except Exception:
                logger.warning('%s cannot connect', str(prod_server['rabbitmq_producer']), exc_info=True)

            finally:
                connection.close()
    else:
        logger.warning("Database version is to low. database: %s, config: %s",
                       dbversion_data['versions'][0]['version_number'], database_version)

if __name__ == '__main__':
    main()
