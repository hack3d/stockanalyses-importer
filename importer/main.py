#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import json
import os
import logging.handlers
import pika

from importer.plugins.bitstamp import client
from importer.plugins.bitfinex.client import Bitfinex

# config
dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(dir_path + '/config')

prod_server = config['prod']
storage = config['path']

##########
# Logger
##########
LOG_FILENAME = storage['storage_logs'] + 'Importer.log'
# create a logger with the custom name
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("stockanalyses.Importer")
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=11000000, backupCount=5)
logger.addHandler(handler)


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
            ch.basic_nack(delivery_tag=method.delivery_tag)

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
            ch.basic_nack(delivery_tag=method.delivery_tag)


def main():
    """
    Main
    :return: 
    """
    logger.info('Start Stockanalyses-Importer...')

    while True:
        logger.debug('Get a job...')
        action_tmp = '-1'
        status = False
        exchange = ''
        base = ''
        quote = ''

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


if __name__ == '__main__':
    main()
