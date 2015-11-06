#!/usr/bin/env python3
import sys
import argparse
import time
import logging
import json_log_filter
from Crawlers import Crawler
from ProxyManager import TorProxyManager, BaseProxyManager
from pydispatch import dispatcher

CSS_CLASS_RESERVABLE = 'buchbar'
CSS_CLASS_FREE_APPOINTMENT = 'frei'

BASE_URL = u'https://service.berlin.de/terminvereinbarung/termin/'
URL_CALENDAR = BASE_URL + u'tag.php'
URL_DETAILS = u'termin.php'

param_service_ids = [  # dienstleister
    '122210', '122217', '122219', '122227', '122231', '122238', '122243', '122252', '122260', '122262', '122254',
    '122271', '122273', '122277', '122280', '122282', '122284', '122291', '122285', '122286', '122296', '150230',
    '122301', '122297', '122294', '122312', '122314', '122304', '122311', '122309', '317869', '324433', '325341',
    '324434', '324435', '122281', '324414', '122283', '122279', '122276', '122274', '122267', '122246', '122251',
    '122257', '122208', '122226',
]
param_request = [  # anliegen
    #   '120703',  # Personalausweis beantragen
    #   '120686',  # Anmelden einer Wohnung
    '121151',  # Reisepass beantragen
    #   '121469',  # Kinderreisepass beantragen
    #   '120926',  # Führungszeugnis beantragen
    #   '120702',  # Meldebescheinigung beantragen
    #   '121627',  # Ersterteilung Führerschein
    #   '121629',  # Erweiterung Führerschein
    #   '121637',  # Neuerteilung Führerschein nach Entzug
    #   '121593',  # Ersatzführerschein nach Verlust
]


def get_date(start_date):
    if start_date:
        pass
    else:
        return int(time.time())


pm = BaseProxyManager()


def main(args):
    logging.basicConfig(
        format='{"time": "%(asctime)s", "level": "%(levelname)s", "source": "%(module)s", "message": %(message)s}',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='buergeramt.log',
        level=logging.DEBUG
    )

    json_log_filter.enable_filter()

    #  don't show the TRACEs from stem in the logs
    logging.getLogger('stem').addFilter(lambda rec: rec.levelname.upper() != 'TRACE')

    logging.info('Start searching for free appointments')
    arguments = parse_args(args)
    if arguments.tor:
        global pm
        logging.debug('TOR enabled')
        pm = TorProxyManager(9051, 'test')
        pm.enable_proxy()

    dispatcher.connect(receiver=on_crawler_progress, signal=Crawler.SIGNAL_PROGRESS)
    dispatcher.connect(receiver=on_crawler_stressed, signal=Crawler.SIGNAL_TOO_MANY_REQUESTS)
    dispatcher.connect(receiver=on_crawler_timeout, signal=Crawler.SIGNAL_TIMEOUT)
    dispatcher.connect(receiver=on_crawler_terminated, signal=Crawler.SIGNAL_TERMINATED)

    crawler = Crawler(worker_count=5, name='Calendar crawler', worker_callback=calendar_callback)
    crawler.add_header(
        'User-Agent',
        'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'
    )
    crawler.set_timeout(10)
    crawler.add_param('Datum', get_date(arguments.start_date))
    crawler.add_param('termin', 1)
    crawler.add_param('dienstleister', param_service_ids)
    crawler.add_param('anliegen', param_request)

    crawler.set_selector("td[class~='{}']>a".format(CSS_CLASS_RESERVABLE))

    while True:
        ct = crawler.crawl(URL_CALENDAR)
        try:
            ct.join()
        except KeyboardInterrupt as ki:
            dispatcher.send(Crawler.SIGNAL_TERMINATE)
            del crawler
            ct.join()

            raise ki
            break
    exit(0)


def on_crawler_progress():
    print('.', end='', flush=True)


def on_crawler_stressed(sender: Crawler, url: str):
    print('8', end='\n', flush=True)
    with sender.lock:
        pm.renew_connection()
    if sender.name == 'Details Crawler':
        sender.crawl(url).join()


def on_crawler_timeout(sender: Crawler, url: str):
    print('T', end='', flush=True)
    with sender.lock:
        pm.renew_connection()


def on_crawler_terminated(*args):
    global crawler_count, cr
    crawler_count -= 1
    if crawler_count <= 0:
        cr = False


def calendar_callback(detail_link):
    with Crawler(worker_count=10, name='Details Crawler',
                 worker_callback=reserve_appointment) as details_crawler:
        details_crawler.set_timeout(15)
        details_crawler.set_selector("td[class~='{}']>a".format(CSS_CLASS_FREE_APPOINTMENT))
        details_crawler.add_header(
            'User-Agent',
            'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'
        )
        details_crawler.crawl(
            BASE_URL + detail_link
        ).join()


def reserve_appointment(appointment_link):
    logging.info('APP: {}'.format(appointment_link))


def parse_args(args):
    parser = argparse.ArgumentParser(description='Find free appointments on buergeramt website.')
    parser.add_argument('--start_date', '-d', help='define the start date')
    parser.add_argument('--tor', '-t', help='If you want to use tor', action='store_true')
    # parser.add_argument('--socks', '-s', help='Use a socks5 proxy', nargs='+')
    arguments = parser.parse_args(args)
    return arguments


cr = False
crawler_count = 0


def wait_exit(signal):
    while cr:
        pass
    exit(signal)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        print(" caught. all abort")
        dispatcher.send(Crawler.SIGNAL_TERMINATE)
        wait_exit(1)
