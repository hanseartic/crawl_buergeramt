#!/usr/bin/env python3
import sys
import argparse
import time

import logging
import json_log_filter
from Crawlers import CalendarCrawler
from ProxyManager import TorProxyManager


def get_date(start_date):
    if start_date:
        pass
    else:
        return int(time.time())


def main(args):
    logging.basicConfig(
        format='{"time": "%(asctime)s", "level": "%(levelname)s", "source": "%(module)s", "message": %(message)s}',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='buergeramt.log',
        level=logging.DEBUG
    )

    json_log_filter.enable_filter()

    logging.info('Start searching for free appointments')
    arguments = parse_args(args)
    if arguments.tor:
        logging.debug('TOR enabled')
        pm = TorProxyManager(9051, 'test')
        pm.enable_proxy()

    c = CalendarCrawler(pm)
    c.set_user_agent('Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36')
    c.set_start_date(get_date(arguments.start_date))
    links = c.crawl()
    for r in links:
        pass

    sys.exit(0)


def parse_args(args):
    parser = argparse.ArgumentParser(description='Find free appointments on buergeramt website.')
    parser.add_argument('--start_date', '-d', help='define the start date')
    parser.add_argument('--tor', '-t', help='If you want to use tor', action='store_true')
    parser.add_argument('--socks', '-s', help='Use a socks5 proxy', nargs='+')
    arguments = parser.parse_args(args)
    return arguments


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        print(" caught. all abort")
        exit(1)
