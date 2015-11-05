#!/usr/bin/env python3
import sys
import argparse
import time

import socket
import _socket

import lxml
import lxml.html as lhtml

import requests
from stem import connection, Signal

import logging
import json_log_filter

css_class_reservable = 'buchbar'
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

BASE_URL = u'https://service.berlin.de/terminvereinbarung/termin/tag.php'
# ?id=&buergerID=&buergername=ich&absagecode=&Datum=1448627400


def getDate(start_date):
    if start_date:
        pass
    else:
        return int(time.time())


def handle_calendar_links(calendar_links):
    logging.info('Found {} links'.format(len(calendar_links)))
    for link in calendar_links:
        logging.debug('{ahref}'.format(ahref=link.attrib['href']))


def main(args):
    logging.basicConfig(
        format='{"time": "%(asctime)s", "level": "%(levelname)s", "source": "%(module)s", "message": %(message)s}',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='buergeramt.log',
        level=logging.DEBUG
    )

    json_log_filter.enable_filter()

    logging.info('Start searching for free appointments')
    direct_socket = socket.socket
    arguments = parse_args(args)
    if arguments.tor:
        logging.debug("TOR requested")
        import socks
        socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 9050)
        socket.socket = socks.socksocket

    startdate = getDate(arguments.start_date)

    params = {'Datum': startdate, 'dienstleister[]': param_service_ids, 'anliegen[]': param_request}

    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'}

    calendar_links = []

    while True:
        try:
            print("-", end="", flush=True)
            response = requests.get(BASE_URL, params, headers=headers)
            if response.status_code == 429:
                print("8", end="\n", flush=True)
                renew_connection(direct_socket)
                continue
            html = response.text
            tree = lhtml.fromstring(html)
            calendar_links = tree.cssselect("td[class~='{}']>a".format(css_class_reservable))
            if len(calendar_links) > 0:
                if handle_calendar_links(calendar_links):
                    break
            print("\b.", end="", flush=True)
        except lxml.etree.XMLSyntaxError:
            print('X', end="", flush=True)

    for link in calendar_links:
        print('{ahref}'.format(ahref=link.attrib['href']))

    sys.exit(0)


def parse_args(args):
    parser = argparse.ArgumentParser(description='Find free appointments on buergeramt website.')
    parser.add_argument('--start_date', '-d', help='define the start date')
    parser.add_argument('--tor', '-t', help='If you want to use tor', action='store_true')
    arguments = parser.parse_args(args)
    return arguments


def renew_connection(direct_socket: _socket.socket):
    logging.debug('Renewing tor-IP address')
    # to reach the TOR instance we need to disable the proxy
    proxy_socket = socket.socket
    socket.socket = direct_socket

    try:
        with connection.connect_port(port=9051, password='test') as controller:
            if not controller.is_newnym_available:
                time.sleep(controller.get_newnym_wait())
            controller.signal(Signal.NEWNYM)
            # wait for the change to apply
            time.sleep(controller.get_newnym_wait())
    finally:
        # re-enable proxy
        socket.socket = proxy_socket
        logging.debug('Re-enabling proxy')
    # print our new ip address
    logging.info('New IP: {}'.format(requests.get('http://ipinfo.io/ip').text.replace('\n', '')))


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        print(" caught. all abort")
        exit(1)
