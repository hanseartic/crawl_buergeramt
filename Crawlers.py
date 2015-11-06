import logging
from queue import Queue
from urllib.parse import urlparse, parse_qs

import requests
import lxml.html as lhtml
import threading

import time
from requests.exceptions import ReadTimeout

from ProxyManager import BaseProxyManager


class Crawler(object):
    BASE_URL = u'https://service.berlin.de/terminvereinbarung/termin/'

    def __init__(self, proxy_manager: BaseProxyManager = BaseProxyManager()):
        self.queue = Queue()
        self.proxy_manager = proxy_manager
        self.user_agent = 'BuergeramtTermine'
        self.lock = threading.RLock()

    def set_user_agent(self, user_agent):
        self.user_agent = user_agent

    def get_headers(self):
        return {
            'User-Agent': self.user_agent
        }

    workers = [threading.Thread]

    def end_workers(self):
        for i in range(len(self.workers)):
            logging.debug('Sending worker {} stop-signal'.format(i))
            self.queue.put(None)
        for worker in self.workers:
            worker.join()
        return True

    def start_workers(self, worker_count, target):
        for i in range(worker_count):
            logging.debug('Starting worker {}'.format(i))
            t = threading.Thread(target=target)
            t.start()
            self.workers.append(t)


class CalendarCrawler(Crawler):
    URL_CALENDAR = Crawler.BASE_URL + u'tag.php'
    CSS_CLASS_RESERVABLE = 'buchbar'

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
    start_date = 0

    def crawl(self):
        # start 5 workers that can crawl details in parallel
        self.start_workers(5, self.explore_day)
        connection_timeout = 10
        with requests.Session() as http_session:
            while True:
                try:
                    with self.proxy_manager.lock:
                        response = http_session.get(
                            url=self.URL_CALENDAR,
                            params=self.get_params(),
                            headers=self.get_headers(),
                            timeout=connection_timeout,
                        )
                    if response.status_code == 429:
                        print('8', end='\n', flush=True)
                        self.proxy_manager.renew_connection()
                        continue
                    html = response.text
                    tree = lhtml.fromstring(html)
                    calendar_links = tree.cssselect("td[class~='{}']>a".format(self.CSS_CLASS_RESERVABLE))
                    if len(calendar_links) > 0:
                        yield calendar_links
                        print('<', end='', flush=True)
                        if self.handle_calendar_links(calendar_links):
                            break
                    time.sleep(2)
                    print('.', end='', flush=True)
                except ReadTimeout as rt:
                    logging.debug(
                        'Connection attempt to {} timed out after {} seconds.'.format(
                            urlparse(rt.request.url).path,
                            connection_timeout
                        )
                    )
                    self.proxy_manager.renew_connection()
                except Exception as e:
                    logging.warning('Could not parse resulting page.')
            # wait for links to be crawled
        self.queue.join()

        # return home
        if self.end_workers():
            print('>', end='', flush=True)

    def set_start_date(self, start_date):
        self.start_date = start_date

    def get_params(self):
        return {
            'termin': 1,
            'Datum': self.start_date,
            'dienstleister[]': self.param_service_ids,
            'anliegen[]': self.param_request,
        }

    def handle_calendar_links(self, calendar_links):
        logging.info('Found {} links. Queueing for work'.format(len(calendar_links)))
        for link in calendar_links:
            self.queue.put(link)
        return True

    def explore_day(self):
        detail_crawler = DetailCrawler(self.proxy_manager)
        detail_crawler.set_user_agent(self.user_agent)
        while True:
            link = self.queue.get()
            if link is None:  # stop the worker explicitly
                break
            link_target = link.attrib['href']
            with self.lock:
                logging.debug('{}'.format(link_target))
            detail_crawler.crawl(link_target)
            self.queue.task_done()


class DetailCrawler(Crawler):
    URL_DAY = Crawler.BASE_URL + u'termin.php'

    def select_appointment(self):
        while True:
            item = self.queue.get()
            if item is None:
                break
            with self.lock:
                logging.debug('Reserving {}'.format(item))
            #  fetch appointment_page
            #  extract form data
            #  fill form
            #  make post request

            self.queue.task_done()

    def crawl(self, link_target):
        # start workers, that can preselect the appointments in parallel
        self.start_workers(10, self.select_appointment)

        logging.info(
            'Finding free appointments on {}.'.format(
                parse_qs(urlparse(link_target).query)['datum']
            )
        )
        with requests.Session() as http_session:
            while True:
                details_view = http_session.get(
                    Crawler.BASE_URL + link_target,
                    headers=self.get_headers(),
                    params=self.get_params()
                )
                if details_view.status_code == 429:
                    self.proxy_manager.renew_connection()
                    continue
                self.queue.put(details_view.text)
                break

        self.queue.join()
        self.end_workers()
