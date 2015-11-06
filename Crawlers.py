import logging
from queue import Queue, Empty
from urllib.parse import urlparse, parse_qs
import requests
import lxml.html as lhtml
import threading
import time
from requests.exceptions import ReadTimeout
from ProxyManager import BaseProxyManager
from pydispatch import dispatcher


class TooManyRequestsError(ConnectionRefusedError):
    pass


class Crawler(object):

    SIGNAL_PROGRESS = 'crawler.progress'
    SIGNAL_TOO_MANY_REQUESTS = 'crawler.too_many_requests'
    SIGNAL_TIMEOUT = 'crawler.connection_timeout'
    SIGNAL_TERMINATE = 'crawler.terminate'
    SIGNAL_TERMINATED = 'crawler.terminated'

    def __init__(self, worker_count=1, name='Crawler', worker_callback: callable = None):
        dispatcher.connect(self._abort, signal=self.SIGNAL_TERMINATE)

        self.params = {}
        self.headers = {'User-Agent': 'PyPoeci'}
        self.css_selector = 'a'  # find all links - most probably too general
        self.connection_timeout = 30

        self.lock = threading.RLock()
        self.queue = Queue()
        self.http_session = requests.Session()
        # share the http_session if the callback is a crawler-instance
        # this will probably decrease the number of refused connections
        if isinstance(worker_callback, Crawler):
            worker_callback.http_session = self.http_session

        self.worker_callback = worker_callback
        self.worker_count = worker_count
        self.name = name

        self.start_workers(worker_count=worker_count, worker=self.parse, worker_args=[worker_callback])

    def __del__(self):
        self.abort_workers()

    def set_timeout(self, timeout):
        self.connection_timeout = timeout

    def add_header(self, key, value):
        self.headers[key] = value

    def add_param(self, key, value):
        if isinstance(value, list):
            key += '[]'
        self.params[key] = value

    def set_selector(self, css_selector):
        self.css_selector = css_selector

    def _abort(self):
        logging.debug('Received TERMINATE signal. Preparing shutdown.')
        self.abort_workers()

    workers = []

    def abort_workers(self):
        try:
            while True:
                # empty the queue
                logging.debug('Clearing worker queue')
                self.queue.get_nowait()
                self.queue.task_done()
        except Empty:
            pass
        finally:
            # signal workers to stop
            logging.debug('Worker queue is empty')
            self.stop_workers()

    def stop_workers(self):
        for i in range(len(self.workers)):
            logging.debug('Sending stop-signal to worker [{}]'.format(i))
            self.queue.put(None)
        for worker in self.workers:
            worker.join()
            logging.debug('Worker [{}] stopped.'.format(worker.name))
        self.workers.clear()
        return True

    def start_workers(self, worker_count: int, worker: callable, worker_args=None):
        for i in range(worker_count):
            worker_name = 'Worker {}'.format(i)
            logging.debug('Starting worker [{}]'.format(worker_name))
            t = threading.Thread(target=worker, args=worker_args, name=worker_name)
            t.start()
            self.workers.append(t)

    def parse(self, callback: callable):
        while True:
            try:
                link = self.queue.get()
                if link is None:
                    break

                link_target = link.attrib['href']
                with self.lock:
                    logging.debug('{}'.format(link_target))
                if hasattr(callback, 'crawl'):
                    callback.crawl(link_target)
                else:
                    callback(link_target)
            finally:
                self.queue.task_done()

    def crawl(self, url):
        crawl_thread = threading.Thread(target=self._main_crawler, args=[url])
        crawl_thread.start()
        return crawl_thread

    def _main_crawler(self, url):
        logging.debug('{} started'.format(self.name))
        # do the crawl and
        # queue the extracted targets
        with self.lock:
            try:
                response = self.http_session.get(
                    url=url,
                    params=self.params,
                    headers=self.headers,
                    timeout=self.connection_timeout,
                )
                if response.status_code == 429:
                    raise TooManyRequestsError()
                self._notify_progress()
                html = response.text
                tree = lhtml.fromstring(html)
                links_to_follow = tree.cssselect(self.css_selector)
                # "td[class~='{}']>a".format(self.CSS_CLASS_RESERVABLE))
                if len(links_to_follow) > 0:
                    logging.info('Found {} links. Queueing for work'.format(len(links_to_follow)))
                    for link in links_to_follow:
                        self.queue.put(link)
            except ReadTimeout as rt:
                logging.info(
                    'Connection attempt to {} timed out after {} seconds.'.format(
                        urlparse(rt.request.url).path,
                        self.connection_timeout
                    )
                )
                self._notify_timeout(url)
            except TooManyRequestsError:
                logging.debug('Too many requests made')
                self._notify_too_many_requests(url)
            except Exception as e:
                logging.warning('Could not parse resulting page. {}'.format(e))
            finally:
                self.queue.join()
        logging.debug('{} done'.format(self.name))

    def _notify_progress(self):
        with self.lock:
            dispatcher.send(signal=self.SIGNAL_PROGRESS, sender=self)

    def _notify_too_many_requests(self, requested_url):
        with self.lock:
            dispatcher.send(
                sender=self,
                signal=self.SIGNAL_TOO_MANY_REQUESTS,
                url=requested_url,
            )

    def _notify_timeout(self, requested_url):
        with self.lock:
            dispatcher.send(
                sender=self,
                signal=self.SIGNAL_TIMEOUT,
                url=requested_url,
            )


class CalendarCrawler(Crawler):

    BASE_URL = u'https://service.berlin.de/terminvereinbarung/termin/'
    URL_CALENDAR = BASE_URL + u'tag.php'
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
                    logging.warning('Could not parse resulting page. {}'.format(e))
                    # wait for links to be crawled
        self.queue.join()

        # return home
        if self.stop_workers():
            print('>', end='', flush=True)

    def set_start_date(self, start_date):
        self.start_date = start_date



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
    BASE_URL = u'https://service.berlin.de/terminvereinbarung/termin/'
    URL_DAY = BASE_URL + u'termin.php'

    def select_appointment(self):
        while True:
            item = self.queue.get()
            if item is None:
                break
            with self.lock:
                logging.debug('Reserving {}'.format(item))
            # fetch appointment_page
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
        self.stop_workers()
