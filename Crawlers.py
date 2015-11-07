import logging
from queue import Queue, Empty
from urllib.parse import urlparse
import requests
import lxml.html as lhtml
import threading
from requests.exceptions import ReadTimeout
from pydispatch import dispatcher


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
        self.workers = []

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_workers()

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
        with self.lock:
            for i in range(len(self.workers)):
                logging.debug('Sending stop-signal to worker [{}]'.format(i))
                self.queue.put(None)
            for worker in self.workers:
                worker.join()
                logging.debug('Worker [{}] stopped.'.format(worker.name))
            self.workers.clear()
        return True

    def start_workers(self, worker_count: int, worker: callable, worker_args=None):
        w = tuple(worker_args)
        for i in range(worker_count):
            worker_name = 'Worker {}'.format(i)
            logging.debug('Starting worker [{}]'.format(worker_name))
            t = threading.Thread(target=worker, args=w+(worker_name,), name=worker_name)
            t.start()
            self.workers.append(t)

    def parse(self, callback: callable, worker_name=''):
        while True:
            try:
                logging.debug('{} waiting for work in queue'.format(worker_name))
                link = self.queue.get()
                logging.debug('{} got work from queue'.format(worker_name))
                if link is None:
                    logging.debug('{}: no more jobs - going home'.format(worker_name))
                    break
                logging.info('{}: Processing link'.format(worker_name))
                if hasattr(link, 'href'):
                    link_target = link.attrib['href']
                else:
                    link_target = link
                logging.debug('{}'.format(link_target))
                if hasattr(callback, 'crawl'):
                    callback.crawl(link_target)
                else:
                    callback(link_target)
            finally:
                self.queue.task_done()
        logging.debug('{}: bye'.format(worker_name))

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
                if len(links_to_follow) > 0 and len(self.workers) > 0:
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


class TooManyRequestsError(ConnectionRefusedError):
    pass
