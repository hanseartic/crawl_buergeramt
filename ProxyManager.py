import socket
import socks
import requests
from requests.exceptions import ReadTimeout
from stem import connection, Signal
import time
import threading
import logging


class BaseProxyManager(object):
    def __init__(self):
        self.lock = threading.RLock()

    def renew_connection(self):
        pass


class TorProxyManager(BaseProxyManager):
    def __init__(self, tor_mgmt_port=9051, tor_mgmt_password=''):
        super().__init__()
        self.direct_socket = socket.socket
        self.proxied_socket = None
        self.tor_mgmt_port = tor_mgmt_port
        self.tor_mgmt_password = tor_mgmt_password
        self.is_proxy_enabled = False

    def enable_proxy(self):
        if self.is_proxy_enabled:
            return True
        if not self.proxied_socket:
            socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, "127.0.0.1", 9050)
            self.proxied_socket = socks.socksocket
        socket.socket = self.proxied_socket
        self.is_proxy_enabled = True
        return False

    def disable_proxy(self):
        if not self.is_proxy_enabled:
            return False
        socket.socket = self.direct_socket
        self.is_proxy_enabled = False
        return True

    def renew_connection(self):
        logging.debug('Renewing tor-IP address')
        self.lock.acquire()
        # to reach the TOR instance we need to disable the proxy
        previous_proxy_state = self.disable_proxy()

        try:
            with connection.connect_port(port=self.tor_mgmt_port, password=self.tor_mgmt_password) as controller:
                if not controller.is_newnym_available:
                    time.sleep(controller.get_newnym_wait())
                controller.signal(Signal.NEWNYM)
                # wait for the change to apply
                time.sleep(controller.get_newnym_wait())
        finally:
            # set proxy to old state
            logging.debug(
                'Resetting to previous proxy-state [{}]'.format(
                    'enabled' if previous_proxy_state else 'disabled'
                )
            )
            self.enable_proxy()
            self.lock.release()
        # print our new ip address
        try:
            logging.info('ip-address renewed: {}'.format(
                requests.get('http://ipinfo.io/ip', timeout=2).text.replace('\n', ''))
            )
        except ReadTimeout:
            logging.info('ip-address renewed.')
        except ConnectionError:
            self.renew_connection()
