import logging

import requests

from ProxyManager import BaseProxyManager

import lxml.html as lhtml

BASE_URL_DETAILS = u''


class Crawler(object):
    def __init__(self, proxy_manager: BaseProxyManager = BaseProxyManager()):
        self.proxy_manager = proxy_manager
        self.user_agent = 'BuergeramtTermine'

    def set_user_agent(self, user_agent):
        self.user_agent = user_agent

    def get_headers(self):
        return {
            'User-Agent': self.user_agent
        }


class CalendarCrawler(Crawler):
    BASE_URL_CALENDAR = u'https://service.berlin.de/terminvereinbarung/termin/tag.php'
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
        calendar_links = []
        while True:
            try:
                response = requests.get(self.BASE_URL_CALENDAR, params=self.get_params(), headers=self.get_headers())
                if response.status_code == 429:
                    print('8', end='\n', flush=True)
                    self.proxy_manager.renew_connection()
                    continue
                html = response.text
                tree = lhtml.fromstring(html)
                calendar_links = tree.cssselect("td[class~='{}']>a".format(self.CSS_CLASS_RESERVABLE))
                if len(calendar_links) > 0:
                    if self.handle_calendar_links(calendar_links):
                        break
                print('.', end='', flush=True)
            except Exception:
                logging.warning('Could not parse resulting page.')
        return calendar_links

    def set_start_date(self, start_date):
        self.start_date = start_date

    def get_params(self):
        return {
            'Datum': self.start_date,
            'dienstleister[]': self.param_service_ids,
            'anliegen[]': self.param_request,
        }

    @staticmethod
    def handle_calendar_links(calendar_links):
        logging.info('Found {} links'.format(len(calendar_links)))
        for link in calendar_links:
            logging.debug('{ahref}'.format(ahref=link.attrib['href']))
