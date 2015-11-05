import logging
import json


def enable_filter():
    logging.getLogger().addFilter(JsonFilter)
    for logger in logging.Logger.manager.loggerDict:
        logging.getLogger(logger).addFilter(JsonFilter)


class JsonFilter(object):
    @staticmethod
    def filter(record: logging.LogRecord):
        record.msg = json.dumps(record.msg)
        return True
