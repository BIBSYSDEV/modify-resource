from classes.RequestHandler import RequestHandler
from common.constants import Constants


def handler(event, context):
    if event is None:
        raise ValueError(Constants.ERROR_MISSING_EVENT)
    else:
        request_handler = RequestHandler()
        return request_handler.handler(event, context)
