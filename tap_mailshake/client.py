import base64
import re
import backoff
import requests
# from requests.exceptions import ConnectionError
from singer import metrics, utils
import singer

LOGGER = singer.get_logger()
API_VERSION = '2017-04-01'


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class MailshakeError(Exception):
    pass


class MailshakeBadRequestError(MailshakeError):
    pass


class MailshakeUnauthorizedError(MailshakeError):
    pass


class MailshakeRequestFailedError(MailshakeError):
    pass


class MailshakeNotFoundError(MailshakeError):
    pass


class MailshakeMethodNotAllowedError(MailshakeError):
    pass


class MailshakeConflictError(MailshakeError):
    pass


class MailshakeForbiddenError(MailshakeError):
    pass


class MailshakeUnprocessableEntityError(MailshakeError):
    pass


class MailshakeInternalServiceError(MailshakeError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: MailshakeBadRequestError,
    401: MailshakeUnauthorizedError,
    402: MailshakeRequestFailedError,
    403: MailshakeForbiddenError,
    404: MailshakeNotFoundError,
    405: MailshakeMethodNotAllowedError,
    409: MailshakeConflictError,
    422: MailshakeUnprocessableEntityError,
    500: MailshakeInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, MailshakeError)


def raise_for_error(response):
    LOGGER.error('ERROR {}: {}, REASON: {}'.format(response.status_code,
                                                   response.text, response.reason))
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Mailshake has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                message = '%s: %s' % (response.get('error', str(error)),
                                      response.get('message', 'Unknown Error'))
                error_code = response.get('status')
                ex = get_exception_for_error_code(error_code)
                raise ex(message)
            else:
                raise MailshakeError(error)
        except (ValueError, TypeError):
            raise MailshakeError(error)


class MailshakeClient(object):
    """MailshakeClient"""

    # pylint: disable=too-many-instance-attributes
    # Eight is reasonable in this case.

    def __init__(self,
                 api_key,
                 user_agent=None):
        self.__api_key = api_key
        self.base_url = "https://api.mailshake.com/{}".format(
            API_VERSION)
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__verified = False

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    @utils.ratelimit(1, 1.2)
    def check_access(self):
        if self.__api_key is None:
            raise Exception('Error: Missing api_key in tap_config.json.')
        headers = {}
        endpoint = 'me'
        url = '{}/{}'.format(self.base_url, endpoint)
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/json'
        headers['Authorization'] = 'Basic ' + self.__api_key
        response = self.__session.get(
            url=url,
            headers=headers)
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            return False
        return True

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    @utils.ratelimit(1, 3)
    def request(self, method, path=None, url=None, json=None, version=None, **kwargs):
        """Perform HTTP request"""

        # pylint: disable=too-many-branches
        # Eight is reasonable in this case.

        # if not self.__verified:
        #     self.__verified = self.check_access()

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Accept'] = 'application/json'
        kwargs['headers']['Authorization'] = 'Basic ' + self.__api_key

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method=method,
                url=url,
                json=json,
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        # pagination details (nextToken) are returned in the body
        next_token = None
        response_body = response.json()
        if response_body.get('nextToken') != "":
            next_token = response_body.get('nextToken')

        return response_body, next_token

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
