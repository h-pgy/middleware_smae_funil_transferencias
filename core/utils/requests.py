from requests import session
from http import HTTPStatus
from requests.exceptions import HTTPError
import time

from core.exceptions import UpstreamError

RETRY_CODES = [
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
]

MAX_RETRIES=3


def max_retries_bytes_request(session:session, url:str, max_retries:int)->bytes:

    for n in range(max_retries):
            try:
                with session.get(url) as response:
                    response.raise_for_status()
                    return response.content
            except HTTPError as exc:
                code = exc.response.status_code
                
                if code in RETRY_CODES:
                    # retry after n seconds
                    time.sleep(n)
                    continue
                raise
    else:
        raise UpstreamError(f'Max retries exceeded for url: {url}. HTTP Exception: {exc}. Status code: {code}')


