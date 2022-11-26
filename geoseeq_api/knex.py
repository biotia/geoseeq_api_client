import logging

import requests

from .file_system_cache import FileSystemCache

# DEFAULT_ENDPOINT = "https://portal.geoseeq.com"
DEFAULT_ENDPOINT = "https://dev1.geoseeq.com"


logger = logging.getLogger("geoseeq_api")  # Same name as calling module
logger.addHandler(logging.NullHandler())  # No output unless configured by calling program


def clean_url(url):
    if url[-1] == "/":
        url = url[:-1]
    return url


class TokenAuth(requests.auth.AuthBase):
    """Attaches MetaGenScope Token Authentication to the given Request object."""

    def __init__(self, token):
        self.token = token

    def __call__(self, request):
        """Add authentication header to request."""
        request.headers["Authorization"] = f"Token {self.token}"
        return request

    def __str__(self):
        """Return string representation of TokenAuth."""
        return self.token


class GeoseeqGeneralError(requests.exceptions.HTTPError):
    pass


class GeoseeqNotFoundError(GeoseeqGeneralError):
    pass


class GeoseeqForbiddenError(GeoseeqGeneralError):
    pass


class GeoseeqInternalError(GeoseeqGeneralError):
    pass


class GeoseeqOtherError(GeoseeqGeneralError):
    pass


class Knex:
    def __init__(self, endpoint_url=DEFAULT_ENDPOINT):
        self.endpoint_url = endpoint_url
        self.endpoint_url += "/api"
        self.auth = None
        self.headers = {"Accept": "application/json"}
        self.cache = FileSystemCache()

    def _logging_info(self, **kwargs):
        base = {"endpoint_url": self.endpoint_url, "headers": self.headers}
        base.update(kwargs)
        return base

    def _clean_url(self, url, url_options={}):
        url = clean_url(url)
        url = url.replace(self.endpoint_url, "")
        if url[0] == "/":
            url = url[1:]
        if url_options:
            opts = [f"{key}={val}" for key, val in url_options.items()]
            opts = "&".join(opts)
            if "?" in url:
                url += "&" + opts
            else:
                url += "?" + opts
        return url

    def add_api_token(self, token):
        self.auth = TokenAuth(token)

    def _handle_response(self, response, json_response=True):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                raise GeoseeqForbiddenError(e)
            if response.status_code == 404:
                raise GeoseeqNotFoundError(e)
            if response.status_code == 500:
                raise GeoseeqInternalError(e)
            raise GeoseeqOtherError(e)
        except Exception:
            logger.debug(f"Request failed. {response}\n{response.content}")
            raise
        if json_response:
            return response.json()
        return response

    def instance_code(self):
        return "gsr1"  # TODO

    def get(self, url, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth)
        logger.debug(f"Sending GET request. {d}")
        response = requests.get(
            f"{self.endpoint_url}/{url}",
            headers=self.headers,
            auth=self.auth,
        )
        return self._handle_response(response, **kwargs)

    def post(self, url, json={}, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth, json=json)
        logger.debug(f"Sending POST request. {d}")
        response = requests.post(
            f"{self.endpoint_url}/{url}", headers=self.headers, auth=self.auth, json=json
        )
        return self._handle_response(response, **kwargs)

    def put(self, url, json={}, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth, json=json)
        logger.debug(f"Sending PUT request. {d}")
        response = requests.put(
            f"{self.endpoint_url}/{url}", headers=self.headers, auth=self.auth, json=json
        )
        return self._handle_response(response, **kwargs)

    def patch(self, url, json={}, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth, json=json)
        logger.debug(f"Sending PATCH request. {d}")
        response = requests.patch(
            f"{self.endpoint_url}/{url}", headers=self.headers, auth=self.auth, json=json
        )
        return self._handle_response(response, **kwargs)

    def delete(self, url, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth)
        logger.debug(f"Sending DELETE request. {d}")
        response = requests.delete(
            f"{self.endpoint_url}/{url}",
            headers=self.headers,
            auth=self.auth,
        )
        logger.debug(f"DELETE request response:\n{response}")
        return self._handle_response(response, json_response=False, **kwargs)
