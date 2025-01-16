import logging
import requests
from os import environ
from .file_system_cache import FileSystemCache
from geoseeq.utils import load_auth_profile
from geoseeq.constants import DEFAULT_ENDPOINT


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

class GeoseeqTimeoutError(GeoseeqGeneralError):
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
        self._verify = self._set_verify()
        self.sess = self._new_session()
        self.auth_required = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def check_auth_required(self):
        """Raise an error if authentication is required but not provided.
        
        This is only a soft barrier. This is not a security measure.
        """
        if self.auth_required and not self.auth:
            raise GeoseeqForbiddenError("Authentication is required but not provided. See `geoseeq config` to set auth.")
        
    def set_auth_required(self, value=True):
        self.auth_required = value
        self.check_auth_required()
        return self

    def close(self):
        self.sess.close()

    def _new_session(self):
        if hasattr(self, 'sess') and self.sess:
            self.sess.close()
        sess = requests.Session()
        sess.headers = self.headers
        sess.auth = self.auth
        sess.verify = self._verify
        return sess
    
    def new_session(self):
        return self._new_session()

    def _set_verify(self):
        try:
            val = environ['GEOSEEQ_NO_SSL_VERIFICATION']
            if val.lower() == 'true':
                return False
            if val.lower() == 'false':
                return True
            return val
        except KeyError:
            return True


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
        self.sess = self._new_session()

    def _handle_response(self, response, json_response=True):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                raise GeoseeqForbiddenError(e, response.content)
            if response.status_code == 404:
                raise GeoseeqNotFoundError(e, response.content)
            if response.status_code == 500:
                raise GeoseeqInternalError(e, response.content)
            if response.status_code == 504:
                raise GeoseeqTimeoutError(e, response.content)
            raise GeoseeqOtherError(e, response.content)
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
        self.check_auth_required()
        logger.debug(f"Sending GET request. {d}")
        response = self.sess.get(f"{self.endpoint_url}/{url}")
        resp = self._handle_response(response, **kwargs)
        return resp

    def post(self, url, json={}, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth, json=json)
        self.check_auth_required()
        logger.debug(f"Sending POST request. {d}")
        response = self.sess.post(
            f"{self.endpoint_url}/{url}",
            json=json
        )
        return self._handle_response(response, **kwargs)

    def put(self, url, json={}, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth, json=json)
        self.check_auth_required()
        logger.debug(f"Sending PUT request. {d}")
        response = self.sess.put(
            f"{self.endpoint_url}/{url}",
            json=json,
        )
        return self._handle_response(response, **kwargs)

    def patch(self, url, json={}, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth, json=json)
        self.check_auth_required()
        logger.debug(f"Sending PATCH request. {d}")
        response = self.sess.patch(
            f"{self.endpoint_url}/{url}",
            json=json,
        )
        return self._handle_response(response, **kwargs)

    def delete(self, url, json={}, url_options={}, **kwargs):
        url = self._clean_url(url, url_options=url_options)
        d = self._logging_info(url=url, auth_token=self.auth)
        self.check_auth_required()
        logger.debug(f"Sending DELETE request. {d}")
        response = self.sess.delete(f"{self.endpoint_url}/{url}", json=json)
        logger.debug(f"DELETE request response:\n{response}")
        return self._handle_response(response, json_response=False, **kwargs)

    @classmethod
    def load_profile(cls, profile=""):
        """Return a knex authenticated with a profile."""
        endpoint, token = load_auth_profile(profile)
        knex = cls(endpoint)
        if token:
            knex.add_api_token(token)
        return knex
    

def with_knex(func):
    def wrapper(*args, **kwargs):
        # check if any of the arguments are a knex instance
        logger.debug(f"Checking for knex in args: {args}, kwargs: {kwargs}")
        any_knex = any([isinstance(arg, Knex) for arg in args])
        if any_knex:
            logger.debug("knex found in args args: {args}, kwargs: {kwargs}")
            return func(*args, **kwargs)
        else:
            logger.debug("knex not found in args args: {args}, kwargs: {kwargs}")
            varnames = [
                varname for varname in func.__code__.co_varnames
                if varname != "knex"
            ]
            kwargs.update(zip(varnames, args))
            if "knex" not in kwargs:
                logger.debug("knex not found in kwargs args: {args}, kwargs: {kwargs}")
                profile = kwargs.pop("profile", "")
                kwargs['knex'] = Knex.load_profile(profile=profile)
            # reorder kwargs to match the function signature
            reordered = {
                key: kwargs[key] for key in func.__code__.co_varnames
                if key in kwargs
            }
            return func(**reordered)
    return wrapper
        