import logging
import logging.handlers

from json import JSONDecodeError
from abc import ABC, ABCMeta, abstractmethod
from typing import Dict
from urllib.parse import urlparse
from time import sleep
from pydash.arrays import chunk

# Third Party
import requests
from requests import HTTPError
from requests.sessions import Session
from requests.exceptions import Timeout
from requests.exceptions import ConnectionError as ReqConnError
from tqdm import tqdm

SLEEP_TIME = 60
RETRIES = 1

class ODataClient(Logger):
    __redis_cache = RedisCache()
    """A shared Redis cache optionally used to cache the results of data queries done by the Client"""

    def __init__(self, username, password):
        """
        Initialize the object with the given username/password combo for Anthology access.

        Arguments:
            username {str} -- A CNS username credential
            password {str} -- User's password credential
        """
        super().__init__()
    
        self.auth(username, password)

        self._session: Session = None

        self.query_cache_timeout_hours = 48
        """Default number of hours to cache query requests, if the cache is enabled"""

        self.cache_query_requests = False
        """Determines if all `get()` requests should automatically be cached"""

        # For local testing only
        # self.cache_query_requests = True

    def auth(self, username: str, password: str):
        with requests.Session() as session:
            self._session = session
            self._session.auth = (username, password)

    def get(self, uri: str, use_cache=None) -> dict:
        """
        Transmit HTTP/GET request to the specified URI, retrying the request upon failure

        Arguments:
            uri {str} -- The URI to access via HTTP/GET
            [use_cache] {bool} -- Determines if this specific request should be cached (overrides the class-level `cache_query_requests` property)

        Raises:
            RuntimeError: If access is denied during the current running operation an error is
            logged and this exception is raised.

        Returns:
            dict -- The response from the server.  If the request fails, times out, or the
            information cannot be decode (e.g. a webpage is returned displaying an error), then
            NoneType is returned
        """
        # Cache the results if the cache is enabled at the class or function level
        # If caching is disabled at the function level, it will take precedence over the class-level setting
        _use_cache = self.cache_query_requests or use_cache and use_cache != False

        if _use_cache:
            cached_value = ODataClient.__redis_cache.retrieve(uri)
            if cached_value:
                return cached_value

        current_retries = RETRIES
        while True:
            try:
                res = self._session.get(uri.strip('/'))
                res.raise_for_status()

                if _use_cache:
                    ODataClient.__redis_cache.store(uri, res.json(), self.query_cache_timeout_hours)
                return res.json()
            except Exception as e:
                if current_retries:
                    self.logger.info(f'GET request failed. Retrying after {SLEEP_TIME} seconds...')
                    sleep(SLEEP_TIME)
                    current_retries -= 1
                    continue
                try:
                    raise e
                except (ReqConnError, Timeout, JSONDecodeError) as exc:
                    self.logger.error(exc)
                    self.logger.error(f'{type(exc).__name__} occurred while making OData GET {uri}')
                    break
                except HTTPError as exc:
                    self.logger.error(exc)
                    try:
                        self.logger.error(res.json())
                    except:
                        self.logger.error(res.content)
                    if res.status_code == 403:
                        self.logger.error(f'Unauthorized. Ensure \'{self._session.auth[0]}\' has CNS permissions and the password in config.ini is correct')
                        # Do NOT raise Runtime error. Stops code even if access may be available for other parts of the code. 
                        #raise RuntimeError('Access was denied')
                    break

        return None

    def get_paged(self, url: str, page_size=100, use_cache=None) -> Dict[str, list]:
        """
        Get results from CNS using paged queries to get large result sets.
        The page size can be configured, but default to getting 50 records at a time.
        This will display a progress bar when run in the command line indicating the progress of the paged requests.

        If any of the individual requests return `None`, this method will also return `None`.

        Arguments:
            - url {str} -- The full URL for which to get results, not including any paging parameters ($top, $skip, or $count)
            - [page_size] {int} -- Optionally set the page size to a number other than 50
            - [use_cache] {bool} -- Determines if this specific request should be cached (overrides the class-level `cache_query_requests` property)

        Returns:
            dict -- A dictionary in the format of `{'value': []}` to remain consistent with the base `get` method
        """
        # Determine if there is currently a query string on the URL
        # If there is, the paging query will be added to the end (appended with an '?')
        # Otherwise, the paging query will be the only query string (appended with an '&')
        parsed_url = urlparse(url)
        query_separator = '?' if parsed_url.query == '' else '&'

        # Importing inside the function, otherwise causes a circular import
        from utilities import parse_query
        # Parse the query string into a dictionary manually since the behavior of the `parse_qs`
        #  method is inconsistent between versions of Python
        query = parse_query(parsed_url)

        # Throw an error if the query string contains the $top, $skip, or $count parameters, as these are not supported with paging
        if '$top' in query or '$skip' in query or '$count' in query:
            error_message = f'URL cannot include $top, $skip, or $count in the query string when using get_paged() - called with "{url}"'
            self.logger.error(error_message)
            raise ValueError(error_message)

        results = []
        total_records = -1
        
        # Continue making requests until all records have been retrieved, then return them
        with tqdm() as progress_bar:
            while total_records == -1 or len(results) < total_records:
                response = self.get(f'{url}{query_separator}$top={page_size}&$skip={len(results)}&$count=true', use_cache=use_cache)
                # If any of the individual requests come back without a successful response, exit and return None
                if response is None:
                    return None
                # If there were no more results, break out of the loop
                # This is necessary if the total number of records decreases after the initial request is made
                # This can happen with requests for a large amount of data that changes frequently, or if the requests were cached
                # If the number of records increases, some records may be left out, but this should not be as much of a problem
                if len(response['value']) == 0:
                    break
                results += response['value']
                if total_records == -1:
                    total_records = response['@odata.count']
                    progress_bar.total = total_records
                progress_bar.update(len(response['value']))

        return {'value': results}
    
    def get_chunked(self, url:str, filter:str, to_chunk_list:list, chunk_size=20) -> list:
        """
        Generates a list of data from OData based on a large list that is 'chunked' into separate
        filtered calls. Adds the '$filter' paramter if there is not already one. 

        Arguments:
            - url: The url to be queried
            - filter: The filter that will used in the query chunks. The filter should include "{}"
            as a place holder for where the filter value should go. 
                - Examples:
                    - "Id eq {}" = "Id eq 1234"
                    - "contains(Code, '{}')" = "contains(Code, '1234')"
                - #### Single quotes should be used if the filter contains a string and quotations are needed in the query.
            - to_chunk_list: The list of values to be included in the chunked filters
            - chunk_size: The size of the chunk to be used in the filters. Default is 20.

        Returns:
            - response_list: A consolidated list of responses from the filtered queries
        """
        # Importing inside the function, otherwise causes a circular import
        from utilities import parse_query

        # Parse the query string into a dictionary manually since the behavior of the `parse_qs`
        #  method is inconsistent between versions of Python
        parsed_url = urlparse(url)
        query = parse_query(parsed_url)

        scheme = parsed_url.scheme
        net_loc = parsed_url.netloc
        path = parsed_url.path

        base_url = f"{scheme}://{net_loc}{path}?"

        response_list = []
        for list_chunk in tqdm(chunk(to_chunk_list, chunk_size), desc='Gathering data chunks'):
            filters = [filter.replace('{}', str(chunk_value)) for chunk_value in list_chunk]

            query_copy = query.copy()
            if '$filter' in query_copy.keys():
                query_copy['$filter'] = f"({query_copy['$filter']}) and ({' or '.join(filters)})"
            else:
                query_copy['$filter'] = f"{' or '.join(filters)}"

            new_str = ''
            first_key = True
            for k, v in query_copy.items():
                if first_key:
                    new_str += f'{k}={v}'
                    first_key = False
                else:
                    new_str += f'&{k}={v}'

            response = self.get(f"{base_url}{new_str}")
            
            # If any of the responses have an error and return None, the function will return None
            if not response:
                return None
            response_list += response['value']
            
        return response_list
    
    def post(self,
        uri: str,
        command: str = '',
        payload: dict = None,
        headers: dict = None,) -> dict:
        """
        Transmit HTTP/POST request to the specified URI

        Arguments:
            uri {str} -- The URI to access via HTTP/POST

        Keyword Arguments:
            command {str} -- A command suffix.  Same as uri + '/suffix' (default: {''})
            payload {dict} -- The payload to send to the URI (default: {None})

        Raises:
            RuntimeError: If access is denied during the current running operation an error is
            logged and this exception is raised.

        Returns:
            dict -- The response from the server.  If the request fails, times out, or the
            information cannot be decode (e.ge a webpage is returned displaying an error), then
            NoneType is returned
        """
        current_retries = RETRIES
        while True:
            try:
                res = self._session.post(f"{uri.strip('/')}/{command}",
                    headers=headers,
                    json=payload,
                    timeout=30.0)
                res.raise_for_status()
                return res.json()
            except Exception as e:
                if current_retries:
                    self.logger.info(f'POST request failed. Retrying after {SLEEP_TIME} seconds...')
                    sleep(SLEEP_TIME)
                    current_retries -= 1
                    continue
                try:
                    raise e
                except HTTPError as exc:
                    self.logger.error(f'HTTP Error during POST: {exc}'
                        f'\n{type(exc).__name__} occurred while making POST {uri}/{command}')
                    self.logger.info(f'Payload: {payload}')
                    self.logger.info(f'Response: {res}\nResponse Body: {res.json()}')
                    if res.status_code == 403:
                        self.logger.error(f'Unauthorized. Ensure \'{self._session.auth[0]}\' has CNS '
                            'permissions and the password in config.ini is correct')
                        raise RuntimeError('Access was denied')
                    break
                except Exception as exc:
                    self.logger.error(f'{type(exc).__name__} occurred while making POST {uri}/{command}: \n {exc}')
                    self.logger.info(f'Payload: {payload}')
                    break
                
        return None
