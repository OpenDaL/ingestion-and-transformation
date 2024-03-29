# -*- coding: utf-8 -*-
"""
Async IO Harvesters for data

Copyright (C) 2021  Tom Brouwer

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import asyncio
import datetime
import logging
from pathlib import Path
import json
import functools
import ssl
import re
import math
import cloudscraper
import copy
import time
from typing import Union, Any, Iterator
import os

import xmltodict

from . import settings, dataio, exceptions, _common

# To fix occasional http 400 errors: https://github.com/aio-libs/aiohttp/issues/5443
os.environ['AIOHTTP_NO_EXTENSIONS'] = '1'
import aiohttp  # noqa:E402

DEFAULT_TIMEOUT = {
    "total": 300,
    "connect": 120,
    "sock_connect": 120,
    "sock_read": 120
}


class Harvester:
    """
    A base harvester class, used by specific harvesters to inherit from.
    """
    NO_RETRY_CODES = {
        401: 'Unauthorized',
        402: 'Payment Required',
        403: 'Forbidden',
        407: 'Proxy Authentication Required',
        410: 'Gone'
    }

    def __init__(
            self, *, id_: str, api_url: str, output_path: Union[Path, str],
            download_delay: int = 1, retry_delays: list[int] = settings.DELAYS,
            cache_size: int = 1000, timeout: dict = DEFAULT_TIMEOUT,
            max_size: int = 50000000, certfile: str = None,
            encoding: str = None, invalid_xml_regex: str = None,
            rows: int = None, cloudflare_bypass: bool = False,
            user_agent: str = None, is_single_request: bool = False,
            allow_repeating_data: bool = False, additional_headers: dict = None
            ):
        """
        Initialize the Harvester Instance

        Args:
            id_:
                The unique identifier for the portal that's harvested (used in
                output file names and in logging)
            api_url:
                The base url of the API endpoint
            output_path:
                The path to output the data retrieved from the API
            download_delay:
                The wait time in seconds between two consecutive requests
            retry_delays:
                The delays when retrying (in seconds), the number of delays
                will be the number of retries for each request
            cache_size:
                The number of resource to cache in memory, before flushing to
                disk
            timeout:
                Timeout parameters that are passed as **kwargs to
                aiohttp.ClientTimeout
            max_size:
                The maximum size of total returned results AND the maximum size
                of a request in Bytes for harvesters that use the get_chunked()
                method. If the harvested data exceeds this size, an
                exceptions.TooMuchDataError is raised
            certfile:
                If the SSL settings for a server are not correctly configured,
                you can define a name of a local certfile here (Is
                automatically prefixed with settings.CERT_DIR). CA Bundle for a
                website can be downloaded from https://www.ssllabs.com/ssltest/
            encoding:
                The encoding of the response. If None, the aoihttp package will
                attempt auto-detection
            invalid_xml_regex:
                The regex pattern to escape invalid html characters, parts
                matching the regex are replaced by '�' (XML Harvesters Only)
            rows:
                The number of results to return per page (Only for harvesters
                that can control this)
            cloudflare_bypass:
                Whether to check for, and try to bypass cloudflare protection,
                in case a 503 status is returned
            user_agent:
                User agent string. If specified it overrides the default
                aiohttp user agent
            is_single_request:
                If True, the delays and checks between yielding new data is
                skipped, since it's all from one request
            allow_repeating_data:
                If True, the check for repeating data is disabled
            additional_headers:
                Update the Session headers using this dict
        """
        self.download_delay = download_delay
        self.retry_delays = retry_delays
        self.retries_left = len(retry_delays)
        self.timeout = timeout
        self.session = None  # Has to be initialized in async function (.run())
        self.api_url = api_url
        self.id = id_
        self.output_path = output_path
        self.write_mode = 'w'
        self.cache_size = cache_size
        self.logger = None
        self.total = 0
        self.max_size = max_size
        self.encoding = encoding
        self.rows = rows
        self.cloudflare_bypass = cloudflare_bypass
        self.user_agent = user_agent
        self.is_single_request = is_single_request
        self.allow_repeating_data = allow_repeating_data
        self.additional_headers = additional_headers
        self.last_req_finish_time = 0
        # In case failure occurs, but scraping is continued, use
        # self.has_failed, to still make sure the .INCOMPLETE is added to the
        # file
        self.has_failed = False
        self.cert_loc = None
        if certfile is not None:
            self.cert_loc = self._get_cert_loc(certfile)
        self.invalid_xml_pattern = re.compile(invalid_xml_regex) if\
            invalid_xml_regex is not None else None
        # Set the uid of the output, based on id and current time
        id_plus_time = id_ + '_' + datetime.datetime.utcnow().isoformat(
            timespec='seconds'
        ) + 'Z'
        self.output_uid = id_plus_time.replace(':', '-')  # Filesystem compat.
        self.total_logged = False

    def _get_cert_loc(self, filename: str) -> Path:
        """
        Gets the location of the certificate file for the given filename
        """
        return Path(settings.CERT_DIR, filename)

    def _prerun_init(self):
        """
        Initialization called by run function.

        Expensive stuff is set at runtime, otherwise it's costly to store the
        instances in memory. also, aiohttp things should be in same task
        """
        self.logger = logging.getLogger('{}.{}'.format(
            self.__class__.__name__,
            self.id
        ))
        timeout = aiohttp.ClientTimeout(**self.timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)
        self.logger.info('Harvester started')

        if self.cert_loc is not None:
            self.ssl = ssl.create_default_context(cafile=self.cert_loc)
        else:
            self.ssl = None

        if self.user_agent is not None:
            self.session._default_headers['User-Agent'] = self.user_agent

        if self.additional_headers is not None:
            self.session._default_headers.update(self.additional_headers)

    async def run(self):
        """
        Run the harvester. At the end of a run
        the session is closed
        """
        self._prerun_init()
        # Create an empty cache and start async iteration of results
        out_data = []
        retries = len(self.retry_delays)
        prev_result = {}
        try:
            # Already create the file, makes it easy to spot incomplete ones
            self.output([])
            async for results in self.request_data():
                out_data.extend(results)
                # Check if there is recurring data
                if not self.is_single_request and len(results) > 0:
                    new_result = results[0]
                    if (not self.allow_repeating_data) and\
                            (new_result == prev_result):
                        raise exceptions.UnexpectedDataError(
                            'New data equals previous'
                        )
                    # Set prev_result for check in next iteration
                    prev_result = new_result
                self.retries_left = retries  # Reset retries for next iteration
                # Output data if the queue lenght is exceeded
                in_cache = len(out_data)
                if in_cache >= self.cache_size:
                    self.output(out_data)
                    out_data = []
                    self.total += in_cache
                    if self.total > self.max_size:
                        raise exceptions.TooMuchDataError(
                            'More items harvested then max_size parameter'
                        )
                    self.log_download_progress(self.total)

            # Output remaining data after for loop
            in_cache = len(out_data)
            self.total += in_cache
            if self.total > 0:
                self.logger.info(
                    'Harvesting finished, {} items downloaded'.format(
                        self.total
                    )
                )
            else:
                raise exceptions.NoResultsError('Zero items were harvested')

            # If self.has_failed=True, some error was handled by the Harvester,
            # but results are still incomplete:
            if self.has_failed:
                self.output(out_data)
            else:
                # Only if nothing failed, file is renamed to remove the
                # .INCOMPLETE that's appended to the filename
                self.output(out_data, last=True)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await self.session.close()
            self.logger.error(
                'The Harvester was stopped by KeyboardInterrupt'
            )
            await asyncio.sleep(5)  # Wait for all sessions to close
            raise
        except Exception:
            # Make sure the last data is written, before logging exception
            self.output(out_data)
            self.logger.exception(
                'The Harvester has stopped with the following exception:'
            )

        await self.session.close()

    def output(self, data: Any, last: bool = False):
        """
        Writes the output to a file. If last is True, the file is renamed to
        remove the .INCOMPLETE suffix
        """
        out_loc = Path(self.output_path, self.output_uid + '.jsonl.INCOMPLETE')
        dataio.savejsonlines(data, out_loc, mode=self.write_mode)
        self.write_mode = 'a'
        if last:
            # Rename to indicate completion
            rename_to = Path(self.output_path, self.output_uid + '.jsonl')
            dataio.rename_if_exists(out_loc, rename_to)

    def retry_on_fail(slf=None, retry_on=()):
        """
        Async decorator (factory) to retry functions when they fail

        Arguments:
            slf:
                Placeholder for if self is passed
            retry_on:
                Exception, or tuple of exceptions handled in the try/except

        Note: If used at the top level of the class, self is not passed, so its
        taken from the first wrapper parameter, since it's passed to the
        function. When this is used on lower level functions (e.g. function
        defined in method), self will be passed to this. This means that at the
        same time it is not passed to the underlying functions.
        """
        def inner_decorator(func):
            @functools.wraps(func)
            async def wrapper(self, *args, **kwargs):
                # Use number of retries from class, since different errors can
                # be generated within the same iterations. Multiple functions
                # may be responsible for decrements
                while self.retries_left > 0:
                    try:
                        # See note about 'self' above
                        if slf is not None:
                            return await func(*args, **kwargs)
                        else:
                            return await func(self, *args, **kwargs)
                    except retry_on as e:
                        delay = self.retry_delays[- self.retries_left]
                        self.logger.warning(
                            'Function {} with args: {} and kwargs: {} raised'
                            ' {}: {}. Retrying in {} seconds'.format(
                                func.__name__,
                                str(args),
                                str(kwargs),
                                e.__class__.__name__,
                                str(e),
                                delay
                            )
                        )
                        await asyncio.sleep(delay)
                        self.retries_left -= 1
                    except Exception as e:
                        # Catch-all, to log the function properties before
                        # logging the error. Helps in debugging
                        self.logger.warning(
                            '{} in Function {} with args: {} and '
                            'kwargs: {}.'.format(
                                e.__class__.__name__,
                                func.__name__,
                                str(args),
                                str(kwargs),
                            )
                        )
                        raise
                else:
                    # See note about 'self' above
                    try:
                        if slf is not None:
                            return await func(*args, **kwargs)
                        else:
                            return await func(self, *args, **kwargs)
                    except Exception as e:
                        # Catch-all, to log the function properties before
                        # logging the error. Helps in debugging
                        self.logger.warning(
                            '{} in Function {} with args: {} and '
                            'kwargs: {}.'.format(
                                e.__class__.__name__,
                                func.__name__,
                                str(args),
                                str(kwargs),
                            )
                        )
                        raise

            # See note about 'self' above
            if slf is not None:
                return functools.partial(wrapper, slf)
            else:
                return wrapper

        return inner_decorator

    def _is_request(func):
        """
        Async decorator for request functions, for which the download_delay
        needs to be implemented

        NOTE: Cannot be used in nested functions inside methods
        """
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            time_elapsed = time.perf_counter() - self.last_req_finish_time
            wait_seconds = self.download_delay - time_elapsed
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)

            try:
                result = await func(self, *args, **kwargs)
            finally:
                self.last_req_finish_time = time.perf_counter()

            return result

        return wrapper

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Override this function, to have a harvester yield data.

        Yields:
            Typically,this function would yield the resources returned by one
            API request as a list of dictionaries
        """
        raise NotImplementedError(
            'Please override this function in child classes'
        )

    async def bypass_cloudflare(
        self, response_headers: dict, response_text: str, user_agent: str,
        *request_args, **request_kwargs
            ) -> Union[str, None]:
        """
        Try to bypass cloudflare and redo request

        Arguments:
            response_headers:
                The response headers that were returned
            response_text:
                The text from the response
            user_agent:
                The user agent used in the initial request
            *request_args:
                Arguments passed to the request function
            **request_kwargs:
                Keyword Arguments passed to the request function

        Returns:
            In case of a succesfull bypass, the result string is returned.
        """
        # If it's not a cloudflare challenge, or there was already and attempt
        # to bypass, do nothing
        if (not (
            response_headers.get('Server', '').startswith('cloudflare')
            and 'jschl_vc' in response_text
            and 'jschl_answer' in response_text
                )):
            self.logger.debug('Not a CloudFlare challenge page')
            return None

        self.logger.info('Found CloudFlare challenge, trying to bypass')

        # Get the cloudflare cookies, TODO: make async
        cloudflare_cookies, _ = cloudscraper.get_tokens(
            request_args[0],  # request URL
            headers={'User-Agent': user_agent}
        )

        # Set cookies and user agent for session
        self.session._cookie_jar.update_cookies(cloudflare_cookies)
        self.session._default_headers['User-Agent'] = user_agent
        self.cloudflare_bypass = False  # Prevent loops

        self.logger.info('Bypassed CloudFlare protection')

        # Redo request and return result
        result = await self.get_request(
            *request_args, **request_kwargs
        )

        return result

    @retry_on_fail(retry_on=(aiohttp.ClientError, exceptions.InvalidStatusCode,
                             asyncio.TimeoutError))
    @_is_request  # After retry, since it should be enforced if retry delay=0
    async def get_request(self, *args, **kwargs) -> str:
        """
        aiohttp get request with error handling. See
        aiohttp.ClienSession.get for documentation

        Args:
            *args:
                Arguments for aiohttp.ClientSession.get
            **kwargs:
                Keyword Arguments for aiohttp.ClientSession.get
        Returns:
            Response.text
        """
        self.logger.debug(
            'GET Request: {}, {}'.format(
                str(args),
                str(kwargs),
            )
        )
        async with self.session.get(*args, **kwargs, ssl=self.ssl) as resp:
            status = resp.status
            result = await resp.text(errors='replace', encoding=self.encoding)

            if status == 200:
                return result
            else:
                if status in self.NO_RETRY_CODES:
                    raise exceptions.NonRetryableHTTPStatus(
                        status,
                        self.NO_RETRY_CODES[status]
                    )
                if (status == 503 and self.cloudflare_bypass):
                    result = await self.bypass_cloudflare(
                        resp.headers,
                        result,
                        resp.request_info.headers.get('User-Agent'),
                        *args,
                        **kwargs
                    )
                    if result is not None:
                        return result

                raise exceptions.InvalidStatusCode(status,
                                                   text_response=result)

    @retry_on_fail(retry_on=(aiohttp.ClientError, exceptions.InvalidStatusCode,
                             asyncio.TimeoutError))
    @_is_request  # After retry, since it should be enforced if retry delay=0
    async def post_request(self, *args, **kwargs) -> str:
        """
        aiohttp post request with error handling. See
        aiohttp.ClienSession.post for documentation

        Arguments:
            *args:
                Arguments for aiohttp.ClientSession.post
            **kwargs:
                Keyword Arguments for aiohttp.ClientSession.post

        Returns:
            Response.text
        """
        self.logger.debug(
            'POST Request: {}, {}'.format(
                str(args),
                str(kwargs),
            )
        )
        async with self.session.post(*args, **kwargs, ssl=self.ssl) as resp:
            status = resp.status
            result = await resp.text(errors='replace', encoding=self.encoding)

            if status == 200:
                return result
            else:
                if status in self.NO_RETRY_CODES:
                    raise exceptions.NonRetryableHTTPStatus(
                        status,
                        self.NO_RETRY_CODES[status]
                    )
                if (status == 503 and self.cloudflare_bypass):
                    result = await self.bypass_cloudflare(
                        resp.headers,
                        result,
                        resp.request_info.headers.get('User-Agent'),
                        *args,
                        **kwargs
                    )
                    if result is not None:
                        return result

                raise exceptions.InvalidStatusCode(status,
                                                   text_response=result)

    @retry_on_fail(retry_on=(aiohttp.ClientError, exceptions.InvalidStatusCode,
                             asyncio.TimeoutError))
    @_is_request  # After retry, since it should be enforced if retry delay=0
    async def get_chuncked(self, *args, **kwargs) -> str:
        """
        Like get_request, except it gets data in chuncks of 1MB and checks if
        max_size is not exceeded
        """
        async with self.session.get(*args, **kwargs, ssl=self.ssl) as resp:
            status = resp.status

            # Get Data in chuncks, so it can be cancelled if max_size is
            # exceeded
            result = b""
            size = 0
            async for data in resp.content.iter_chunked(1024 * 1024):
                size += len(data)
                if size > self.max_size:
                    raise exceptions.TooMuchDataError(
                        'Single response size bigger than max_size in bytes'
                    )
                result += data

            # Get encoding, in this way it mimmics get_request
            if self.encoding is not None:
                encoding = self.encoding
            else:
                encoding = resp.get_encoding()

            # Like get_request, the result is a string
            result = result.decode(encoding, errors='replace')

            if status == 200:
                return result
            elif status in self.NO_RETRY_CODES:
                raise exceptions.NonRetryableHTTPStatus(
                    status,
                    self.NO_RETRY_CODES[status]
                )
            else:
                raise exceptions.InvalidStatusCode(status,
                                                   text_response=result)

    # Note this does not retry on fail, since it may keep on returning partial
    # results causing duplicates, and the decorators are only suited for
    # regular functions Please note that download delays are also not
    # implemented for this
    async def get_lines(self, *args, **kwargs) -> Iterator[str]:
        """
        Async generator that does a get requests, and yields the result line
        by line
        """
        async with self.session.get(*args, **kwargs, ssl=self.ssl) as resp:
            status = resp.status

            if status != 200:
                if status in self.NO_RETRY_CODES:
                    raise exceptions.NonRetryableHTTPStatus(
                        status,
                        self.NO_RETRY_CODES[status]
                    )
                else:
                    raise exceptions.InvalidStatusCode(status,
                                                       text_response='')

            # Get encoding like default get request
            if self.encoding is not None:
                encoding = self.encoding
            else:
                encoding = 'utf-8'  # Body is not yet ready

            async for line in resp.content:
                yield line.decode(encoding, errors='replace')

    @retry_on_fail(retry_on=(json.JSONDecodeError))
    async def get_json(
            self, *args, get_chuncked: bool = False, **kwargs
            ) -> Any:
        """
        Get data, and return the result as JSON.
        See aiohttp.ClienSession.get for parameter documentation

        Adds the 'get_chuncked' parameter, which indicates whether to use the
        get_chucked function, which checks response size (usefull for single
        large file downloads)
        """
        if not get_chuncked:
            text_response = await self.get_request(*args, **kwargs)
        else:
            text_response = await self.get_chuncked(*args, **kwargs)

        return json.loads(text_response)

    @retry_on_fail(retry_on=(json.JSONDecodeError))
    async def post_json(self, *args, **kwargs) -> Any:
        """
        Get data (POST Request), and return the response parsed from JSON.
        See aiohttp.ClienSession.post for parameter documentation
        """
        text_response = await self.post_request(*args, **kwargs)

        return json.loads(text_response)

    @retry_on_fail(retry_on=(xmltodict.expat.ExpatError))
    async def get_xml(
            self, *args, get_chuncked: bool = False, **kwargs
            ) -> Any:
        """
        Get XML data, and return the result as a dict. See
        aiohttp.ClienSession.get for parameter documentation

        Adds the 'get_chuncked' parameter, which indicates whether to use the
        get_chucked function, which checks response size (usefull for single
        large file downloads)
        """
        if not get_chuncked:
            text_response = await self.get_request(*args, **kwargs)
        else:
            text_response = await self.get_chuncked(*args, **kwargs)

        if self.invalid_xml_pattern:
            text_response = self.invalid_xml_pattern.sub('�', text_response)

        # Limit depth, to prevent recursionerrors at later point
        return _common.limit_depth(xmltodict.parse(text_response), 50)

    def log_download_progress(self, number: int):
        """
        Log the download progress
        """
        self.logger.info('Downloaded {} items'.format(number))

    def log_total(self, number: int):
        """
        Log the total number of entries (Only done on first time triggered)
        """
        if not self.total_logged:
            self.logger.info('Total number of entries {}'.format(number))
            self.total_logged = True


class CKAN3Harvester(Harvester):
    """
    An Async harvester for CKAN v3 APIs.
    """

    def __init__(
            self, *args, rows: int = 1000, custom_endpoint: str = None,
            throttle_on_invalid_status: bool = False,
            iterate_until_total: bool = False, skip_on_fail: bool = False,
            **kwargs
            ):
        """
        Initializes the CKAN3Harvester instance

        The init parameters of the Harvester class are extended with the
        following:

        Args:
            custom_endpoint:
                A custom endpoint url suffix, in case it's not at the default
                location (/3/action/package_search)
            throttle_on_invalid_status:
                By default, don't throttle if a invalid status is returned
            iterate_until_total:
                By default, new requests stop if a subsequent request yields
                an emtpy 'results' key. In some cases however, results seem to
                be filtered in API, which can lead to intermediary requests
                being empty. Setting 'true' here, makes sure requests stop only
                when start > total, where it uses the total reported in the
                most recent request
            skip_on_fail:
                If after throttling a request still fails, skip the page by
                incrementing the 'rows' parameter; disabled by default
        """
        self.custom_endpoint = custom_endpoint
        self.throttled = False
        self.throttle_on_invalid_status = throttle_on_invalid_status
        self.iterate_until_total = iterate_until_total
        self.reported_total = 1
        self.skip_on_fail = skip_on_fail
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            response_data = await self.get_json(endpoint, params=prms)
            data = []
            if 'results' in response_data['result']:
                data = response_data['result']['results']
            else:
                # Workaround for london data store
                data = response_data['result']['result']

            if self.iterate_until_total:
                self.reported_total = response_data['result']['count']

            return data

        if self.custom_endpoint is not None:
            endpoint = self.api_url.strip('/') + self.custom_endpoint
        else:
            endpoint = self.api_url.strip('/') + '/3/action/package_search'

        params = {'rows': self.rows, 'start': 0}
        payload = True  # Placeholder to initially start the while loop

        while ((self.iterate_until_total
                and params['start'] < self.reported_total)
               or (not self.iterate_until_total and payload)):
            try:
                payload = await get_data(endpoint, params)
            except asyncio.TimeoutError:
                if not self.throttled:
                    # Throttle by reducing the 'rows' parameter
                    params['rows'] = int(params['rows'] / 10)
                    self.throttled = True
                    self.logger.warning('Throttling because of timeouts')
                    self.retries_left = len(self.retry_delays)
                    continue
                elif self.skip_on_fail:
                    self.logger.warning(
                        'Skipping page with params {}'.format(
                            str(params)
                        )
                    )
                    self.retries_left = len(self.retry_delays)
                    params['start'] += params['rows']
                    continue
                else:
                    raise
            except exceptions.InvalidStatusCode:
                # When set, throttle on invalid status code
                if self.throttle_on_invalid_status and not self.throttled:
                    # Throttle by reducing the 'rows' parameter
                    params['rows'] = int(params['rows'] / 10)
                    self.throttled = True
                    self.logger.warning(
                        'Throttling because of an invalid status code'
                    )
                    self.retries_left = len(self.retry_delays)
                    continue
                elif self.skip_on_fail:
                    self.logger.warning(
                        'Skipping page with params {}'.format(
                            str(params)
                        )
                    )
                    params['start'] += params['rows']
                    self.retries_left = len(self.retry_delays)
                    continue
                else:
                    raise

            yield payload

            params['start'] += params['rows']


class OAIPMHHarvester(Harvester):
    """
    A harvester for OAI-PMH endpoints.
    """

    def __init__(
            self, *args, metadata_prefix: str = 'oai_dc',
            collection_set: Union[str, list[str]] = None,
            preserve_params: bool = False, **kwargs
            ):
        """
        Initializes the OAIPMHHarvester instance

        Extends the base harvester with additional arguments:

        Args:
            metadata_prefix:
                Optional; The metadata format requested from the API
            collection_set:
                Optional; The collection set(s) to query (pass a list if
                multiple)
            preserve_params:
                Optional; Set to True if URL params like metadataPrefix must be
                preserved in requests with a resumptionToken
        """
        self.metadata_prefix = metadata_prefix
        if not isinstance(collection_set, list):
            self.collection_set = [collection_set]
        else:
            self.collection_set = collection_set
        self.preserve_params = preserve_params
        super().__init__(*args, encoding='utf-8', **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator to iterate through consecutive API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            """
            Get data with retries. Returns the results and a resumptiontoken
            """
            response_data = await self.get_xml(endpoint, params=prms)
            cleaned_data = _common.remove_xml_namespaces(response_data)
            try:
                list_rec = cleaned_data['OAI-PMH']['ListRecords']
            except KeyError as e:
                # more elaborate error description, since a lot of KeyErrors
                # are unclear
                missing_key = str(e)
                message = '{} was not found in {}'.format(missing_key,
                                                          str(cleaned_data))
                raise KeyError(message) from None

            # If the last page is empty, in some cases the ListRecords is a
            # selfclosing tag
            if list_rec is not None:
                results = list_rec.get('record')
            else:
                results = None

            if isinstance(results, dict):
                self.logger.warning(
                    "Received a dict rather than a list, assuming it's a "
                    "single item: {}".format(str(results))
                )
                results = [results]
            resumption_token = None
            if list_rec:
                resumption_token = list_rec.get('resumptionToken')
            if resumption_token is None:
                resumption_token = cleaned_data['OAI-PMH'].get(
                    'resumptionToken'
                )
            # Because the resumtion token itself is also none sometimes, you
            # cannot Set a dict as default, and do a .get on it...
            if resumption_token is not None and\
                    isinstance(resumption_token, dict):
                resumption_token = resumption_token.get('#text')
            return results, resumption_token

        multiple_collection_sets = (len(self.collection_set) > 1)
        # Iterate through each collection_set that's given:
        for cset in self.collection_set:
            # Set initial request parameters
            params = {
                'verb': 'ListRecords',
                'metadataPrefix': self.metadata_prefix
            }

            if multiple_collection_sets:
                # In case of multiple sets, indicate which one has started
                self.logger.info('Starting to harvest set {}'.format(cset))
            if cset is not None:
                params['set'] = cset

            try:
                results, resumption_token = await get_data(self.api_url,
                                                           params)
            except (aiohttp.ClientError, exceptions.InvalidStatusCode,
                    asyncio.TimeoutError, KeyError):
                if multiple_collection_sets:
                    self.has_failed = True  # Filename will contain .INCOMPLETE
                    self.logger.exception(
                        'Harvesting of set {} failed:'.format(cset)
                    )
                    continue
                else:
                    raise

            yield results

            if self.preserve_params:
                params['resumptionToken'] = resumption_token
            else:
                params = {
                    'verb': 'ListRecords',
                    'resumptionToken': resumption_token
                }

            # Iterate through API responses,
            # token becomes zero for geonode endpoints
            while resumption_token is not None and resumption_token != '0':
                try:
                    results, resumption_token = await get_data(self.api_url,
                                                               params)
                except (aiohttp.ClientError, exceptions.InvalidStatusCode,
                        asyncio.TimeoutError, KeyError):
                    if multiple_collection_sets:
                        self.has_failed = True
                        self.logger.exception(
                            'Harvesting of set {} failed:'.format(cset)
                        )
                        break
                    else:
                        raise

                if results is None:
                    self.logger.warning(
                        'No more records found, ASSUMING all is downloaded'
                        )
                    break

                yield results

                # Set for next round. Previous properties do not need to be
                # re-sent
                params['resumptionToken'] = resumption_token


class CSW2Harvester(Harvester):
    """A Harvester for CSW v2.0.2 endpoints."""
    format_param_mapping = {
        'csw': {
            'typeNames': 'csw:Record',
            'outputSchema': 'http://www.opengis.net/cat/csw/2.0.2'
        },
        'gmd': {
            'typeNames': 'gmd:MD_Metadata',
            'outputSchema': 'http://www.isotc211.org/2005/gmd'
        }
    }

    def __init__(
            self, *args, rows: int = 100, harvest_format: str = 'csw', **kwargs
            ):
        """
        Initializes the CSW2 Harvester Instance

        The arguments of the Harvester class are extended with the
        following:

        Args:
            harvest_format:
                Optional; The format to harvest, which maps to a
                typeName/outputSchema pair. Options are 'csw' and 'gmd'.
        """
        self.harvest_format = harvest_format
        self.format_params = self.format_param_mapping[harvest_format]
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator to iterate through consecutive API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms, first=False):
            """
            Get data with retries. Returns the results and a resumptiontoken
            """
            response_data = await self.get_xml(endpoint, params=prms)

            cleaned = _common.remove_xml_namespaces(response_data)
            search_results = cleaned['GetRecordsResponse']['SearchResults']
            if self.harvest_format == 'gmd':
                payload = search_results.get('MD_Metadata')
            else:
                payload = search_results.get('Record')
            # If there's only one result on the last page, payload is a dict:
            if isinstance(payload, dict):
                # Convert to list, since this is expected by other functions
                payload = [payload]
            elif payload is None:
                self.logger.warning('Received emtpy page')
            total = int(search_results['@numberOfRecordsMatched'])
            return payload, total

        # Set initial request parameters
        params = {
            'request': 'GetRecords',
            'version': '2.0.2',
            'service': 'CSW',
            'elementSetName': 'full',
            'resultType': 'results',
            'maxRecords': self.rows
        }

        params.update(self.format_params)

        # First request, to determine result size
        payload, total = await get_data(self.api_url, params)
        self.log_total(total)
        yield payload

        start = self.rows + 1
        while start <= total:
            params['startPosition'] = start
            payload, total = await get_data(self.api_url, params)
            if payload is None:
                continue
            yield payload

            start += self.rows


class GeonodeHarvester(Harvester):
    """A harvester for Geonode instances"""

    def __init__(
            self, *args, rows: int = 100, get_layers: bool = True,
            get_documents: bool = True, **kwargs
            ):
        """
        Initializes the GeonodeHarvester instance

        Extends the Harvester class arguments with the following:

        Args:
            get_layers:
                Whether to get layer metdata from the layers endpoint
            get_documents:
                Whether to get document metadata from the documents endpoint
        """
        self.get_layers = get_layers
        self.get_documents = get_documents
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator to iterate through consecutive API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, add_datatype):
            """
            Get data with retries. Returns the results and a resumptiontoken
            """
            response_data = await self.get_json(endpoint)
            results = response_data['objects']
            for res in results:
                res['type'] = add_datatype
            next = response_data['meta']['next']
            return results, next

        # endpoints with type:
        endpoints = []
        if self.get_layers:
            endpoints.append(('/api/layers/', 'Dataset:Geographic'))
        if self.get_documents:
            endpoints.append(('/api/documents/', 'Document'))
        # Don't include the /api/maps endpoint. as they're just combined layers

        # Get the data from each endpoint:
        for endpoint, datatype in endpoints:
            url = self.api_url.strip('/') + endpoint + '?limit={}'.format(
                self.rows
            )
            data, next_url = await get_data(url, datatype)
            yield data
            prev_url = next_url
            req_count = 1
            while next_url is not None:
                data, next_url = await get_data(
                    self.api_url.strip('/') + next_url,
                    datatype
                )
                if next_url == prev_url:
                    raise exceptions.UnexpectedDataError(
                        'New request url same as previous one'
                    )
                req_count += 1
                yield data
                prev_url = next_url


class DataverseHarvester(Harvester):
    """A harvester for Dataverse websites."""

    def __init__(
            self, *args, rows: int = 100, API_key: str = None,
            key_as_url_param: bool = False, use_exporter: str = None, **kwargs
            ):
        """
        Initializes the DataverseHarvester instance

        Extends the base harvester with the following arguments:

        Args:
            API_key:
                Optional; API key that's used for authentication
            key_as_url_param:
                Whether to include the API key as a URL parameter, rather
                than as a request header (this fails for some sites)
            use_exporter:
                If given, this exporter is used to retrieve the data for each
                dataset through the 'api/datasets/export' endpoint. Please note
                that this requires a request per dataset, which means the
                performance of harvesting reduces greatly (Note: This only
                supports JSON export formats)
        """
        self.API_key = API_key
        self.key_as_url_param = key_as_url_param
        self.use_exporter = use_exporter
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator to iterate through consecutive API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, params, headers):
            """
            Get data with retries. Returns the results and a resumptiontoken
            """
            response_data = await self.get_json(
                endpoint,
                params=params,
                headers=headers
            )
            return response_data['data']['items']

        base_url = self.api_url.strip('/')
        search_url = f'{base_url}/search'
        export_url = f'{base_url}/datasets/export'

        search_params = {
            'per_page': self.rows,
            'q': '*',
            'type': 'dataset'
        }

        # API key can be given in headers or as URL parameter
        req_headers = None
        if self.API_key is not None:
            if self.key_as_url_param:
                search_params['key'] = self.API_key
            else:
                req_headers = {'X-Dataverse-key': self.API_key}

        # Get the first page of data
        datasets = await get_data(search_url, search_params, req_headers)

        # Get new pages, as long as there are entries in the previous page
        start = 0
        while isinstance(datasets, list) and datasets != []:
            # Process datasets
            if self.use_exporter is None:
                yield datasets
            else:
                for dataset_summary in datasets:
                    export_params = {
                        'exporter': self.use_exporter,
                        'persistentId': dataset_summary['global_id']
                    }
                    full_dataset = await self.get_json(
                        export_url,
                        params=export_params,
                        headers=req_headers
                    )
                    full_dataset['global_id'] = dataset_summary['global_id']
                    yield [full_dataset]

            # Get new datasets
            start += len(datasets)
            search_params['start'] = start
            datasets = await get_data(search_url, search_params, req_headers)


class DKANHarvester(Harvester):
    """An Async harvester for DKAN APIs"""

    def __init__(
            self, *args, rows: int = 10, one_request: bool = False, **kwargs
            ):
        """
        Initializes the DKANHarvester instance

        Arguments are inherited from Harvester class
        """
        self.one_request = one_request
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            response_data = await self.get_json(endpoint, params=prms)
            if isinstance(response_data['result'], dict):
                results = response_data['result']['result']
            else:
                results = response_data['result']

            if len(results) == 0:
                return []

            if isinstance(results[0], dict):
                return results
            else:
                return results[0]

        endpoint_url = self.api_url.strip('/') +\
            '/3/action/current_package_list_with_resources'

        params = {
            'limit': self.rows
        }
        payload = await get_data(endpoint_url, params)
        params['offset'] = len(payload)
        yield payload

        if not self.one_request:
            while payload:
                payload = await get_data(endpoint_url, params)
                yield payload
                params['offset'] += len(payload)


class DataONEHarvester(Harvester):
    """An Async harvester for DataONE"""

    def __init__(self, *args, rows: int = 2000, **kwargs):
        """
        Initializes the DataOneHarvester instance

        Arguments are the same as for the Harvester class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            response_data = await self.get_json(endpoint, params=prms)
            return response_data['response']['docs']

        endpoint_url = self.api_url.strip('/') + (
            '/v2/query/solr/?'
            'q=%20-obsoletedBy:*%20AND%20formatType:METADATA&wt=json'
        )

        add_params = {
            'rows': self.rows
        }
        payload = await get_data(endpoint_url, add_params)
        add_params['start'] = len(payload)
        yield payload

        while payload:
            payload = await get_data(endpoint_url, add_params)
            yield payload
            add_params['start'] += len(payload)


class SingleJSONHarvester(Harvester):
    """
    An async harvester to gather data from a single JSON payload that's
    returned by a server.
    """

    def __init__(
            self, *args, max_size: int, result_key: Union[str, dict] = None,
            **kwargs
            ):
        """
        Initializes the SingleJSONHarvester Instance

        The base Harvester class arguments are extended by:

        Args:
            max_size:
                (Now a required argument)
            result_key:
                The path in the JSON response to find the array with results
        """
        self.result_key = result_key
        super().__init__(*args, max_size=max_size, **kwargs)

    def get_result(self, rdata: Union[dict, list]) -> list[dict]:
        """
        Get the resulting array from the response_data (parsed JSON)
        """
        if self.result_key is not None:
            result = _common.get_data_from_loc(rdata, self.result_key)
            if not isinstance(result, list):
                raise exceptions.UnexpectedDataError(
                    'No array at "result_key" location'
                )
            return result
        else:
            if not isinstance(rdata, list):
                raise exceptions.UnexpectedDataError(
                    'The response data is not an array'
                )
            return rdata

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint):
            response_data = await self.get_json(endpoint, get_chuncked=True)
            return self.get_result(response_data)

        payload = await get_data(self.api_url)
        yield payload


class ArcGISOpenDataHarvester(Harvester):
    """An Async harvester for the ArcGIS Open Data API (v3)"""

    def __init__(self, *args, rows: int = 99, **kwargs):
        """
        Initializes the ArcGISOpenDataHarvester instance

        Arguments are the same as with the base Harvester class
        """
        if rows >= 100:
            raise ValueError('ArcGIS V3 API only supports rows < 100')
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms, initial_request=False):
            """ Get using from the endpoint url, with url parameters. If
            initial_request=True, the max_size for the harvester is set to the
            value reported in the initial response + 1000"""
            rdata = await self.get_json(endpoint, params=prms)
            return rdata['data']

        def filter_duplicate_ids(
                entries: list, previous_ids: set, current_ids: set
                ) -> list:
            """
            Filter entries with id's that were previously harvested
            """
            filtered_entries = []
            for entry in entries:
                e_id = entry['id']
                # Can also be in current_ids, if entries since last request
                if e_id not in previous_ids and e_id not in current_ids:
                    filtered_entries.append(entry)
                current_ids.add(e_id)

            return filtered_entries

        endpoint_url = self.api_url.strip('/') + '/v3/datasets'
        params = {
            'sort': '-modified',
            'filter[openData]': "true",
            'fields[datasets]': (
                'access,categories,collection,created,description,extent,'
                'license,licenseInfo,modified,name,organization,slug,'
                'structuredLicense,tags,type,typeCategories'
            ),
            'page[size]': self.rows,
        }

        # Keep track of id's because paging with modified date, means overlap
        cur_ids = set()
        prev_ids = set()
        harvest_finished = False

        # Initial request with data from first page, sets and logs count
        payload = await get_data(
            endpoint_url,
            params,
            initial_request=True
        )
        filtered_payload = filter_duplicate_ids(payload, prev_ids, cur_ids)
        yield filtered_payload

        # Iterate over the responses, to get data for all pages
        # Because it is ES, stop before 10.000 and add modified constraint, to
        # do next 10.000
        start_page = 2
        while not harvest_finished:
            self.logger.debug('Starting new round')
            max_page = math.floor(10000 / self.rows)
            for page_number in range(start_page, max_page):
                self.logger.debug(f'Getting page {page_number} of {max_page}')
                params['page[number]'] = page_number
                payload = await get_data(endpoint_url, params)
                if len(payload) == 0:
                    self.logger.debug('0 results returned, harvester finished')
                    harvest_finished = True
                    break

                filtered_payload = filter_duplicate_ids(
                    payload, prev_ids, cur_ids
                )
                yield filtered_payload
            else:
                # Set the 'modified date' property for the next round
                last_mod_stamp = payload[-1]['attributes']['modified']
                last_mod = datetime.date.fromtimestamp(last_mod_stamp / 1000)
                last_mod_str = last_mod.isoformat()
                if last_mod_str in params.get('filter[modified]', ''):
                    # last mod is same as previous round, reduce it, because
                    # otherwise an infinite loop is created. Will cause loss
                    # of some entries
                    self.logger.warning(
                        f'Skipping further entries for date {last_mod_str}'
                        ', additional entries ignored'
                    )
                    last_mod = last_mod - datetime.timedelta(days=1)
                    last_mod_str = last_mod.isoformat()

                params['filter[modified]'] = 'before({})'.format(last_mod_str)
                # Next one should start from page 1, because intial request is
                # missing. Current ids set is previous in next round
                start_page = 1
                prev_ids = cur_ids
                cur_ids = set()


class SocrataDiscoveryHarvester(Harvester):
    """An Async harvester for the Socrata Discovery API (v1)."""

    def __init__(self, *args, rows: int = 10000, **kwargs):
        """
        Initializes the SocrataDiscoveryHarvester instance

        Uses the same arguments as the base 'Harvester' class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            rdata = await self.get_json(endpoint, params=prms)
            return rdata['results']

        endpoint_url = self.api_url.strip('/') + '/catalog/v1'
        params = {
            'limit': self.rows,
            'order': 'dataset_id'
        }

        # Initial request
        payload = await get_data(endpoint_url, params)
        yield payload

        # Iterate over the responses, to get data for all pages
        params.pop('order')  # Subsequent requests use scroll id
        while payload:
            # Set last ID from previous payload as scroll id
            last_id = payload[-1]['resource']['id']
            params['scroll_id'] = last_id

            # Yield new page of data
            payload = await get_data(endpoint_url, params)
            yield payload


class KnoemaDCATHarvester(Harvester):
    """An Async harvester for Knoema API v1.0 DCAT."""

    def __init__(
            self, *args, download_delay: Union[float, int] = 0.2, **kwargs
            ):
        """
        Initializes the KnoemaDCATHarvester instance

        Uses the same arguments at the base 'Harvester' class. Note that the
        Download delay was decreased, since each dataset is one request for
        this harvester
        """
        super().__init__(*args, download_delay=download_delay, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_dataset_links(endpoint):
            """Get a list of links to dataset metadata"""
            rdata = await self.get_xml(endpoint)
            cleaned = _common.remove_xml_namespaces(rdata)
            return cleaned['RDF']['Catalog']['Dataset']

        @self.retry_on_fail(retry_on=(KeyError))
        async def get_dataset_metadata(endpoint):
            """Get the metadata from a specific dataset URL"""
            rdata = await self.get_xml(endpoint)
            cleaned = _common.remove_xml_namespaces(rdata)
            return cleaned['RDF']['Dataset']

        endpoint_url = self.api_url.strip('/') + '/1.0/dcat/'

        # First get the list of URLS for dataset metadata
        dataset_links = await get_dataset_links(endpoint_url)

        # Now retrieve the metadata of each dataset
        for link in dataset_links:
            dataset_metadata = await get_dataset_metadata(link)
            yield [dataset_metadata]  # Function should always yield a list


class OpenDataSoftHarvester(Harvester):
    """An Async harvester for OpenDataSoft API (v1)."""

    def __init__(self, *args, rows: int = 1000, **kwargs):
        """
        Initializes an OpenDataSoftHarvester instance

        Arguments are the same as with the base 'Harvester' class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """

        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            """Get a page of resource descriptions"""
            rdata = await self.get_json(endpoint, params=prms)
            return rdata['datasets']

        endpoint_url = self.api_url.strip('/') + '/datasets/1.0/search/'
        params = {
            'rows': self.rows
        }
        payload = await get_data(endpoint_url, params)
        yield payload

        params['start'] = 0
        while payload:
            params['start'] += len(payload)
            payload = await get_data(endpoint_url, params)
            yield payload


class BlacklightHarvester(Harvester):
    """An Async harvester for Blacklight Instances."""

    def __init__(self, *args, rows: int = 100, **kwargs):
        """
        Initializes a BlacklightHarvester instance

        Arguments are the same as for the base 'Harvester' class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """

        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms, initial_request=False):
            """
            Get a page of resource descriptions. If initial request is
            True, it will set the max-size based on the count returned
            """
            rdata = await self.get_json(endpoint, params=prms,
                                        headers={'Accept': 'application/json'})
            if initial_request:
                total_count = None
                if 'pages' in rdata['response']:
                    total_count = rdata['response']['pages']['total_count']
                elif 'numFound' in rdata['response']:
                    total_count = rdata['response']['numFound']
                if total_count is not None:
                    self.logger.info('Total count: {}'.format(total_count))
                    self.max_size = total_count + 1000
                    # Add 1000 to allow for changing size

            return rdata['response']['docs']

        endpoint_url = self.api_url
        params = {
            'per_page': self.rows
        }
        payload = await get_data(endpoint_url, params, initial_request=True)
        yield payload

        params['page'] = 1
        while payload:
            params['page'] += 1
            payload = await get_data(endpoint_url, params)
            yield payload


class DataGovINHarvester(Harvester):
    """An Async harvester for data.gov.in."""

    def __init__(
            self, *args, rows: int = 1000, max_size: int = 50000, **kwargs
            ):
        """
        Initilizes the DataGovINHarvester instance

        Arguments are the same as for the base 'Harvester' class
        """
        super().__init__(*args, rows=rows, max_size=max_size, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            response_data = await self.get_json(endpoint, params=prms)
            self.log_total(response_data['total'])
            return response_data['records']

        params = {'format': 'json', 'offset': 0, 'limit': self.rows}
        payload = await get_data(self.api_url, params)
        count = len(payload)
        yield payload

        while payload:
            params['offset'] = count
            payload = await get_data(self.api_url, params)
            count += len(payload)

            yield payload


class ScienceBaseHarvester(Harvester):
    """An Async harvester for ScienceBase.gov."""

    def __init__(self, *args, rows: int = 500, **kwargs):
        """
        Initializes the ScienceBaseHarvester instance

        Arguments are the same as with the base 'Harvester' class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            """
            Get data from the endpoint, combined with url params. If
            first_request, total count is logged
            """
            response_data = await self.get_json(endpoint, params=prms)
            results = response_data['items']
            self.log_total(response_data['total'])
            return results

        params = {'max': self.rows, 'offset': 0}

        payload = True  # Placeholder to initially start the while loop
        while payload:
            payload = await get_data(self.api_url, params)
            yield payload
            params['offset'] += len(payload)


class GeoPlatformHarvester(Harvester):
    """An Async harvester for ScienceBase.gov."""

    def __init__(self, *args, rows=1000, **kwargs):
        """
        Initializes the GeoPlatformHarvester instance

        Arguments are the same as with the base 'Harvester' class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint, prms):
            """
            Get data from the endpoint, combined with url params. If
            first_request, total count is logged
            """
            response_data = await self.get_json(endpoint, params=prms)
            results = response_data['results']
            self.log_total(response_data['totalResults'])
            return results

        params = {'size': self.rows, 'page': 0}

        payload = True  # Placeholder to initially start the while loop
        while payload:
            payload = await get_data(self.api_url, params)
            yield payload
            params['page'] += 1


class ElasticSearchScrollHarvester(Harvester):
    """Async harvester for ElasticSearch Scroll endpoints."""

    def __init__(
            self, *args, rows: int = 1000, query: dict = None,
            scroll_url: str = None, scroll_using_get: bool = False,
            scroll: str = '15s', **kwargs
            ):
        """
        Initializes the ElasticSearchScrollHarvester instance

        Arguments of the base 'Harvester' class are extended by:

        Args:
            scroll_url:
                Location of the scroll endpoint (Pass the _search endpoint to
                the api_url)
            query:
                Optional; In case match_all should not be used, specify a
                query. query.size is overriden by rows (Not yet implemented)
            scroll_using_get:
                Optional; Scroll by sending a get request with payload, in case
                post is not accepted by the server (Only True is implemented
                now)
            scroll:
                Optional; The 'scroll' parameter passed to ES to indicate for
                how long the search context should be left active
        """
        if scroll_url is None:
            raise TypeError('scroll_endpoint is a required argument')
        elif query is not None or not scroll_using_get:
            raise NotImplementedError('POST requests are not yet implemented')
        self.query = query
        self.scroll_url = scroll_url
        self.scroll_using_get = scroll_using_get
        self.scroll = scroll
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function.
            """
            response_data = await self.get_json(*args, **kwargs)
            results = response_data['hits']['hits']
            scroll_id = response_data['_scroll_id']
            self.log_total(response_data['hits']['total'])
            return results, scroll_id

        hits, scroll_id = await get_data(
            self.api_url,
            params={'size': self.rows, 'scroll': self.scroll}
        )
        yield hits

        while len(hits) == self.rows:
            body = {'scroll': self.scroll, 'scroll_id': scroll_id}
            hits, scroll_id = await get_data(self.scroll_url, json=body)
            yield hits


class ElasticSearchHarvester(Harvester):
    """
    Async harvester for ElasticSearch

    This only uses the _search endpoint. It uses POST requests with
    json-payloads.
    """

    def __init__(
            self, *args, rows: int = 1000, search_after_field: str = None,
            query=None, **kwargs
            ):
        """
        Initializes the ElasticSearchHarvester instance

        Arguments of the base 'Harvester' class are extended by:

        Args:
            search_after_field:
                Optional; If given, the 'search_after' paging is enabled, which
                allows to page beyond 10.000 entries. Please note that this
                field MUST have 'doc_values' enabled to effectively use it in
                sorting, and it MUST have unique values.
            query:
                Optional; In case match_all should not be used, specify a
                query. query.size is overriden by rows, and query.sort is
                overridden in case 'search_after_field' is enabled
        """
        if query is None:
            query = {'query': {'match_all': {}}}
        self.query = copy.deepcopy(query)
        if search_after_field is not None:
            self.query['sort'] = [{search_after_field: {'order': 'asc'}}]
        self.query['size'] = rows
        self.rows = rows
        self.search_after_field = search_after_field
        super().__init__(*args, rows=rows, **kwargs)

    def get_sa_value(self, hit: dict, sa_field: str) -> Any:
        """
        Get the Search After value

        Args:
            hit:
                The last hit returned by ElasticSearch
            sa_field:
                The full search_after field that's used. If the 'fields' option
                is used to create sub-fields, still provide the full field
                here. The function will resolve to the value of a parent level,
                if the child is not available.

        Returns:
            The value of the search after field in the hit
        """
        field_hierarchy = sa_field.split('.')
        payload = hit['_source']
        for fn in field_hierarchy:
            payload = payload.get(fn)
            if payload is None:
                raise ValueError('Search After field not found in hit!')
            elif isinstance(payload, (str, int, float)):
                return payload
            elif not isinstance(payload, dict):
                raise ValueError(
                    'Search After field does not contain str, int or float in'
                    ' hit:\n{}'.format(hit)
                )
        else:
            raise ValueError(
                'Search After field does not contain str, int or float in'
                ' hit:\n{}'.format(hit)
            )

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function. If
            first_request, total count is logged
            """
            response_data = await self.post_json(*args, **kwargs)
            results = response_data['hits']['hits']
            self.log_total(response_data['hits']['total'])

            return results

        from_ = 0
        hits = await get_data(
            self.api_url,
            json=self.query
        )
        yield hits

        while len(hits) == self.rows:
            from_ += self.rows
            if self.search_after_field is not None:
                # Use more efficient 'search_after' for paging
                search_after_value = self.get_sa_value(
                    hits[-1], self.search_after_field
                )
                self.query['search_after'] = [search_after_value]
            else:
                # Use regular 'from/size' (Only goes untill 10.000)
                self.query['from'] = from_
                if from_ + self.rows > 10000:
                    raise ValueError(
                        'From + Size exceeds 10.000. Not all entries can be '
                        'harvested'
                    )
            hits = await get_data(self.api_url, json=self.query)
            yield hits


class InvenioAPIHarvester(Harvester):
    """Async harvester for Invenio API endpoints."""

    def __init__(self, *args, rows: int = 1000, **kwargs):
        """
        Initializes the InvenioAPIHarvester instance

        Uses the same arguments as the base 'Harvester class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function. If
            first_request, total count is logged
            """
            response_data = await self.get_json(*args, **kwargs)
            results = response_data['hits']['hits']
            self.log_total(response_data['hits']['total'])

            return results

        params = {
            'size': self.rows,
            'facets': '',
            'sort': 'mostrecent',
            }

        # Since it's ES, a max of 10.000 can be retrieved. If this is reached,
        # Add the q parameter to add a max 'created' date
        max_page = math.floor(10000 / self.rows)

        hits = range(self.rows)   # Get loop started
        while len(hits) == self.rows:
            for page in range(1, max_page + 1):
                params['page'] = page
                hits = await get_data(self.api_url, params=params)
                yield hits
                if not len(hits) == self.rows:
                    break
            else:
                # Cannot page beyond 10.000, therefore set a max created date
                # It's an inclusive maximum, so there's one item overlap
                last_created = datetime.datetime.fromisoformat(
                    hits[-1]['created']
                )
                query_string = "created:[* TO {}]".format(
                    last_created.strftime('%Y-%m-%dT%H:%M:%S')
                )
                params['q'] = query_string


class MagdaAPIHarvester(Harvester):
    """Async harvester for Magda API endpoints."""

    def __init__(self, *args, rows: int = 1000, **kwargs):
        """
        Initializes the MagdaAPIHarvester instance

        Uses the arguments from the harvesters.Harvester class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function.
            """
            response_data = await self.get_json(*args, **kwargs)
            results = response_data['records']
            hm = response_data['hasMore']
            npt = response_data.get('nextPageToken')

            return results, hm, npt

        params = {
            'aspect': 'dcat-dataset-strings',
            'limit': self.rows,
            'optionalAspect': 'dataset-publisher'
        }

        endpoint = self.api_url.strip('/') + '/v0/registry/records'

        has_more = True
        while has_more:
            records, has_more, next_page_token = await get_data(
                endpoint, params=params
            )
            params['pageToken'] = next_page_token
            yield records


class GeonetworkAPIHarvester(Harvester):
    """Async harvester for Geonetwork Q search API."""

    def __init__(self, *args, rows: int = 100, **kwargs):
        """
        Initializes the GeonetworkAPIHarvester instance

        Use the same arguments as with the harvesters.Harvester class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function.
            """
            response_data = await self.get_json(*args, **kwargs)
            results = response_data['metadata']

            return results

        params = {
            '_content_type': 'json',
            'buildSummary': 'false',
            'fast': 'index',
            'sortBy': 'changeDate',
            'from': 1,
            'to': self.rows
        }

        results = range(self.rows)  # start while loop
        while len(results) == self.rows:
            results = await get_data(self.api_url, params=params)
            yield results
            params['from'] += self.rows
            params['to'] += self.rows


class EUDPHarvester(Harvester):
    """Async harvester for the European Data Portal."""

    def __init__(self, *args, rows: int = 1000, **kwargs):
        """
        Initializes the EUDPHarvester instance

        Uses the same arguments as the harvesters.Harvester class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function. Returns the results and a
            scroll id when applicable
            """
            response_data = await self.get_json(*args, **kwargs)
            results = response_data['result']['results']
            self.log_total(response_data['result']['count'])
            sid = response_data['result'].get('scrollId')

            return results, sid

        params = {
            'filter': 'dataset',
            'aggregation': 'false',
            'limit': self.rows,
            'scroll': 'true'
        }
        scroll_id = 1  # Start the while loop
        url_suffix = '/search'
        results = [1]  # Get the loop started
        while results:
            results, scroll_id = await get_data(
                self.api_url.strip('/') + url_suffix,
                params=params
            )
            yield results
            params = {
                'scrollId': scroll_id
            }
            url_suffix = '/scroll'


class JunarAPIHarvester(Harvester):
    """Async harvester for the Junar API (v2)."""
    possible_resource_types = set(
        ['datasets', 'datastreams', 'visualziations', 'dashboards'])

    def __init__(
            self, *args, resource_types: list[str], rows: int = 1000,
            max_size: int = 50000, API_key: str = None,
            API_key_fetch_url: str = None, **kwargs
            ):
        """
        Initializes the JunarAPIHarvester instance

        Uses the same arguments as the harvesters.Harvester class, extended by:

        Args:
            resources_types:
                The resource types to query from API, options are datasets,
                datastreams (these are dataviews), visualizations, dashboards.
            API_key:
                Optional; The API key to use for the requests
            API_key_fetch_url:
                Optional; The URL to retrieve an API key for the Junar API
                (e.g. https://beta.datos.gob.cl/manageDeveloper/create/)
        """
        self.API_key = API_key
        self.API_key_fetch_url = API_key_fetch_url

        has_invalid_resource_types = any(
            (t not in self.possible_resource_types) for t in resource_types
        )
        if has_invalid_resource_types:
            raise TypeError('Invalid data for argument \'resource_types\'')
        self.resource_types = resource_types
        super().__init__(*args, rows=rows, max_size=max_size, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function.
            """
            response_data = await self.get_json(*args, **kwargs)
            if isinstance(response_data, dict):
                results = response_data['results']
                if self.first_request:
                    resource_type = args[0].split('/')[-1]
                    self.logger.info(
                        f"Total in {resource_type}: {response_data['count']}"
                    )
                    self.first_request = False
            else:
                results = response_data

            return results

        @self.retry_on_fail(retry_on=(KeyError))
        async def fetch_api_key():
            """
            Get data, using the get_json function.
            """
            params = {'_': int(time.time() * 1000)}
            response_data = await self.get_json(
                self.API_key_fetch_url, params=params
            )

            return response_data['pApiKey']

        params = {
            'limit': self.rows
        }
        if self.API_key is not None:
            params['auth_key'] = self.API_key
        elif self.API_key_fetch_url is not None:
            params['auth_key'] = await fetch_api_key()

        for rtype in self.resource_types:
            self.first_request = True
            endpoint = self.api_url.strip('/') + f'/api/v2/{rtype}.json'
            params['offset'] = 0

            results = [None]  # Get while loop started
            while len(results) > 0:
                results = await get_data(endpoint, params=params)
                yield results
                params['offset'] += self.rows


class UdataHarvester(Harvester):
    """
    Async harvester for Udata portals.

    Uses version 1 of the Udata API
    """

    def __init__(self, *args, rows: int = 500, **kwargs):
        """
        Initializes the UdataHarvester instance

        Arguments are the same as for the harvesters.Harvester class
        """
        super().__init__(*args, rows=rows, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(*args, **kwargs):
            """
            Get data, using the get_json function.
            """
            response_data = await self.get_json(*args, **kwargs)
            results = response_data['data']
            self.log_total(response_data['total'])

            return results

        api_endpoint = self.api_url.strip('/') + '/1/datasets/'

        params = {
            'page': 1,
            'page_size': self.rows
        }

        results = range(self.rows)  # start while loop
        while len(results) == self.rows:
            results = await get_data(api_endpoint, params=params)
            yield results
            params['page'] += 1


class SingleXMLHarvester(Harvester):
    """An async harvester to gather data from a single XML payload"""

    def __init__(self, *args, max_size, result_key=None, **kwargs):
        """
        Initializes the SingleXMLHarvester instance

        Uses the same arguments as the harvester.Harvester class, extended by:

        Args:
            max_size:
                (Is made a mandatory argument). Set this explicity (in bytes)
                to prevent running out of memory.
            result_key:
                Optional; The path where the list of results is found (in the
                namespace cleaned data) (e.g. {'RDF': {'Catalog': 'dataset'}},
                gets it from rdf:RDF > dcat:Catalog > dcat:dataset)
        """
        if not isinstance(max_size, int):
            raise ValueError(
                'max_size parameter should be set explicitly (integer)'
                ', to limit the maximum data size thats returned'
                )
        self.result_key = result_key
        super().__init__(*args, max_size=max_size, **kwargs)

    def get_result(self, rdata: Any) -> list[dict]:
        """
        Get the resulting array from the response_data (parsed XML)
        """
        if self.result_key is not None:
            result = _common.get_data_from_loc(rdata, self.result_key)
            if not isinstance(result, list):
                raise exceptions.UnexpectedDataError(
                    'No array at "result_key" location'
                )
            return result
        else:
            if not isinstance(rdata, list):
                raise exceptions.UnexpectedDataError(
                    'The response data is not an array'
                )
            return rdata

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator yielding the data from the single request
        """
        @self.retry_on_fail(retry_on=(KeyError))
        async def get_data(endpoint):
            response_data = await self.get_xml(endpoint, get_chuncked=True)
            cleaned_data = _common.remove_xml_namespaces(response_data)
            return self.get_result(cleaned_data)

        payload = await get_data(self.api_url)
        yield payload


class XMLLinesHarvester(Harvester):
    """
    Harvester to retrieve XML data line by line

    Use this for large XML files, that should not be loaded into memory at once

    NOTE: Currently, no retry behaviour is implemented, since this would lead
    to duplicate data
    """

    def __init__(self, *args, resource_element: str, **kwargs):
        """
        Initializes the XMLLinesHarvester instance

        The arguments of the base harvesters.Harvester class are extended by:

        Args:
            resource_element:
                The Resource element, to detect where a single resource starts
                and stops from the lines (e.g. dcat:Dataset)
        """
        self.resource_element = resource_element
        super().__init__(*args, is_single_request=True, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator yielding the data from each resource detected in the
        lines
        """
        opening_element = f'<{self.resource_element} '
        closing_element = f'</{self.resource_element}>'
        element_str = ''
        in_element = False
        line_nr = 0
        async for line in self.get_lines(self.api_url):
            line_nr += 1
            if line_nr % 10000 == 0:
                self.logger.debug(f'Received {line_nr} lines')
            if opening_element in line:
                if in_element:
                    raise ValueError(
                        'Found opening element while already in element'
                    )
                in_element = True
            elif closing_element in line:
                if not in_element:
                    raise ValueError(
                        'Found closing element outside element'
                    )
                element_str += line
                yield [
                    _common.remove_xml_namespaces(
                        _common.limit_depth(
                            xmltodict.parse(
                                element_str
                            ),
                            50
                        )
                    )
                ]
                element_str = ''
                in_element = False
            if in_element:
                element_str += line


class JSONIndexHarvester(Harvester):
    """
    Async harvester that harvests individual datasets that are described in a
    main 'index' json file, which contains an array with the urls
    """

    def __init__(self, *args, url_location: Union[str, dict], **kwargs):
        """
        Initializes the JSONIndexHarvester instance

        Arguments of the base harvesters.Harvester are extended by:

        Args:
            url_location:
                If the index file is a list of dictionaries, this is the
                location in the dictionary where url for the resource can be
                found (e.g. 'url', or {'resource': 'url'})
        """
        self.url_location = url_location
        super().__init__(*args, **kwargs)

    async def request_data(self) -> Iterator[list[dict]]:
        """
        Async generator iterating through the API responses
        """
        @self.retry_on_fail(retry_on=(KeyError, TypeError))
        async def get_index():
            """
            Get data, using the get_json function.
            """
            response_data = await self.get_json(self.api_url)
            urls = []
            for item in response_data:
                if isinstance(item, str):
                    urls.append(item)
                elif isinstance(item, dict):
                    url = _common.get_data_from_loc(item, self.url_location)
                    if not isinstance(url, str):
                        raise TypeError('URL should be a string')
                    urls.append(
                        url
                    )

            self.log_total(len(urls))

            return urls

        url_list = await get_index()
        for url in url_list:
            metadata = await self.get_json(url)
            yield [metadata]
