#
# Copyright (C) 2025 Isima, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from http import HTTPStatus
from http.client import (
    HTTPConnection,
    HTTPSConnection,
    RemoteDisconnected,
    CannotSendRequest,
)
import json
import socket
import ssl
import time
from urllib.parse import urlparse


class OperationError(Exception):
    def __init__(self, status, response):
        self.status = status
        if isinstance(response, str):
            try:
                self.response = json.loads(response)
            except Exception:
                self.response = response
        else:
            self.response = None


class RestClient:
    """ A utility class that executes REST API calls. """

    # Request size limit
    _REQUEST_MAX_SIZE = 1000000

    # Maximum number of trials in an HTTP call
    _NUM_TRIALS = 8
    _INITIAL_SLEEPMS = 0.1
    _BACKOFF_FACTOR = 2.0

    @classmethod
    def call(
        cls,
        connection,
        method,
        path,
        data=None,
        headers=None,
        token=None,
        no_retry=False,
    ):
        """Make a REST API call.

        Args:
            connection (HttpConnection): Connection to the server
            method (object): HTTP method to be used for the call.
            path (str): Target API path.
            data (str or dict): Request data if any.

        Returns:
            (str, dict): tuple of (REST API response as JSON string) and (headers as a dict)
        """
        num_trials = cls._NUM_TRIALS
        sleepms = cls._INITIAL_SLEEPMS
        if isinstance(data, dict):
            data = json.dumps(data)
        while True:
            try:
                cls.async_request(connection, method, path, data, headers, token)
                return cls.get_response(connection)
            except (
                BrokenPipeError,
                RemoteDisconnected,
                CannotSendRequest,
                ConnectionResetError,
            ):
                if num_trials < cls._NUM_TRIALS:
                    time.sleep(sleepms)
                    sleepms *= cls._BACKOFF_FACTOR
                num_trials -= 1
                if num_trials == 0:
                    # raise ServiceError(ErrorCode.SERVER_CHANNEL_ERROR, str(connection)) from err
                    raise Exception(str(connection))
                connection.reconnect()
                continue
            except ConnectionRefusedError:
                # raise ServiceError(ErrorCode.SERVER_CONNECTION_FAILURE, str(connection)) from err
                raise Exception(str(connection))

    @classmethod
    def async_request(cls, connection, method, path, data=None, headers=None, token=None):
        """Send a REST API request asynchronously.

            The method sends a request to server and returns without waiting for the response
            from the server.

        Args:
            connection (HttpConnection): The connection to the target server
            method (str): HTTP method to use
            path (str): Target path
            data (object): Request data if any

        Returns:
            int: Stream ID to be used for receiving response
        """
        if data:
            body = str(data)
            content_length = len(body)
            if not headers:
                headers = {}
            # update the header only if not specified by the caller
            if "Content-Type" not in headers:
                headers.update({"Content-Type": "application/json"})
            # update the length
            headers.update(
                {
                    "Content-Length": str(content_length),
                }
            )
            if token:
                headers.update({"Authorization": "Bearer " + token})
        else:
            body = None
            if token:
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                }
            else:
                headers = {}

        return connection.async_request(method, path, body=body, headers=headers)

    @classmethod
    def get_response(cls, connection):
        """Method to receive response from the server.

        The method blocks until receiving response from the server.

        Args:
            connection (HttpConnection): The connection to the target server

        Returns:
            str: REST API response as JSON string
        """
        resp = connection.get_response()
        reply_text_src = resp.read()
        if reply_text_src:
            reply_text = reply_text_src.decode("utf8")
        else:
            reply_text = ""

        if resp.status != 200:
            raise OperationError(resp.status, reply_text)

        # parse headers
        headers_list = resp.getheaders()
        headers = {}
        for (name, value) in headers_list:
            headers[name.lower()] = value

        return reply_text, headers


class HttpConnection:
    """A class to manage HTTP2 connection.

    An instance of this method has three properties as follows:

        - host (str): <host>[:<port>]
        - port (str): default port, if not part of host
        - secure (boolean): whether if connection is secured
        - path (str): path to check service availability after connection
        - cafile (str): Certification Authority certfile to be used for secure connection.
            The file should consist of concatenated CA certificates in PEM format.
    """

    def __init__(self, url=None, host=None, port=None, secure=None, cafile=None):
        """The constructor.

        Args:
            url (str): target server url
            host (str): Server host name
            port (int): Server port number
            ssl_enabled (bool): Whether if the server connection uses secured connection
            cafile (str): Path to a file of concatenated CA certificates in PEM format.
        """
        self.host = host
        self.port = port
        self.secure = secure
        self.cafile = cafile
        self._is_closed = True

        self.path = "/"
        if url:
            _url = urlparse(url)
            self.secure = _url.scheme == "https"

            if self.secure:
                port = 443
            else:
                port = 80

            elements = _url.netloc.split(":")
            self.host = elements[0]
            if len(elements) > 1:
                self.port = int(elements[1])

        self._make_connection()
        self._connection.request("GET", self.path)
        resp = self._connection.getresponse()
        resp.read()

        self._is_closed = False

    def request(self, method, path, body, headers):
        """Synchronous HTTP request.

        Args:
            method (str): HTTP method
            path (str): HTTP path
            body (str): HTTP body
            headers (dict): HTTP headers

        Returns:
            HttpResponse: response from server
        """
        self._connection.request(method, path, body=body, headers=headers)
        return self._connection.getresponse()

    def async_request(self, method, path, body, headers):
        """Asynchronous HTTP request.

        The method finishes after sending the request without waiting for response from server.
        Use method get_response() to receive response.

        Args:
            method (str): HTTP method
            path (str): HTTP path
            body (str): HTTP body
            headers (dict): HTTP headers
        """
        self._connection.request(method, path, body=body, headers=headers)

    def get_response(self):
        """Method to receive response from the server.

        Returns:
            HttpResponse: response from the server
        """
        return self._connection.getresponse()

    def close(self):
        # Do nothing if the connection is closed already
        if self._is_closed:
            return

        try:
            self._connection.close()
        except ssl.SSLError:
            # TODO(TFOS-793) Move this fix into Hyper
            pass

        self._is_closed = True

    def is_closed(self):
        """Answers whether if the connection is closed in this object"""
        return self._is_closed

    def reconnect(self):
        """Try reconnection"""
        try:
            self._connection.close()
        except Exception:
            # Closing connection may fail but we'll continue anyway
            pass

        self._make_connection()
        self._is_closed = False

    def get_local_address(self):
        """Get local address of the connection.

        The method first tries to fetch local address from the client socket.
        If it fails, the method asks the system.

        Returns:
            str: Socket name
        """
        if self._connection.sock:
            sockname = self._connection.sock.getsockname()
            return sockname[0]
        return socket.gethostbyname(socket.gethostname())

    def _make_connection(self):
        if self.secure:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            if self.cafile:
                ssl_context.load_verify_locations(cafile=self.cafile)
            else:
                ssl_context.load_default_certs()
            self._connection = HTTPSConnection(self.host, port=self.port, context=ssl_context)
        else:
            self._connection = HTTPConnection(self.host, port=self.port)

    def __enter__(self):
        return self

    def __exit__(self, unused_type, unused_val, unused_tb):
        self.close()

    def __del__(self):
        self.close()
