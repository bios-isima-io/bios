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

import asyncio
import json
import logging
import math
import os
import queue
import threading
import time

import _bios_csdk_native as csdk

from ..errors import ServiceError
from ._error_handler import _ErrorHandler


class CSdkSession:
    """
    C-SDK Python connector
    """

    _error_handler = _ErrorHandler()
    _logger = logging.getLogger(__name__)
    _next_session_handle = 0
    _seqno = 1
    _multithreading_mode = False
    _pid = None

    _futures = {}
    _pending_ingest_bulks = {}

    _csdk = csdk

    def __init__(self, error_handler=None, multi_threading=None):
        """Constructor

        Args:
            error_handler (ErrorHandler): The error handler for the session class
        """
        if error_handler:
            self._error_handler = error_handler
        self._session_handle = CSdkSession._next_session_handle
        CSdkSession._next_session_handle += 1
        self._session_id = None
        self._tenant_name = None
        self._app_name = None
        self._app_type = None
        self._is_signed_in = False
        self._is_valid = False
        self._loop_thread = None
        (self._fdr, self._fdw) = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)

        self._multithreading = multi_threading or self._multithreading_mode
        self._thread_id = threading.get_ident()
        if self._multithreading:
            # We don't prepare the event loop here. It's done by _seup_multi_threading() in
            # another loop thread via method _loop_thread_main()
            self._loop = None
            self._setup_multi_threading()
        else:
            current_pid = os.getpid()
            if CSdkSession._pid is not None and CSdkSession._pid != current_pid:
                # This is a forked subprocess. The parent may be using the loop still. We replace
                # the loop for the thread blindly.
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                CSdkSession._pid = current_pid
            else:
                # This is a parent process or an already-handled subprocess
                loop = self.prepare_loop()
            CSdkSession._pid = current_pid
            self._loop = loop
            self._setup_single_threading()

        self._entries = queue.Queue()
        self._run(self._add_reader())
        self._csdk.create_session(self._session_handle, self._fdw)
        self._is_valid = True

    @classmethod
    def prepare_loop(cls):
        """Utility method that returns an event loop for the thread"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    def __del__(self):
        """Destructor"""
        self.close()

    def _check_multithreading(self):
        current_thread_id = threading.get_ident()
        if not self._multithreading and current_thread_id != self._thread_id:
            raise RuntimeError(
                "Multi-threading is disabled."
                " Call 'bios.enable_threads() before starting sessions in multi-threads."
            )

    @classmethod
    def enable_threads(cls):
        cls._multithreading_mode = True

    @classmethod
    def disable_threads(cls):
        cls._multithreading_mode = False

    async def _stop_multithread_loop(self):
        self._loop.stop()

    def _loop_thread_main(self):
        # We don't use the loop for the class but the loop for the thread.
        self._loop = self.prepare_loop()
        self._loop.run_forever()

    def _setup_multi_threading(self):
        if self._loop_thread is not None:  # already set up
            return
        self._loop_thread = threading.Thread(target=self._loop_thread_main)
        self._loop_thread.daemon = True
        self._current_run = self._run_multi_thread
        self._loop_thread.start()
        wait_start = time.time()
        wait_time = 0.01
        while not self._loop or not self._loop.is_running():
            if time.time() - wait_start > 10:
                raise RuntimeError(f"Thread loop did not start, loop={self._loop}")
            time.sleep(wait_time)
            wait_time *= 2

    def _setup_single_threading(self):
        self._current_run = self._run_single_thread

    # Getters and Setters #################################################################

    def _set_session_id(self, session_id):
        """Sets session ID issued by the C-SDK.
        Used internally on starting a session."""
        self._session_id = session_id

    def get_tenant_name(self):
        return self._tenant_name

    def get_app_name(self):
        return self._app_name

    def get_app_type(self):
        return self._app_type

    # Session APIs ########################################################################

    @classmethod
    def get_operation_name(cls, op_id):
        """Gets operation name for the specified operation id.

        The method is used for testing consitency of Operation IDs between the Python wrapper and
        C-SDK.

        Args:
            op_id (CSdkOperationId): Operation ID

        Returns: str: Operation name
        """
        return cls._csdk.get_operation_name(op_id.value)

    @classmethod
    def list_error_codes(cls):
        status_codes = cls._csdk.list_status_codes()
        error_codes = []
        for status_code in status_codes:
            error_codes.append(cls._error_handler.resolve_code(status_code))
        return error_codes

    @classmethod
    def get_status_name(cls, error_code):
        """Gets status name for the specified error code.

        The method is used for testing consitency of error codes between the Python wrapper and
        C-SDK.

        Args:
            error_code (ErrorCode): Error code

        Returns: str: Status name
        """
        return cls._csdk.get_status_name(error_code.value)

    def close(self, shutdown_gracefully=True):
        """Close the session."""
        if not self._is_valid:
            return

        if self._session_handle is not None:
            session_id = self._session_id or -1
            self._csdk.close_session(self._session_handle, session_id)
        if self._fdr:
            if self._multithreading:
                if shutdown_gracefully:
                    self._run(self._remove_reader())
                    # request to stop the loop; don't wait for the response
                    asyncio.run_coroutine_threadsafe(self._stop_multithread_loop(), self._loop)
                else:
                    # If we reach here, this method is likely to be called during shutdown
                    # (session was left open until the process ends and __del__ is called without
                    # session being closed explicitly).
                    # If called from __del__ or during shutdown (not clear which is the cause),
                    # the event loop behaves unexpectedly. Multi-thread semantics above would
                    # freeze. Below execution would pass through, but the loop is unlikely to
                    # stop. In that case, later loop.close() would raise a RuntimeError, but
                    # we'll ignore it.
                    self._loop.remove_reader(self._fdr)
                    self._loop.stop()
                if self._loop_thread:
                    self._loop_thread.join()
            else:
                self._loop.remove_reader(self._fdr)

            try:
                self._loop.close()
            except RuntimeError:
                if shutdown_gracefully:
                    raise
            os.close(self._fdr)
            os.close(self._fdw)
            self._fdr = None
            self._fdw = None
        self._session_id = None
        self._is_valid = False

    def _run(self, method):
        return self._current_run(method)

    def _run_multi_thread(self, method):
        """Method to _run an asynchronous method and wait for its result.

        Args:
            method (Future): Awaitable method

        Returns: (any): The result of the awaitable method
        """
        return asyncio.run_coroutine_threadsafe(method, self._loop).result()

    def _run_single_thread(self, method):
        """Method to _run an asynchronous method and wait for its result.

        Args:
            method (Future): Awaitable method

        Returns: (any): The result of the awaitable method
        """
        return self._loop.run_until_complete(method)

    def start_session(
        self,
        host: str,
        port: int,
        ssl_enabled: bool,
        journal: str,
        op_timeout: int,
        ssl_cert_file: str = None,
    ) -> "CSdkSession":
        """API method to start session.

        This method must be called first when you start a session. Calling other API methods before
        calling this method may cause RuntimeError.

        Args:
            host (str): Server host name
            port (int): Server port number
            ssl_enabled (bool): Whether using secure connections
            journal (str): Journal directory
            op_timeout (int): Deafult operation timeout milliseconds
            ssl_cert_file (str = None): Keyword parameter to specify the SSL certificate file for
                the server. If omitted, value of environment variable SSL_CERT_FILE is taken.

        Returns: CSdkSession: Self
        """
        response = self._run(
            self._start_session_core(
                host,
                port,
                ssl_enabled,
                journal,
                ssl_cert_file,
                op_timeout,
            )
        )
        session_info = self._handle_response(response[0], response[1], response[2])
        self._set_session_id(session_info.get("sessionId"))
        return self

    async def _start_session_core(
        self,
        host,
        port,
        is_secure,
        journal,
        ssl_cert_file,
        op_timeout,
    ):
        """Internal method that calls StartSession C-SDK operation."""
        (seqno, future) = self._create_future()
        handle = int(self._session_handle)
        self._logger.debug("calling csdk.start_session handle=%s", handle)
        self._csdk.start_session(
            0,
            handle,
            seqno,
            host,
            port,
            is_secure,
            journal,
            ssl_cert_file,
            op_timeout,
        )
        return await future

    def simple_method(
        self,
        op_id,
        tenant=None,
        stream=None,
        data=None,
        raw_out=False,
        endpoint=None,
        skip_logging=False,
    ):
        """Method to execute a C-SDK operation synchronously.

        Args:
            op_id (CSdkOperationId): Operation ID
            tenant (str): Keyword parameter for target tenant name (default=None)
            stream (str): Keyword parameter for target stream name (default=None)
            data (object): Keyword parameter for payload object (default=None)
            raw_out (bool): The method deserializes the output to dict in default. But when this
                flag is True, the method returns the output as JSON string.
            endpoint (str): Endpoint to send the request, load balancing would be disabled when
                            this option parameter is set.

        Returns: tuple of the following
            JSON decoded server response
            endpoint
            start_time
            internal latency in microseconds
        """
        wrapper_start = time.time()
        payload = self._make_payload(data)

        result = self._run(self._simple_method_core(op_id, tenant, stream, payload, endpoint))
        endpoint = result[2] or ""
        start_time = result[3]
        csdk_elapsed = result[4]
        num_reads = result[5]
        num_writes = result[6]
        num_qos_retry_considered = result[7]
        num_qos_retry_sent = result[8]
        num_qos_retry_response_used = result[9]
        wrapper_elapsed = math.ceil((time.time() - wrapper_start) * 1000 * 1000)
        try:
            handled = self._handle_response(result[0], result[1], result[2], raw_out)
        except ServiceError as err:
            self._csdk.log(
                self._session_handle,
                self._session_id,
                start_time,
                csdk_elapsed,
                wrapper_elapsed,
                0,
                0,
                str(op_id),
                err.error_code.name,
                "message=" + err.message if err.message else "message=",
                endpoint,
                stream or "",
                0,
                0,
                0,
            )
            raise
        if not skip_logging:
            self._csdk.log(
                self._session_handle,
                self._session_id,
                start_time,
                csdk_elapsed,
                wrapper_elapsed,
                num_reads,
                num_writes,
                str(op_id),
                "OK",
                "",
                endpoint,
                stream or "",
                0,
                0,
                0,
            )
        return (
            handled,
            endpoint,
            start_time,
            csdk_elapsed,
            num_qos_retry_considered,
            num_qos_retry_sent,
            num_qos_retry_response_used,
        )

    def generic_method(
        self,
        op_id,
        resources=None,
        options=None,
        data=None,
        raw_out=False,
        timeout=None,
        skip_logging=False,
        num_upserts=None,
    ):
        """Method to execute a C-SDK operation synchronously.

        Args:
            op_id (CSdkOperationId): Operation ID
            resources (dict): Key value pairs of operation resource parameters
            options (dict): Key value pairs of options
            data (object): Keyword parameter for payload object (default=None)
            raw_out (bool): The method deserializes the output to dict in default. But when this
                flag is True, the method returns the output as JSON string.
            timeout (int): Operation timeout milliseconds, 0 for no timeout

        Returns: tuple of the following
            JSON decoded server response
            endpoint
            start_time
            internal latency in microseconds
        """
        wrapper_start = time.time()

        payload = self._make_payload(data)

        resources_list = []
        if resources:
            for key, value in resources.items():
                resources_list.append(key)
                resources_list.append(value)

        options_list = []
        if options:
            for key, value in options.items():
                options_list.append(key)
                options_list.append(value)

        if timeout is None:
            timeout = -1
        result = self._run(
            self._generic_method_core(op_id, resources_list, options_list, payload, timeout),
        )
        endpoint = result[2] or ""
        start_time = result[3]
        csdk_elapsed = result[4]
        num_reads = result[5]
        num_writes = result[6]
        if num_upserts:
            num_writes = num_upserts
        wrapper_elapsed = math.ceil((time.time() - wrapper_start) * 1000 * 1000)
        stream = resources.get("stream") or "" if resources else ""
        try:
            ret = self._handle_response(result[0], result[1], result[2], raw_out)
        except ServiceError as err:
            self._csdk.log(
                self._session_handle,
                self._session_id,
                start_time,
                csdk_elapsed,
                wrapper_elapsed,
                0,
                0,
                str(op_id),
                err.error_code.name,
                "message=" + err.message if err.message else "message=",
                endpoint,
                stream or "",
                0,
                0,
                0,
            )
            raise
        if not skip_logging:
            self._csdk.log(
                self._session_handle,
                self._session_id,
                start_time,
                csdk_elapsed,
                wrapper_elapsed,
                num_reads,
                num_writes,
                str(op_id),
                "OK",
                "",
                endpoint,
                stream,
                0,
                0,
                0,
            )
        return (ret, endpoint, start_time, csdk_elapsed, 0, 0, 0)

    def add_endpoint(self, endpoint, node_type):
        """Method to add endpoint

        Args:
            op_id (CSdkOperationId): Operation ID
            endpoint (str): Endpoint to send the request, load balancing would be disabled when
                            this option parameter is set.
            node_type (NodeType): node type

        Returns: Object: JSON decoded server reponse
        """
        result = self._run(self._add_endpoint_core(endpoint, node_type))
        return self._handle_response(result[0], result[1], result[2])

    @classmethod
    def _make_payload(cls, data):
        """Internal method to generate a payload JSON from input data object.

        Args:
            data (dict): Input data

        Returns: str: Data serialized to JSON or data if already serialized,
        None in case the input is None
        """
        if data is None:
            return None
        return data if (isinstance(data, (bytes, str))) else json.dumps(data)

    async def _simple_method_core(self, op_id, tenant, stream, payload, endpoint):
        """Internal coroutine to execute a C-SDK operation."""
        (seqno, future) = self._create_future(payload)
        self._csdk.simple_method(
            self._session_handle,
            seqno,
            self._session_id,
            op_id.value,
            tenant,
            stream,
            payload,
            endpoint,
        )
        return await future

    async def _generic_method_core(self, op_id, resources, options, payload, timeout):
        """Internal coroutine to execute a C-SDK operation."""
        (seqno, future) = self._create_future(payload)
        self._csdk.generic_method(
            self._session_handle,
            seqno,
            self._session_id,
            op_id.value,
            resources,
            options,
            payload,
            timeout,
        )
        return await future

    async def _add_endpoint_core(self, endpoint, node_type):
        """Internal coroutine to execute a C-SDK operation."""
        (seqno, future) = self._create_future()
        self._csdk.add_endpoint(
            self._session_handle,
            seqno,
            self._session_id,
            endpoint,
            node_type.value,
        )
        return await future

    def _create_future(self, data=None):
        """Internal method to be used for creating a future for an operation.

        The method first creates a sequence number that is a unique number in the session.
        Then a future to be used for the operation is created. The future is registered to the
        futures table with sequence number as the key. This is used later when completing the
        operation.

        Args:
          data (Object): Data that the caller wants to hold until the operation completes

        Returns: tuple: Created sequence number and future.
        """
        seqno = self._seqno
        CSdkSession._seqno += 1
        if CSdkSession._seqno == 0x80000000:
            CSdkSession._seqno = 0

        self._check_multithreading()

        future = self._loop.create_future()
        future.data = data
        self._futures[seqno] = future
        self._logger.debug(
            "registered future handle=%s seqno=%s future=%s",
            self._session_handle,
            seqno,
            future,
        )
        return (seqno, future)

    def _handle_notification(self):
        """Internal method that handles completion notification from the Py-C connector.

        An operation completion callback function in C-SDK puts the response data in a queue, then
        sends a notification via the pipe write FD that this session object owns. The event loop
        monitors the pipe read FD and invokes this method on notification, which picks up items
        from the queue via Py-C connector method fetch_next(). For each data entry, the method
        resolves the future object for the seqno to put the received data into it. It completes the
        original coroutine.
        """
        # Read 1 byte to consume the notification.
        notif = os.read(self._fdr, 1)
        self._logger.debug(
            "handle_notification notif=%s, session=%s",
            notif,
            self._session_handle,
        )

        # True for more indicates an incoming more request
        more_request = notif == b"m"

        # Retrieve data until exhausted.
        while True:
            if more_request:
                entry = self._csdk.fetch_next_bulk(self._session_handle)
            else:
                entry = self._csdk.fetch_next(self._session_handle)
            if not entry:
                break
            seqno = entry[0]
            done = True
            self._logger.debug(
                " retrieved seq=%s bulks=%s",
                seqno,
                self._pending_ingest_bulks,
            )
            bulk_handler = self._pending_ingest_bulks.get(seqno)
            if bulk_handler:
                done = False
                if more_request:
                    bulk_ctx_id = entry[1]
                    from_index = entry[2]
                    to_index = entry[3]
                    self._logger.debug(
                        "  handle=%s seqno=%s bulk_id=%s from_index=%s to_index=%s",
                        self._session_handle,
                        seqno,
                        bulk_ctx_id,
                        from_index,
                        to_index,
                    )
                    bulk_handler.process_more_request(bulk_ctx_id, from_index, to_index)
                else:
                    status = entry[1]
                    reply = entry[2]
                    endpoint = entry[3]
                    from_index = entry[4]
                    if len(entry) > 5:
                        start_time = entry[5]
                        csdk_elapsed = entry[6]
                    else:
                        start_time = 0
                        csdk_elapsed = 0
                    complete_response = bulk_handler.process_partial_response(
                        status, reply, endpoint, from_index, start_time, csdk_elapsed
                    )
                    if complete_response:
                        entry = complete_response
                        done = True
            else:
                if more_request:
                    done = False
                    entry = self._csdk.fetch_next_bulk(self._session_handle)
                    self._logger.debug("Ignored unexpected Bulk ingest more request from C-SDK")

            if done:
                future = self._futures.get(seqno)
                status = entry[1]
                reply = entry[2]
                endpoint = entry[3]
                if len(entry) > 5:
                    start_time = entry[5]
                    csdk_elapsed = entry[6]
                else:
                    start_time = 0
                    csdk_elapsed = 0
                if len(entry) > 8:
                    num_reads = entry[7]
                    num_writes = entry[8]
                else:
                    num_reads = 0
                    num_writes = 0
                if len(entry) > 9:
                    num_qos_retry_considered = entry[9]
                    num_qos_retry_sent = entry[10]
                    num_qos_retry_response_used = entry[11]
                else:
                    num_qos_retry_considered = 0
                    num_qos_retry_sent = 0
                    num_qos_retry_response_used = 0

                self._logger.debug(
                    "  handle=%s seqno=%s future=%s futures=%s status=%s reply=%s endpoint=%s,"
                    " start_time=%d, csdk_elapsed=%d",
                    self._session_handle,
                    seqno,
                    future,
                    len(self._futures),
                    hex(status),
                    reply,
                    endpoint,
                    start_time,
                    csdk_elapsed,
                )

                del self._futures[seqno]
                self._logger.debug("  size of futures: %s", len(self._futures))
                future.set_result(
                    (
                        status,
                        reply,
                        endpoint,
                        start_time,
                        csdk_elapsed,
                        num_reads,
                        num_writes,
                        num_qos_retry_considered,
                        num_qos_retry_sent,
                        num_qos_retry_response_used,
                    )
                )

    @classmethod
    def _handle_response(cls, status_code, reply, endpoint, raw_out=False):
        """Internal method to handle a C-SDK response.

        The method deserializes the response JSON to a dict object. If the status is error,
        the method raises a ServiceError.

        Args:
            status_code (int): Operation status code
            reply (str): Server reply JSON
            endpoint (str): Server endpoint when the endpoint is available, otherwise None

        Returns: dict: Response object
        """
        cls._error_handler.handle_response(status_code, reply, endpoint=endpoint)
        return None if not reply else reply if raw_out else json.loads(reply)

    async def _add_reader(self):
        self._loop.add_reader(self._fdr, self._handle_notification)

    async def _remove_reader(self):
        self._loop.remove_reader(self._fdr)

    async def _echo(self, message, delay):
        return await self._would_echo(message, delay)

    def _would_echo(self, message, delay, binary_data=False, seqno=None):
        future = None
        # seqno is tracked outside the current scope
        if not seqno:
            (seqno, future) = self._create_future()
        if binary_data:
            self._csdk.echo(self._session_handle, seqno, message, delay, True)
        else:
            self._csdk.echo(self._session_handle, seqno, message, delay)
        return future

    def _ask(self, seqno, from_index, to_index):
        self._csdk.ask(self._session_handle, seqno, from_index, to_index)
