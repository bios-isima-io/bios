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
import logging


class BatchData:
    """Class to carry batch execution data"""

    def __init__(self, from_index, to_index, payload):
        self.from_index = from_index
        self.to_index = to_index
        self.payload = payload


class IngestBulkHandler:
    """
    C-SDK Python Connector for Ingest Bulk.
    """

    _logger = logging.getLogger(__name__)

    def __init__(self, client):
        self._client = client
        self._csdk = None
        self._session_handle = None
        self._session_id = None
        self._seqno = None
        self._op_id = None
        self._num_events = None
        self._bulk_ctx_id = None
        self._batch_data = {}
        self._resources_list = []
        self._options_list = []

    def start_ingest_bulk(
        self,
        csdk,
        session_handle,
        session_id,
        seqno,
        op_id,
        num_events,
        resources,
        options,
    ):
        """C-SDK session preparing the handler for ingest bulk

        Args:
            csdk (_bios_csdk_native): C-SDK native connector
            session_handle (int) : Session Handle
            session_id (int) : Session ID
            seqno (int) : Sequence number that uniquely identifies this ingest bulk async operation
            op_id (CSdkOperationId) : Operation ID
            num_events (int) : total number of ingests in this bulk request

        """
        self._csdk = csdk
        self._session_handle = session_handle
        self._session_id = session_id
        self._seqno = seqno
        self._op_id = op_id
        self._num_events = num_events
        if resources:
            for key, value in resources.items():
                self._resources_list.append(key)
                self._resources_list.append(value)
        if options:
            for key, value in options.items():
                self._options_list.append(key)
                self._options_list.append(value)
        csdk.ingest_bulk_start(session_handle, seqno, session_id, op_id.value, num_events)

    def process_more_request(self, bulk_ctx_id, from_index, to_index):
        """C-SDK asking for more ingest bulk

        Args:
            bulk_ctx_id (int): Bulk Ingest context ID as understood by C-SDK
            from_index (int): Starting index
            to_index (int): Ending index

        """
        if self._bulk_ctx_id is None:
            self._bulk_ctx_id = bulk_ctx_id
        payload = self._client.make_bulk_request(from_index, to_index)
        self._batch_data[from_index] = BatchData(from_index, to_index, payload)
        self._csdk.ingest_bulk(
            self._session_handle,
            self._seqno,
            self._session_id,
            bulk_ctx_id,
            from_index,
            to_index,
            self._resources_list,
            self._options_list,
            payload,
        )

    def process_partial_response(
        self, status_code, reply, endpoint, from_index, start_time, csdk_elapsed
    ):
        """C-SDK pushing the incoming partial response.
        If a valid status (error or otherwise is returned by this method, it implies that this was
        the last expected response.

        Args:
            status_code (int): Response status
            reply (str or bytes): Raw Response
            endpoint: Endpoint
            from_index (int): starting index of the batch ingest

        Returns:
            Tuple of the following when this is the last expected response:
                sequence no
                overall status
                reply
                endpoint
                from_index
                start_time
                internal latency in microseconds
                num_reads (0)
                num_writes (num_events)
            None otherwise.
            reply will be None, except when error, as all responses should be already consumed
            by the client layer
        """
        batch_data = self._batch_data.get(from_index)
        (status, reply) = self._client.process_partial_response(
            status_code,
            reply,
            endpoint,
            batch_data.from_index,
            batch_data.to_index,
        )
        if status is not None:
            self._logger.debug("Notifying bulk completion status=%s", status)
            self._csdk.ingest_bulk_end(
                self._session_handle,
                self._seqno,
                self._session_id,
                self._bulk_ctx_id,
            )
            result = (
                self._seqno,
                status,
                reply,
                endpoint,
                from_index,
                start_time,
                csdk_elapsed,
                0,
                self._num_events,
            )
        else:
            result = None

        if batch_data:
            del self._batch_data[from_index]

        return result
