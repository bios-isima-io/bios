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
import json
import logging

import signal
from configparser import ConfigParser
from multiprocessing import Process
from typing import List, Tuple

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from deli.configuration import (
    ConfigUtils,
    DataFlowConfiguration,
    Keyword,
    SourceConfiguration,
    SourceDataSpec,
    SourceType,
)

from .data_importer import DataImporter, EndpointHandler, SourceFilter

from ..utils import coerce_types


class KafkaUtils:
    KW_ARGS_TYPES = {
        "client_id": str,
        "group_id": str,
        "fetch_max_wait_ms": int,
        "fetch_min_bytes": int,
        "fetch_max_bytes": int,
        "max_partition_fetch_bytes": int,
        "request_timeout_ms": int,
        "retry_backoff_ms": int,
        "reconnect_backoff_ms": int,
        "reconnect_backoff_max_ms": int,
        "max_in_flight_requests_per_connection": int,
        "auto_offset_reset": str,
        "enable_auto_commit": bool,
        "auto_commit_interval_ms": int,
        "check_crcs": bool,
        "metadata_max_age_ms": int,
        "max_poll_records": int,
        "max_poll_interval_ms": int,
        "session_timeout_ms": int,
        "heartbeat_interval_ms": int,
        "receive_buffer_bytes": int,
        "send_buffer_bytes": int,
        "sock_chunk_bytes": int,
        "sock_chunk_buffer_count": int,
        "consumer_timeout_ms": float,
        "security_protocol": str,
        "ssl_check_hostname": bool,
        "api_version_auto_timeout_ms": int,
        "connections_max_idle_ms": int,
        "metrics_num_samples": int,
        "metrics_sample_window_ms": int,
        "metric_group_prefix": str,
        "exclude_internal_topics": bool,
    }

    @staticmethod
    def create_admin_client(bootstrap_servers, client_id):
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id=client_id)
        return admin_client

    @staticmethod
    def create_topic(admin_client, topic_name, num_partitions, replication_factor):
        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
        ]
        return admin_client.create_topics(new_topics=topic_list, validate_only=False)

    @staticmethod
    def create_producer(args):
        if not args.get("bootstrap_servers"):
            raise Exception(
                "create_producer: 'bootstrap_servers' is not set. Please check the configuration."
            )
        return KafkaProducer(**args)

    @staticmethod
    def create_consumer(topic_name, args):
        if not args.get("bootstrap_servers"):
            raise Exception(
                "create_consumer: 'bootstrap_servers' is not set. Please check the configuration."
            )
        elif not args.get("group_id"):
            logging.warn("create_consumer: 'group_id' is not set. Offset commits will be disabled")
        return KafkaConsumer(topic_name, **args)


def check_if_empty(param: str, param_val: str):
    if not param_val or not param_val.strip():
        raise Exception("Config parameter: " + param + " :cannot be empty")


class KafkaDataImporter(DataImporter):
    def __init__(
        self,
        config: SourceConfiguration,
        flow_config: DataFlowConfiguration,
        system_config: ConfigParser,
    ):
        super().__init__(config)
        self.config = config
        self.system_config = system_config
        self.source_data_spec = None
        self.count = 0

    def start_importer(self):
        pass

    def shutdown_importer(self):
        pass

    def generate_handler_id(self, source_data_spec: SourceDataSpec) -> str:
        check_if_empty("importSourceId", source_data_spec.get(Keyword.IMPORT_SOURCE_ID))
        check_if_empty("topic", source_data_spec.get(Keyword.TOPIC))
        self.count = self.count + 1
        source_id = source_data_spec.get_or_raise(Keyword.IMPORT_SOURCE_ID)
        topic = source_data_spec.get_or_raise(Keyword.TOPIC)
        group_id = source_data_spec.get_or_raise(Keyword.GROUP_ID)
        return f"{source_id}_{topic}_{group_id}_{self.count}"

    def create_handler(self, handler_id: str, source_data_spec: SourceDataSpec):
        # handler ID is the service path
        return KafkaHandler(self.config, source_data_spec, handler_id)


class KafkaHandler(EndpointHandler):
    def __init__(
        self, config: SourceConfiguration, source_data_spec: SourceDataSpec, handler_id: str
    ):
        super().__init__()
        self.config = config
        self.source_data_spec = source_data_spec
        self._handler_id = handler_id
        self._process = None
        self.source_name = self.config.get_name()

        # Read required import source parameters here to stop the service when any is missing
        self.topic = self.source_data_spec.get_or_raise(Keyword.TOPIC)
        self.group_id = self.source_data_spec.get_or_raise(Keyword.GROUP_ID)
        self.client_id = self.source_data_spec.get_or_raise(Keyword.CLIENT_ID)
        self.payload_type = self.source_data_spec.get_or_raise(Keyword.PAYLOAD_TYPE)

        logger_name = f"{type(self).__name__}:{self.topic}"
        if self.group_id:
            logger_name += f":{self.group_id}"
        self.logger = logging.getLogger(logger_name)
        self.consumer = None
        self._shutdown = False

    def setup_kw_args(self, dest_kw_args):
        kw_args = {
            "max_poll_records": 100,
            "enable_auto_commit": True,
            "auto_offset_reset": "earliest",
        }
        # create a merged dictionary , overide with user supplied values
        kw_args.update(dest_kw_args)

        # typecast for safety
        kw_args = coerce_types(kw_args, KafkaUtils.KW_ARGS_TYPES)
        return kw_args

    def run_subprocess(self):
        try:
            self.subprocess_main()
        except Exception as error:
            self.logger.error("An error encountered in subprocess: %s", error, exc_info=True)
            self.logger.error("Terminating the import handler")

        self.logger.info("Terminated")

    def subprocess_main(self):
        signal.signal(signal.SIGTERM, self._term_handler)
        self._process = None
        source_type = self.config.get(Keyword.TYPE)
        if not str(source_type).upper() == SourceType.KAFKA.name:
            return
        bootstrap_servers = self.config.get(Keyword.BOOTSTRAP_SERVERS)
        api_version = tuple(self.config.get(Keyword.API_VERSION))
        kw_args = self.source_data_spec.get_or_default(Keyword.KW_ARGS, {})
        kw_args = self.setup_kw_args(kw_args)
        kw_args["group_id"] = self.group_id
        kw_args["client_id"] = self.client_id
        payload_type = self.payload_type
        self.is_json = str(payload_type).lower() == Keyword.JSON
        kw_args["bootstrap_servers"] = bootstrap_servers
        kw_args["api_version"] = api_version
        auth = self.config.get(Keyword.AUTHENTICATION)
        auth_type = ConfigUtils.get_property(auth, Keyword.TYPE)
        if str(auth_type).upper() == "SASLPLAINTEXT":
            kw_args[Keyword.SECURITY_PROTOCOL] = Keyword.SASL_PLAINTEXT
            kw_args[Keyword.SASL_MECHANISM] = Keyword.PLAIN
            kw_args[Keyword.SASL_PLAIN_USERNAME] = ConfigUtils.get_property(auth, Keyword.USER)
            kw_args[Keyword.SASL_PLAIN_PASSWORD] = ConfigUtils.get_property(auth, Keyword.PASSWORD)
        self.logger.info("kwArgs: %s", kw_args)
        self.consumer = KafkaUtils.create_consumer(topic_name=self.topic, args=kw_args)

        while not self._shutdown:
            msg_pack = self.consumer.poll(timeout_ms=3000, update_offsets=True)
            published = False
            for topic_partition, records in msg_pack.items():
                num_messages = 0
                num_errors = 0
                if self.is_json:
                    value_records, num_errors = self._parse_json_records(records)
                else:
                    value_records = [record.value for record in records]
                try:
                    self.bulk_publish(value_records)
                except Exception as error:
                    num_errors += 1
                    self.logger.error(
                        "An error encountered while publishing records", exc_info=True
                    )
                num_messages += len(value_records)
                published = True
            if published:
                self.consumer.commit()
                self.logger.info(
                    "Processed %s messages from topic: %s partition %s with %s errors",
                    num_messages,
                    self.topic,
                    topic_partition,
                    num_errors,
                )

    def _term_handler(self, sig, frame):
        self.logger.debug("SHUTDOWN shutdown=%s", self._shutdown)
        self._shutdown = True

    def _parse_json_records(self, records) -> Tuple[List[dict], int]:
        value_records = []
        num_errors = 0
        for record in records:
            message = record.value.decode("utf-8")
            try:
                value_records.append(json.loads(message))
            except Exception as err:
                self.logger.warning(
                    "JSON decoding failed; source=%s, error=%s, record=%s",
                    self.source_name,
                    err,
                    message,
                )
                num_errors += 1
        return value_records, num_errors

    def setup(self):
        pass

    def start(self):
        # create consumer and start polling
        self._process = Process(target=self.run_subprocess)
        self._process.start()

    def shutdown(self):
        self.logger.debug("Shutting down the importer")
        if self._process:
            self._process.terminate()

    def shutdown_await(self):
        if self._process:
            self._process.join()

    def create_source_filter(self, source_data_spec: SourceDataSpec) -> SourceFilter:
        pass


# Register the importer to the factory method in the base class
DataImporter.IMPORTER_BUILDERS[SourceType.KAFKA] = KafkaDataImporter
