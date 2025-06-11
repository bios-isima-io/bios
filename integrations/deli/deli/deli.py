#!/usr/bin/env python3
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
"""deli: The deli framework."""

import logging
import os
import time
from configparser import ConfigParser
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler

import bios

from ._version import __version__
from .config_loader import ConfigLoader
from .configuration import Configuration, DataFlowConfiguration, Keyword, SourceType
from .data_feeder import DirectDataFeeder, PipeDataFeeder
from .data_importer import DataImporter
from .data_pipe import DataPipe
from .data_processor import ImportedModules
from .delivery_channel import DeliveryChannel

source_type_to_app_config = {
    SourceType.FACEBOOKAD: ("bios-intg-fb-ads", bios.models.AppType.BATCH),
    SourceType.FILE: ("bios-intg-file", bios.models.AppType.BATCH),
    SourceType.FILETAILER: ("bios-intg-file-tailer", bios.models.AppType.REALTIME),
    SourceType.GOOGLEAD: ("bios-intg-google-ads", bios.models.AppType.BATCH),
    SourceType.HIBERNATE: ("bios-intg-hibernate", bios.models.AppType.REALTIME),
    SourceType.KAFKA: ("bios-intg-kafka", bios.models.AppType.REALTIME),
    SourceType.MAXWELL: ("bios-intg-maxwell", bios.models.AppType.REALTIME),
    SourceType.MYSQLPULL: ("bios-intg-mysql-pull", bios.models.AppType.BATCH),
    SourceType.S3: ("bios-intg-s3", bios.models.AppType.BATCH),
    SourceType.WEBHOOK: ("bios-intg-webhook", bios.models.AppType.REALTIME),
    SourceType.RESTCLIENT: ("bios-intg-rest-client", bios.models.AppType.BATCH),
}


class Deli:
    """The deli framework."""

    INSTANCE = None

    def __init__(self, system_config: ConfigParser, source_type: SourceType = None):
        assert isinstance(system_config, ConfigParser)
        if source_type:
            assert isinstance(source_type, SourceType)
        logging.info("Deli version %s", __version__)
        self.system_config = system_config
        self.config_loader = ConfigLoader.provide_loader(self.system_config)
        self.tenant_config = None
        # Use direct data feeder in case of single source usage
        self.data_feeder = DirectDataFeeder() if source_type else PipeDataFeeder()
        # dict of source ID and the instance, built on demand
        self.data_importers = {}
        # list of data pipes
        self.data_pipes = []
        self.imported_modules = None
        self.source_type = source_type

    def setup(self):
        """Sets up the deli server framework."""
        logging.info("Setting up deli...")
        self.tenant_config = Configuration(self.config_loader.load(), self.system_config)
        self.shutdown()  # necessary for reloading
        self.data_feeder.setup()
        self.imported_modules = ImportedModules(self.tenant_config.get_processor_configs())
        self.imported_modules.load()
        if source_type_to_app_config[self.source_type]:
            app_name, app_type = source_type_to_app_config[self.source_type]
            self.system_config["Common"]["appName"] = app_name
            self.system_config["Common"]["appType"] = app_type.name

        # to handle a case that global initialization is necessary even when there is
        # no configured flow
        if self.source_type:
            DataImporter.global_init(self.source_type, self.system_config)

        # connect flows, destinations, and sources
        for flow_config in self.tenant_config.get_flow_configs():
            try:
                # skip irrelevant flows in case of single source type usage
                if self.source_type:
                    source_config = self.tenant_config.get_source_config(
                        flow_config.get_source_id()
                    )
                    if not source_config or source_config.get_type() is not self.source_type:
                        continue

                if source_config.is_cdc_sink:
                    flow_config.transform_to_cdc_sink_flow()

                self._setup_delivery_channel(flow_config.get_destination_id())
                data_pipe = DataPipe(flow_config, self.tenant_config, self.data_feeder)
                data_importer = self._provide_importer(flow_config.get_source_id(), flow_config)
                data_importer.register(data_pipe, self.imported_modules)
                self.data_pipes.append(data_pipe)
            except Exception as error:
                logging.error("Error in flow config %s: %s", flow_config.get_name(), error)
                raise
        # setup data importers
        for data_importer in self.data_importers.values():
            data_importer.setup()
        logging.info("Importers:")
        for source_id, importer in self.data_importers.items():
            logging.info(
                "  %s: %s: %s",
                source_id,
                type(importer).__name__,
                importer.get_importer_name(),
            )
        logging.info("Delivery Channels:")
        for destination_id, channel in self.data_feeder.get_delivery_channels().items():
            logging.info("  %s: %s", destination_id, type(channel).__name__)
        logging.info("Data Flows:")
        for pipe in self.data_pipes:
            flow_conf = pipe.flow_config
            source_id = flow_conf.get_source_id()
            source_name = self.tenant_config.get_source_config(source_id).get_name() or source_id
            dest_id = flow_conf.get_destination_id()
            dest_name = self.tenant_config.get_destination_config(dest_id).get_name() or dest_id
            logging.info(
                "  %s: <%s> -> <%s> (%s %s)",
                flow_conf.get_name(),
                source_name,
                dest_name,
                flow_conf.get_destination_spec().get_stream_type().name,
                flow_conf.get_destination_spec().get_stream_name(),
            )
        logging.info("Setting up done.")

    def schedule_importers(self):
        """Run importers in polling mode"""
        # if is it polling it shall run each importer and sleep for the polltime before calling
        # the importer again. This functionality should move to base class,
        # but to make backward compatible and allow for migration, need to do it this way
        # only mysql shall use it to begin with
        # mini scheduler
        # initialize the time outs
        timeouts = {}
        for name, importer in self.data_importers.items():
            timeouts[name] = importer.poll_timeout()
        while True:
            importers_times = sorted(timeouts.items(), key=lambda x: x[1])
            sleep_time = importers_times[0][1]
            logging.info("Sleeping for  %d seconds", sleep_time)
            time.sleep(sleep_time)
            # update the sleep times
            for k in timeouts.keys():
                timeouts[k] -= sleep_time
                if timeouts[k] <= 0:
                    timeouts[k] = 0
            # run all  importers which have timedout
            for k in timeouts.keys():
                if timeouts[k] == 0:
                    logging.info("Running Importer %s", k)
                    importer = self.data_importers[k]
                    timeouts[k] = importer.poll_and_return()
                    if timeouts[k] is None:
                        timeouts[k] = importer.poll_timeout()

    def bootstrap(self, is_polling=False):
        """Bootstraps the deli application."""
        for data_pipe in self.data_pipes:
            data_pipe.start()
        # used for multiple importers of the same type
        if is_polling:
            self.schedule_importers()
        else:
            for data_importer in self.data_importers.values():
                # this call blocks for every importer other than webhook
                data_importer.start()
        self.data_feeder.start()

    def shutdown(self):
        """Shuts down the application."""
        for data_importer in self.data_importers.values():
            data_importer.shutdown()
        for data_pipe in self.data_pipes:
            data_pipe.shutdown()
        if self.data_feeder:
            self.data_feeder.stop()

    def get_system_config(self) -> ConfigParser:
        """Returns system global configuration."""
        return self.system_config

    def _provide_importer(
        self, source_id: str, flow_config: DataFlowConfiguration
    ) -> DataImporter:
        assert isinstance(source_id, str)
        assert isinstance(flow_config, DataFlowConfiguration)
        importer = self.data_importers.get(source_id)
        if not importer:
            source_config = self.tenant_config.get_source_config(source_id)
            if not source_config:
                raise Exception(f"Source ID {source_id} not found in {Keyword.IMPORT_SOURCES}")
            importer = DataImporter.build(source_config, flow_config, self.system_config)
            self.data_importers[source_id] = importer
        return importer

    def _setup_delivery_channel(self, destination_id: str) -> DeliveryChannel:
        assert isinstance(destination_id, str)
        delivery_channel = self.data_feeder.get_delivery_channel(destination_id)
        if not delivery_channel:
            destination_config = self.tenant_config.get_destination_config(destination_id)
            if not destination_config:
                raise Exception(
                    f"Destination ID {destination_id} not found in {Keyword.IMPORT_DESTINATIONS}"
                )
            delivery_channel = DeliveryChannel.build(destination_config, self.system_config)
            delivery_channel.setup()
            self.data_feeder.register_delivery_channel(destination_id, delivery_channel)
        return delivery_channel


def run_framework(
    source_type: SourceType = None, is_plugin: bool = False, is_polling: bool = False
):
    """Builds and startup the deli framework.

    Args:
        source_type (SourceType): Specify in case of single source usage
        is_plugin (bool): Whether the framework runs as a plugin
    """
    try:
        deli = prepare_framework(source_type, is_plugin)
        deli.bootstrap(is_polling=is_polling)
    except Exception as err:
        logging.critical("FATAL ERROR: %s", err, exc_info=True)


def prepare_framework(
    source_type: SourceType = None, is_plugin: bool = False, is_polling: bool = False
) -> Deli:
    if source_type:
        assert isinstance(source_type, SourceType)

    deli_home = os.environ.get("DELI_HOME") or "."
    if not os.path.isdir(deli_home):
        raise Exception(f"Directory DELI_HOME={deli_home} does not exist")

    system_config = ConfigParser()
    system_config_file = os.environ.get("DELI_CONFIG") or f"{deli_home}/integrations.ini"
    if len(system_config.read(system_config_file)) == 0:
        logging.error("Failed to load system configuration file %s", system_config_file)
        raise Exception()

    # Configure logging
    log_format = system_config.get(
        "Common",
        "logFormat",
        fallback="%(asctime)s - %(levelname)s [%(name)s] %(message)s",
    )
    log_date_format = system_config.get("Common", "logDateFormat", fallback="%d/%b/%Y:%H:%M:%S %z")

    logging_handlers = None
    if not is_plugin:
        log_file_name = system_config.get(
            "Common", "logFileName", fallback=f"{deli_home}/logs/integrations.log"
        )
        log_file_dir = os.path.dirname(log_file_name)
        if log_file_dir and not os.path.isdir(log_file_dir):
            os.mkdir(log_file_dir)
        logging_handlers = [
            StreamHandler(),
            TimedRotatingFileHandler(log_file_name, when="midnight", interval=1, backupCount=14),
        ]

    logging.basicConfig(
        format=log_format,
        datefmt=log_date_format,
        level=logging.INFO,
        handlers=logging_handlers,
    )

    logging.info("DELI_HOME is set to %s", os.path.abspath(deli_home))

    deli = Deli(system_config, source_type)
    Deli.INSTANCE = deli
    try:
        deli.setup()
    except Exception:  # pylint: disable=broad-except
        logging.error("Setting up failed", exc_info=True)
        raise

    return deli


if __name__ == "__main__":
    run_framework()
