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
"""DbDozer utilities"""

import logging
import logging.handlers
import os
import random
import re
import subprocess
from typing import Any, Dict, List, Tuple

import yaml

from .db_client import DbClient
from .dozer_conf import DozerConf
from .keyspace_category import KeyspaceCategory
from .options import Options

KEY_CONTEXT_NAME = "context_name"
KEY_CONTEXT_STATUS = "context_status"
KEY_DATACENTER = "datacenter"
KEY_DB_RESTART = "db_restart"
KEY_KEYSPACE_NAME = "keyspace_name"
KEY_NAME = "name"
KEY_NODES = "nodes"
KEY_LAST_BACKUP_TIME = "last_backup_time"
KEY_LAST_FINISHED = "last_finished"
KEY_LAST_INCR_BACKUP_TIME = "last_incr_backup_time"
KEY_LAST_STARTED = "last_started"
KEY_LAST_RESTARTED = "last_restarted"
KEY_LAST_TABLE = "last_table"
KEY_LAST_UPDATE = "last_update"
KEY_STATUS = "status"
KEY_TABLE_INFO_COLLECTION = "table_info_collection"
KEY_TABLE_NAME = "table_name"
KEY_TENANT_NAME = "tenant_name"

VALUE_IN_PROGRESS = "in_progress"
VALUE_DONE = "done"


def setup_logging(options):
    """Sets up logging in a syslog format by log level
    :param options: options as returned by the OptionParser
    """
    stderr_log_format = (
        "%(levelname) -6s %(asctime)s %(funcName) -20s line:%(lineno) -5d: %(message)s"
    )
    file_log_format = "%(asctime)s.%(msecs)03d - %(levelname)s [%(name)s] %(message)s"
    if options.debug:
        level = logging.DEBUG
    elif options.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    handlers = []
    if options.syslog:
        handlers.append(logging.handlers.SysLogHandler(facility=options.syslog))
        # Use standard format here because timestamp and level will be added by syslogd.
    if options.logfile:
        log_backup_count = options.log_backup_count or 1
        if options.log_rotate_size:
            handler = logging.handlers.RotatingFileHandler(
                options.logfile, maxBytes=options.log_rotate_size, backupCount=log_backup_count
            )
        else:
            log_rotate_days = options.log_rotation_days or 1
            handler = logging.handlers.TimedRotatingFileHandler(
                options.logfile,
                when="midnight",
                interval=log_rotate_days,
                backupCount=log_backup_count,
            )
        handler.setFormatter(logging.Formatter(file_log_format))
        handlers.append(handler)
    if not handlers:
        handlers.append(logging.StreamHandler())
        handlers[0].setFormatter(logging.Formatter(stderr_log_format))
    logging.basicConfig(level=level, handlers=handlers)


def make_generic_options(dozer_conf: DozerConf) -> Options:
    options = Options()
    setattr(options, "nodetool", dozer_conf.nodetool_path)
    setattr(options, "hosts", dozer_conf.nodetool_initial_hosts)
    setattr(options, "port", dozer_conf.nodetool_initial_port)
    setattr(options, "ssl", dozer_conf.nodetool_use_ssl)
    setattr(options, "username", dozer_conf.username)
    setattr(options, "password", dozer_conf.password)
    setattr(options, "debug", dozer_conf.debug)
    setattr(options, "verbose", True)
    setattr(options, "syslog", None)
    setattr(options, "logfile", dozer_conf.log_filename)
    setattr(options, "log_rotate_size", dozer_conf.log_rotate_size)
    setattr(options, "log_backup_count", dozer_conf.log_backup_count)
    setattr(options, "exclude_keyspaces", dozer_conf.exclude_keyspaces)
    setattr(options, "backup_keyspaces", dozer_conf.backup_keyspaces)
    setattr(options, "backup_days", dozer_conf.backup_days)
    setattr(options, "backup_command", dozer_conf.backup_command)
    setattr(options, "keypair_file", dozer_conf.keypair_file)
    setattr(options, "ssh_user", dozer_conf.ssh_user)
    setattr(options, "command_timeout", dozer_conf.command_timeout)
    setattr(options, "dry_run", dozer_conf.dry_run)
    setattr(options, "quiet", False)
    return options


def make_nodetool_options(host: str, dozer_conf: DozerConf) -> Options:
    """Makes an options object used for running nodetool"""
    options = Options()
    setattr(options, "host", host)
    setattr(options, "port", dozer_conf.nodetool_initial_port)
    setattr(options, "ssl", dozer_conf.nodetool_use_ssl)
    setattr(options, "username", dozer_conf.username)
    setattr(options, "password", dozer_conf.password)
    setattr(options, "verbose", 2)
    setattr(options, "steps", dozer_conf.steps_per_token)
    setattr(options, "workers", dozer_conf.concurrency)
    setattr(options, "nodetool", dozer_conf.nodetool_path)
    setattr(options, "debug", dozer_conf.debug)
    setattr(options, "dry_run", dozer_conf.dry_run)
    setattr(options, "max_tries", 1)
    setattr(options, "initial_sleep", 1)
    setattr(options, "sleep_factor", 2)
    setattr(options, "max_sleep", 1800)
    setattr(options, "command_timeout", dozer_conf.command_timeout)
    setattr(options, "quiet", False)
    return options


def get_command(options, *args, host=None):
    cmd = [options.nodetool]
    if host is None:
        host = options.host
    cmd.extend(["-h", host, "-p", options.port])
    if options.ssl:
        cmd.append("--ssl")
    if options.username and options.password:
        cmd.extend(["-u", options.username, "-pw", options.password])
    cmd.extend(args)
    return cmd


def run_command(options, *command):
    """Execute a shell command and return the output
    :param command: the command to be run and all of the arguments
    :returns: success_boolean, command_string, stdout, stderr
    """
    cmd = " ".join(map(str, command))
    timeout = options.command_timeout if options else None
    logging.debug("run_command (timeout=%s): %s", timeout, re.sub(r"-pw \S+", "-pw *****", cmd))
    with subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    ) as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as err:
            proc.kill()
            return False, cmd, "", str(err)
        return proc.returncode == 0, cmd, stdout, stderr


def run_ssh(options: Options, host: str, *command):
    ssh_command = [
        "ssh",
        "-i",
        options.keypair_file,
        "-o",
        "StrictHostKeyChecking=no",
        f"{options.ssh_user}@{host}",
        *command,
    ]
    return run_command(options, *ssh_command)


def run_scp(options: Options, host: str, source_file, target_dir):
    """Copies the source file to the target directory on remote host"""
    scp_command = [
        "scp",
        "-i",
        options.keypair_file,
        " -o",
        "StrictHostKeyChecking=no",
        source_file,
        f"{options.ssh_user}@{host}:{target_dir}/",
    ]
    return run_command(options, *scp_command)


def run_nodetool(options: Options, *command, log_context=None) -> Tuple[bool, str, str, str]:
    """Runs nodetool command"""
    cmd = get_command(options, *command)
    ctx = f"[{log_context}] " if log_context else ""
    if not options.quiet:
        logging.info("    %srunning (%s): %s", ctx, options.host, " ".join(map(str, command)))
    result, command_string, stdout, stderr = run_command(options, *cmd)
    return result, command_string, stdout, stderr


def enumerate_dcs(options):
    all_hosts = options.hosts.copy()
    random.shuffle(all_hosts)
    for index, host in enumerate(all_hosts):
        logging.info("running nodetool status against %s", host)
        cmd = get_command(options, "status", host=host)
        success, _, stdout, stderr = run_command(options, *cmd)
        if success:
            break
        if index < len(all_hosts) - 1:
            logging.warning(
                "Failed to run nodetool status; host=%s, stdout=%s, stderr=%s",
                host,
                stdout,
                stderr,
            )
            continue
        raise RuntimeError(f"Died in enumerate_dcs because: {stdout}\n{stderr}")

    pattern_dc = re.compile(r"Datacenter\s*:\s*(\S+)")
    pattern_node = re.compile(r"UN\s+(\S+)\s.*")
    datacenters = []
    dc_name = None
    nodes = []
    for line in stdout.split("\n"):
        m_dc = pattern_dc.match(line)
        if m_dc:
            if dc_name:
                datacenters.append({KEY_DATACENTER: dc_name, KEY_NODES: nodes})
            dc_name = m_dc.group(1)
            nodes = []
            logging.debug("datacenter=%s", dc_name)
        else:
            m_node = pattern_node.match(line)
            if m_node:
                node = m_node.group(1)
                nodes.append(node)
                logging.debug("  node=%s", node)
    if dc_name:
        datacenters.append({KEY_DATACENTER: dc_name, KEY_NODES: nodes})

    return datacenters


def collect_table_info(
    dozer_conf: DozerConf, nodes: List[str]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Collects table info from specified nodes"""
    result = {}
    with DbClient(dozer_conf) as db_client:
        for node in nodes:
            node_info = get_table_info_in_node(dozer_conf, node, db_client)
            result[node] = node_info
    return result


def get_table_info_in_node(dozer_conf: DozerConf, node: str, db_client: DbClient) -> dict:
    """Gets all table info."""
    # get table info using nodetool
    options = make_generic_options(dozer_conf)
    options.hosts = [node]
    table_info = enumerate_keyspaces(options, detail=True)
    # get table metadata from DB
    comment_pattern = re.compile(
        r".*tenant=(\S+) .*(signal|context|rollup|index|view)=(\S+) \(.+ersion=([0-9]+)\)$"
    )
    known_keyspaces = {
        "tfos_admin": "_system",
        "tfos_bi_meta": "_system",
        "system_auth": "_db_system",
        "system_traces": "_db_system",
        "system_distributed": "_db_system",
    }
    for keyspace, tables in table_info.items():
        tenant = known_keyspaces.get(keyspace)
        for table, table_params in tables.items():
            table_options = db_client.get_table_options(keyspace, table)
            if not table_options:
                continue
            table_params["ttl"] = table_options.get("default_time_to_live")
            table_params["gcGraceSeconds"] = table_options.get("gc_grace_seconds")
            _parse_table_comment(table_options, comment_pattern, table_params)
            if not tenant and table_params.get("tenant"):
                tenant = table_params.get("tenant")
            table_params["tenant"] = tenant
    return table_info


def _parse_table_comment(table_options: dict, comment_pattern: re.Pattern, table_info: dict):
    comment = table_options.get("comment")
    if not comment:
        return
    m = comment_pattern.match(comment)
    if m:
        table_info["tenant"] = m.group(1)
        table_info["streamType"] = m.group(2)
        table_info["stream"] = m.group(3)
        table_info["version"] = m.group(4)


def enumerate_keyspaces(options, detail: bool = False) -> Dict[str, List[str]]:
    """Get a dict of all keyspaces and their column families.
    :param options: OptionParser result
    :returns: Dictionary of keyspace and table names
    """
    all_hosts = options.hosts.copy()
    random.shuffle(all_hosts)
    for index, host in enumerate(all_hosts):
        logging.info("running nodetool tablestats")
        cmd = get_command(options, "tablestats", host=host)
        success, _, stdout, stderr = run_command(options, *cmd)
        if success:
            break
        if index < len(all_hosts) - 1:
            logging.warning(
                "Failed to run nodetool status; host=%s, stdout=%s, stderr=%s",
                host,
                stdout,
                stderr,
            )
            continue
        raise RuntimeError("Died in enumerate_keyspaces because: " + stderr)

    logging.debug("tablestats retrieved, parsing output to retrieve keyspaces")
    return parse_tablestats(stdout, options.exclude_keyspaces, detail=detail)


def parse_tablestats(
    tablestats_output: str, keyspaces_to_exclude: List[str], detail: bool = False
) -> Dict[str, Any]:
    """Parsees nodetool tablestats output."""

    prop_to_attr = {
        "Table": ("table", str),
        "SSTable count": ("sstables", int),
        "Space used (live)": ("spaceLive", int),
        "Space used (total)": ("spaceTotal", int),
        "Space used by snapshots (total)": ("spaceSnapshots", int),
        "Off heap memory used (total)": ("totalOffHeapMemory", int),
        "SSTable Compression Ratio": ("sstableCompressionRatio", float),
        "Number of partitions (estimate)": ("partitions", int),
        "Memtable cell count": ("memtableCells", int),
        "Memtable data size": ("memtableDataSize", int),
        "Memtable off heap memory used": ("memtableOffHeapMemory", int),
        "Memtable switch count": ("memtableSwitches", int),
        "Local read count": ("localReads", int),
        "Local read latency": ("localReadLatency", _parse_latency),
        "Local write count": ("localWrites", int),
        "Local write latency": ("localWriteLatency", _parse_latency),
        "Pending flushes": ("pendingFlushes", int),
        "Percent repaired": ("percentRepaired", float),
        "Bloom filter false positives": ("bloomFilterFalsePositives", int),
        "Bloom filter false ratio": ("bloomFilterFalseRatio", float),
        "Bloom filter space used": ("bloomFilterSpaceUsed", int),
        "Bloom filter off heap memory used": ("bloomFilterOffHeapMemory", int),
        "Index summary off heap memory used": ("indexSummaryOffHeapMemory", int),
        "Compression metadata off heap memory used": ("compressionMetadataOffHeapMemory", int),
        "Compacted partition minimum bytes": ("compactedPartitionMinimumBytes", int),
        "Compacted partition maximum bytes": ("compactedPartitionMaximumBytes", int),
        "Compacted partition mean bytes": ("compactedPartitionMeanBytes", int),
        "Average live cells per slice (last five minutes)": ("averageLiveCellsPerSlice", float),
        "Maximum live cells per slice (last five minutes)": ("maximumLiveCellsPerSlice", int),
        "Average tombstones per slice (last five minutes)": ("averageTombstonesPerSlice", float),
        "Maximum tombstones per slice (last five minutes)": ("maximumTombstonesPerSlice", int),
        "Dropped Mutations": ("droppedMutations", int),
    }

    keyspaces = {}
    keyspace = None
    table = None
    pattern_keyspace = re.compile(r"\s*Keyspace\s*:\s*(\S+)")
    pattern_table = re.compile(r"\s*Table\s*:\s*(\S+)")
    pattern_prop = re.compile(r"\s*(\S+.*):\s*(\S+.*)")
    for line in tablestats_output.split("\n"):
        m = pattern_keyspace.match(line)
        if m:
            keyspace = m.group(1)
            keyspaces[keyspace] = {} if detail else []
            table = None
        else:
            m = pattern_table.match(line)
            if m:
                table = m.group(1)
                if detail:
                    keyspaces[keyspace][table] = {}
                else:
                    keyspaces[keyspace].append(table)
            elif detail and table is not None:
                m = pattern_prop.match(line)
                if m:
                    mapping = prop_to_attr.get(m.group(1))
                    if not mapping:
                        continue
                    name = mapping[0]
                    try:
                        value = mapping[1](m.group(2))
                        if value != value:  # nan
                            value = None
                    except ValueError:
                        value = None
                    if name:
                        keyspaces[keyspace][table][name] = value

    for exclusion in keyspaces_to_exclude:
        logging.debug("exclusion=%s", exclusion)
        keyspaces.pop(exclusion, None)
    return keyspaces


def _parse_latency(src: str) -> int:
    """Parses a latency value in tablestats response and returns the value in microsecond"""
    elements = src.split(" ")
    try:
        value = float(elements[0])
    except ValueError:
        return None
    unit = elements[1]
    if unit == "ms":
        value *= 1000
    else:
        raise ValueError(f"Unknown unit: src={src}")
    return int(value)


def resolve_keyspace_category(keyspace_name: str) -> KeyspaceCategory:
    """Resolves KeyspaceCategory from a keyspace name"""
    if keyspace_name.startswith("tfos_d_"):
        return KeyspaceCategory.KEYSPACE_DATA
    elif keyspace_name.startswith("tfos_"):
        return KeyspaceCategory.KEYSPACE_ADMIN
    else:
        return KeyspaceCategory.KEYSPACE_MISC


def dump_dict_to_file(dict_data: dict, file_name: str):
    """Dumps a dict data to a file"""
    with open(file_name + ".new", "w") as file:
        yaml.dump(dict_data, file)
    try:
        os.rename(file_name, file_name + ".old")
    except EnvironmentError:
        pass
    os.rename(file_name + ".new", file_name)
    try:
        os.remove(file_name + ".old")
    except EnvironmentError:
        pass
