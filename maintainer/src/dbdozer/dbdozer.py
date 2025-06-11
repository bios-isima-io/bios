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

"""dbdozer - Schedules and runs DB maintenance"""

import datetime
import logging
import os
import random
import re
import sys
import threading
import time
import traceback
from datetime import timezone
from typing import Dict, List, Tuple

import yaml

from .db_status_collector import DbStatusCollector
from .dbdozer_utils import (
    KEY_DATACENTER,
    KEY_LAST_BACKUP_TIME,
    KEY_LAST_INCR_BACKUP_TIME,
    KEY_NODES,
    enumerate_dcs,
    enumerate_keyspaces,
    make_generic_options,
    make_nodetool_options,
    resolve_keyspace_category,
    run_command,
    run_nodetool,
    setup_logging,
)
from .dozer_conf import DozerConf
from .options import Options
from .scheduler import DozerStatusRecorder


def dump_dozer_status(dozer_status: dict, file_name: str):
    """Dumps dbdozer status to a file"""
    with open(file_name + ".new", "w", encoding="utf-8") as file:
        yaml.dump(dozer_status, file)
    try:
        os.rename(file_name, file_name + ".old")
    except EnvironmentError:
        pass
    os.rename(file_name + ".new", file_name)
    try:
        os.remove(file_name + ".old")
    except EnvironmentError:
        pass


def get_time():
    """Returns: int: Current time in seconds since epoch"""
    return int(time.time())


def repair(options: object, keyspace: str, table: str):
    """Runs repair against a table"""
    logging.debug("--> Repairing %s %s (requested to %s)", keyspace, table, options.host)
    start = time.time()
    result, command, _, stderr = run_nodetool(
        options, "repair", "-full", "--dc-parallel", keyspace, table
    )
    elapsed = str(datetime.timedelta(seconds=time.time() - start))
    logging.info(
        "    ... %s repair %s %s; requested_node=%s, elapsed=%s, stderr=%s",
        "done" if result else "FAILED",
        keyspace,
        table,
        options.host,
        elapsed,
        stderr,
    )
    logging.debug("command was: %s", command)


def compact(options: object, keyspace: str, table: str):
    """Runs compaction against a table"""
    logging.debug("--> Compacting %s %s (requested to %s)", keyspace, table, options.host)
    start = time.time()
    result, command, _, stderr = run_nodetool(options, "compact", keyspace, table)
    elapsed = str(datetime.timedelta(seconds=time.time() - start))
    logging.info(
        "    ... %s compacting %s %s; requested_node=%s, elapsed=%s, stderr=%s",
        "done" if result else "FAILED",
        keyspace,
        table,
        options.host,
        elapsed,
        stderr,
    )
    logging.debug("command was: %s", command)


def do_nodetool_maintenance(
    options: Options, ctx: str, action: str, keyspace: str, table: str = None
):
    """Executes a nodetool maintenance action

    Args:
        options (Options): System options for nodetool
        ctx (str): Log context
        action (str): Nodetool command/action
        keyspace (str): Target keyspace
        table (str): Target table
    """
    start = time.time()
    params = (action, keyspace, table) if table else (action, keyspace)
    result, command, _, stderr = run_nodetool(options, log_context=ctx, *params)
    elapsed = str(datetime.timedelta(seconds=time.time() - start))
    params_str = " ".join(params)
    if result:
        logging.info(
            "    [%s] .. done (%s): %s; elapsed=%s", ctx, options.host, params_str, elapsed
        )
    else:
        logging.info(
            "    [%s] .. FAIL (%s): %s; elapsed=%s, stderr=%s",
            ctx,
            options.host,
            params_str,
            elapsed,
            stderr,
        )
    logging.debug("command was: %s", command)


def do_nodetool_prepjob(node: str, dozer_conf: DozerConf, *params):
    options = make_nodetool_options(node, dozer_conf)
    result, _, stdout, stderr = run_nodetool(options, *params)
    if not result:
        raise RuntimeError(
            f"nodetool {params[0]} failed for {stdout or stderr}. unable to continue"
        )


def get_backup_status(options: object, keyspace: str, status: object):
    # return object specifying if the backup is needed or not
    backup_status = {"required": False, "type": None}
    seconds_in_day = 60 * 60 * 24
    if not keyspace in options.backup_keyspaces:
        logging.info("Keyspace %s is not in the backup keyspace list, skipping", keyspace)
        return backup_status

    # give default, value else it shall bomb, during migration
    last_snap_backup = int(status.get(KEY_LAST_BACKUP_TIME, 0))
    last_incr_backup = int(status.get(KEY_LAST_INCR_BACKUP_TIME, 0))
    now = get_time()
    today = datetime.datetime.fromtimestamp(now, timezone.utc)
    weekday = today.weekday()
    seconds_since_last_snapshot = now - last_snap_backup
    seconds_since_any_backup = now - max(last_snap_backup, last_incr_backup)
    # if the difference between last backup and now is greater than a day
    if seconds_since_last_snapshot > seconds_in_day:
        # if snapshot day or snapshot is more than 4 days older, safety check
        if weekday in options.backup_days or (seconds_since_last_snapshot > (4 * seconds_in_day)):
            backup_status["type"] = "snap"
            backup_status["required"] = True
        else:
            # this prevents any aggressive incremental backups
            if seconds_since_any_backup > seconds_in_day:
                backup_status["type"] = "incr"
                backup_status["required"] = True
    else:
        backup_status["required"] = False
    if not backup_status["required"]:
        logging.info("Keyspace %s is not in the backup schedule, skipping", keyspace)
    return backup_status


def run_backup_schedule(options: object, keyspace: str, status: object, all_nodes: List[str]):
    bkup_status = get_backup_status(options, keyspace, status)
    if not bkup_status["required"]:
        return
    # Create a backup ID ... All table bacups in the keyspace will use this common ID
    now = get_time()
    # form the backup id for this complete keyspace for all nodes
    backup_id = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
    bkup_type = bkup_status["type"]
    backup(options, backup_id, bkup_type, keyspace, all_nodes)
    if bkup_type == "snap":
        status[KEY_LAST_BACKUP_TIME] = now
    elif bkup_type == "incr":
        status[KEY_LAST_INCR_BACKUP_TIME] = now
    else:
        assert False


def backup(options: object, backup_id: str, backup_type: str, keyspace: str, all_nodes: List[str]):
    if not keyspace in options.backup_keyspaces:
        logging.info("Keyspace %s is not in the backup keyspace list, skipping", keyspace)
        return
    logging.info("--> STARTED backing up keyspace %s, backup ID=%s", keyspace, backup_id)
    for node in all_nodes:
        start = time.time()
        logging.info("    Backing up keyspace %s on node %s", keyspace, node)
        command = [
            "ssh",
            "-i",
            options.keypair_file,
            "-o",
            "StrictHostKeyChecking=no",
            f"{options.ssh_user}@{node}",
            *options.backup_command.split(),
            "-s",
            backup_id,
            "-t",
            backup_type,
            "-k",
            keyspace,
        ]
        result, executed, _, stderr = run_command(options, *command)
        elapsed = str(datetime.timedelta(seconds=time.time() - start))
        logging.info(
            "    %s snapshotting %s; elapsed=%s, stderr=%s",
            "done" if result else "FAILED",
            keyspace,
            elapsed,
            stderr.replace("\n", "\\n"),
        )
        logging.debug("command was: %s", executed)
    logging.info("    ... COMPLETED backing up keyspace %s", keyspace)


def dbdoze(dozer_conf_file_name):
    """dbdozer main program"""
    dozer_conf = DozerConf(dozer_conf_file_name)
    initial_options = make_generic_options(dozer_conf)

    if initial_options.logfile:
        dirname = os.path.dirname(initial_options.logfile)
        if dirname:
            try:
                os.mkdir(dirname)
            except FileExistsError:
                pass
    setup_logging(initial_options)

    db_status_collector = DbStatusCollector(dozer_conf)
    try:
        dbdoze_core(dozer_conf_file_name, dozer_conf, initial_options, db_status_collector)
    finally:
        db_status_collector.stop()


def dbdoze_core(
    dozer_conf_file_name: str, dozer_conf: DozerConf, initial_options, db_status_collector
):
    """Executes db maintenance"""

    logging.info("################### Running dbdozer with following config: \n%s", dozer_conf)

    logging.info("")
    logging.info("******** Start dbdozing!!! ********")

    # Loads maintenance statuses of keyspaces recorded in the status file by the last execution
    logging.info("**** Loading the initial maintenance states")
    dirname = os.path.dirname(dozer_conf.progress_filename)
    if dirname:
        try:
            os.mkdir(dirname)
        except FileExistsError:
            pass
    dozer_status_recorder = DozerStatusRecorder(dozer_conf=dozer_conf)
    db_status_collector.attach_dozer_status_recorder(dozer_status_recorder)

    last_system_update = 0
    # set of <keyspace>.<table> handled in one-shot operation
    one_shot_handled = set()

    while True:
        # Finds available db nodes grouped by datacenter
        now = get_time()
        if now > last_system_update + 24 * 3600:
            logging.info("**** Maintenance environment update started")
            datacenters, all_nodes = get_datacenters(initial_options, dozer_conf)

            db_status_collector.set_nodes(all_nodes)
            if not db_status_collector.is_started():
                db_status_collector.start()

            # Finds keyspaces in the DB
            logging.info("**** Discovering keyspaces")
            tables_by_keyspace = enumerate_keyspaces(initial_options)
            logging.info("Found %s keyspaces", len(tables_by_keyspace))
            for keyspace in sorted(tables_by_keyspace.keys()):
                logging.info("  %s : %s", resolve_keyspace_category(keyspace).name, keyspace)

            if dozer_conf.one_shot:
                validate_one_shot_config(dozer_conf, tables_by_keyspace)
                logging.info("**** Running in one-shot mode, only following tables are handled:")
                for entry in dozer_conf.one_shot:
                    logging.info(
                        "       keyspace=%s, table=%s",
                        entry[DozerConf.KEYSPACE],
                        entry[DozerConf.TABLE],
                    )

            dozer_status_recorder.update_available_keyspaces(tables_by_keyspace.keys())
            last_system_update = now

        now = get_time()

        if dozer_conf.one_shot:
            keyspace, last_table = get_next_keyspace(dozer_conf, one_shot_handled)
            if keyspace is None:
                logging.info("################### DONE All one-shot maintenances, suspending")
                while True:
                    time.sleep(60)
            next_repair_time = now
        else:
            keyspace, last_table, next_repair_time = dozer_status_recorder.get_next_keyspace()
        if not keyspace:
            logging.warning("No keyspaces to repair. Sleeping for an hour")
            last_system_update = 0
            time.sleep(3600)
            continue

        first = True
        while next_repair_time > now:
            if first:
                logging.info(
                    "sleeping until %s (next=%s)",
                    time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(next_repair_time)),
                    keyspace,
                )
                first = False
            time.sleep(min(next_repair_time - now, 10))
            may_restart_cassandra(dozer_status_recorder, db_status_collector)
            now = get_time()

        maintain_keyspace(
            dozer_conf_file_name,
            dozer_conf,
            dozer_status_recorder,
            db_status_collector,
            all_nodes,
            datacenters,
            tables_by_keyspace,
            keyspace,
            last_table,
            one_shot_handled,
        )


def validate_one_shot_config(dozer_conf: DozerConf, tables_by_keyspace: dict):
    for entry in dozer_conf.one_shot or []:
        keyspace = entry.get(DozerConf.KEYSPACE)
        table = entry.get(DozerConf.TABLE)
        if keyspace not in tables_by_keyspace:
            raise RuntimeError(
                f"{keyspace}: Non-existing keyspace is specified in on-shot execution"
            )
        if table not in tables_by_keyspace[keyspace]:
            raise RuntimeError(
                f"{keyspace}.{table}: Non-existing table is specified in one-shot execution"
            )


def get_next_keyspace(dozer_conf: DozerConf, one_shot_handled: set) -> Tuple[str, str]:
    """Determine the next keyspace and table to handle in one-shot"""
    if not dozer_conf.one_shot:
        return None, None
    for entry in dozer_conf.one_shot:
        keyspace = entry.get(DozerConf.KEYSPACE)
        table = entry.get(DozerConf.TABLE)
        table_full_name = f"{keyspace}.{table}"
        if table_full_name not in one_shot_handled:
            return keyspace, table
    return None, None


def maintain_keyspace(
    dozer_conf_file_name: str,
    dozer_conf: DozerConf,
    scheduler: DozerStatusRecorder,
    db_status_collector: DbStatusCollector,
    all_nodes: List[str],
    datacenters: dict,
    tables_by_keyspace: Dict[str, List[str]],
    keyspace: str,
    last_table: str,
    one_shot_handled: set,
):
    """Maintain a keyspace"""
    logging.info("==== keyspace=%s: maintenance started =========================", keyspace)
    scheduler.record_maintenance_started(keyspace, get_time())

    if dozer_conf.one_shot:
        tables = [
            entry[DozerConf.TABLE]
            for entry in dozer_conf.one_shot
            if entry[DozerConf.KEYSPACE] == keyspace
        ]
    else:
        tables = sorted(tables_by_keyspace[keyspace])
    num_tables = len(tables)

    # start with the next table to the one maintained in the last execution
    offset = 0
    for offset, table in enumerate(tables):
        if table > last_table:
            tables = tables[offset:]
            break

    for table_index, table in enumerate(tables):
        may_restart_cassandra(scheduler, db_status_collector)
        start = time.time()
        logging.info(
            "[%s.%s] table maintenance started (%s/%s) ----------------------",
            keyspace,
            table,
            table_index + offset + 1,
            num_tables,
        )

        # Mark the last table before the maintenance starts. If the tool crashes during
        # the maintenance, maintenance after restart would begin with the next table
        scheduler.mark_last_table(keyspace, table)

        try:
            maintain_table(
                dozer_conf,
                scheduler,
                db_status_collector,
                all_nodes,
                datacenters,
                keyspace,
                table,
            )
            elapsed = str(datetime.timedelta(seconds=time.time() - start))
            logging.info(
                "[%s.%s] table maintenance finished; elapsed=%s ----------------------",
                keyspace,
                table,
                elapsed,
            )
            one_shot_handled.add(f"{keyspace}.{table}")
        except Exception:
            elapsed = str(datetime.timedelta(seconds=time.time() - start))
            logging.error(
                "[%s.%s] table maintenance aborted; elapsed=%s ----------------------",
                keyspace,
                table,
                elapsed,
                exc_info=True,
            )

        # Check the config for each table iteration
        dozer_conf_new = DozerConf(dozer_conf_file_name)
        if dozer_conf_new.__dict__ != dozer_conf.__dict__:
            logging.info("")
            logging.info("################### Configuration changed: \n%s", dozer_conf_new)
            dozer_conf = dozer_conf_new
            scheduler.set_failure_backoff_seconds(dozer_conf.failure_backoff_seconds)
            db_status_collector.update_dozer_conf(dozer_conf)

    logging.info("==== keyspace=%s: maintenance finished =========================", keyspace)
    scheduler.record_maintenance_finished(keyspace, get_time())


def may_restart_cassandra(scheduler: DozerStatusRecorder, db_status_collector: DbStatusCollector):
    """Restart cassandra when it is time"""
    if scheduler.is_restart_time():
        db_status_collector.restart_cassandra_nodes()
        scheduler.record_restart_time(get_time())


def get_datacenters(initial_options, dozer_conf: DozerConf) -> Tuple[dict, List[str]]:
    """Get datacenter information"""
    start = time.time()
    retry = ""
    while True:
        logging.info("**** %sLooking for available DB nodes", retry)
        datacenters = enumerate_dcs(initial_options)
        logging.info("Found %d datacenters:", len(datacenters))
        all_nodes = []
        for datacenter in datacenters:
            logging.info("  %s: %s", datacenter[KEY_DATACENTER], datacenter[KEY_NODES])
            all_nodes.extend(datacenter[KEY_NODES])

        all_configured_nodes = dozer_conf.nodetool_initial_hosts
        for configured_node in all_configured_nodes:
            if configured_node not in all_nodes:
                logging.error("Configured node %s is unavailable", configured_node)
                retry = "(retry) "
        if len(retry) == 0:
            break
        retry = ""
        if time.time() - start < 900:  # try up to 15 minutes
            logging.info("Retrying to fetch DB nodes after a minute")
            time.sleep(60)

    return datacenters, all_nodes


def maintain_table(
    dozer_conf: DozerConf,
    scheduler: DozerStatusRecorder,
    db_status_collector: DbStatusCollector,
    all_nodes: List[str],
    datacenters: dict,
    keyspace: str,
    table: str,
):
    """Maintains a table."""
    # disable auto compaction first in all nodes
    for node in all_nodes:
        do_nodetool_prepjob(node, dozer_conf, "disableautocompaction", keyspace, table)

    try:
        options = make_nodetool_options(random.choice(all_nodes), dozer_conf)
        repair(options, keyspace, table)
        may_restart_cassandra(scheduler, db_status_collector)
        threads = []
        for dc_info in datacenters:
            datacenter = dc_info.get(KEY_DATACENTER)
            nodes = dc_info.get(KEY_NODES)
            thread = threading.Thread(
                target=maintain_node,
                args=(dozer_conf, datacenter, nodes, keyspace, table),
            )
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        logging.info("[%s.%s] completing the maintenance", keyspace, table)
        for node in all_nodes:
            options = make_nodetool_options(node, dozer_conf)
            run_nodetool(options, "enableautocompaction", keyspace, table)


def maintain_node(
    dozer_conf: DozerConf, datacenter: str, nodes: List[str], keyspace: str, table: str
):
    """Runs maintenance per node sequentially to all nodes in the data center"""
    logging.info(
        "  [%s: %s.%s] per-datacenter maintenance started",
        datacenter,
        keyspace,
        table,
    )
    for node in nodes:
        try:
            options = make_nodetool_options(node, dozer_conf)
            do_nodetool_maintenance(options, datacenter, "flush", keyspace, table)
            # do_nodetool_maintenance(options, datacenter, "clearsnapshot", keyspace, table)
            if dozer_conf.garbagecollect_enabled:
                do_nodetool_maintenance(options, datacenter, "garbagecollect", keyspace, table)
            do_nodetool_maintenance(options, datacenter, "compact", keyspace, table)
        except Exception:
            logging.warning(
                "Error happened during a per-node maintenance; keyspace=%s, table=%s, node=%s",
                keyspace,
                table,
                node,
                exc_info=True,
            )
    logging.info(
        "  [%s: %s.%s] per-datacenter maintenance finished",
        datacenter,
        keyspace,
        table,
    )


def main() -> int:
    """The main program"""
    argv = sys.argv
    if len(argv) < 2:
        print(f"Usage: {os.path.basename(argv[0])} <dbdozer.yaml>")
        sys.exit(1)

    # os.chdir(os.path.dirname(argv[0]))

    status = 0
    try:
        dbdoze(argv[1])
    except Exception:
        logging.error("Fatal error\n%s", re.sub(r"-pw \S+", "-pw *****", traceback.format_exc()))
        status = 1
    finally:
        logging.error("dbdozer has stopped")
    return status


if __name__ == "__main__":
    sys.exit(main())
