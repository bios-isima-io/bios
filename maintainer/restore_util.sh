#!/usr/bin/env bash
KEYSPACES_TABLES_NAMES=/tmp/keyspace_tables.txt

function db_datadir {
 declare -a data_file_dirs=("/mnt/disks/data1/data/data1" "/mnt/disks/data2/data/data2")
 seqno=$1
 index=$(($seqno - 1))
 echo "${data_file_dirs[$index]}"
}

function db_numdatadirs {
  declare -a data_file_dirs=("/mnt/disks/data1/data/data1" "/mnt/disks/data2/data/data2")
  echo "${#data_file_dirs[@]}"
}


function list_tablenames {
 
  local keyspace=$1
  : > $KEYSPACES_TABLES_NAMES
  local USAGE="$FUNCNAME <keyspace>"
  numdirs=$(db_numdatadirs)
  for idx in $(seq 1 $numdirs)
  do
    if [[ ! -d $(db_datadir $idx)/$keyspace ]]; then
       echo "unknown keyspace $keyspace"
       echo "$USAGE"
       return
    fi
  done
  for idx in $(seq 1 $numdirs)
  do
    ( cd $(db_datadir $idx)/$keyspace;
      ls -1 $PWD >> $KEYSPACES_TABLES_NAMES
    )
  done
  echo $(cat $KEYSPACES_TABLES_NAMES | sort | uniq ) 
  return
}

function is_keyspace {
  for idx in $(seq 1 $numdirs)
  do
    if [[ ! -d $(db_datadir $idx)/$1 ]]; then
       return 1
    fi
  done
  return 0 
}

function restore_keyspace {

    local   keyspace_name="$1"
    local backupdir="${2}"
    local USAGE="$FUNCNAME <keyspace> <backupdir>"
    set -x
    if ! [[ -d "${backupdir}" ]]; then 
      echo -e "\nERROR: Backup not found at '${backupdir}'";
      echo $USAGE;
      return 1; 
    fi
    if  ! is_keyspace $keyspace_name ; then
      echo -e "\nERROR: Keyspace path '${keyspace_name}' is not valid";
      echo $USAGE;
      return 1; 
    fi
    numdirs=$(db_numdatadirs)
    keyspace_tables=$(mktemp)
    backup_tablenames=$(mktemp)
    for idx in $(seq 1 $numdirs)
    do
      keyspace_path=$(db_datadir $idx)/$keyspace_name
      find "${keyspace_path}/" -maxdepth 1 -mindepth 1 -type d -fprintf ${keyspace_tables}.$idx "%f\n"
      find "${backupdir}/data$idx/$keyspace_name" -maxdepth 1 -mindepth 1 -type d -fprintf ${backup_tablenames}.$idx "%f\n"
    done
    cat ${keyspace_tables}.* > ${keyspace_tables}
    cat ${backup_tablenames}.* > ${backup_tablenames}
    sort -u -o ${keyspace_tables} ${keyspace_tables}
    sort -u -o ${backup_tablenames} ${backup_tablenames}
    keyspace_tablenames=$(mktemp)
    table_names=()
    table_uuids=()
    while IFS= read -r table
    do
      str=${table##*/}
      uuid=${str##*-}
      len=$((${#str} - ${#uuid} - 1))
      name=${str:0:${len}}
      echo "${name}" >> ${keyspace_tablenames}
      table_names+=(${name})
      table_uuids+=(${uuid})
    done < ${keyspace_tables}
    set +x
    sort -o ${keyspace_tablenames} ${keyspace_tablenames}
    diff -a -q ${keyspace_tablenames} ${backup_tablenames}
    if [ $? -ne 0 ]; then
      echo -e "\nERROR: The tables on the keyspace at, '${keyspace_path}', 
                $are not the same as the ones from the backup at, '${backupdir}'"
      return 1
    fi
    for ((i=0; i<${#table_names[*]}; i++));
    do
      echo "Restoring table, '${table_names[i]}'"
      for idx in $(seq 1 $numdirs)
      do
        keyspace_path=$(db_datadir $idx)/$keyspace_name
        table_dir="${keyspace_path}/${table_names[i]}-${table_uuids[i]}"
        sudo rm -r "${table_dir}"
        sudo mkdir "${table_dir}"
        src="${backupdir}/data$idx/$keyspace_name/${table_names[i]}"
        if [[ -d $src ]]; then
          sudo cp -r -a "${src}"/* "${table_dir}/"
        else
          echo "skipping empty directory ${src} for table ${table_names[i]}"
        fi
      done
    done
}

function copy_backup {
  echo "fill in the copy commands for the right version, needs to be automated"
  #example commands
  #aws s3 cp --recursive s3://bios-load-snapshots/2021-09-05T19:48/10.1.4.79 tfos_admin/
  #aws s3 cp --recursive s3://bios-load-snapshots/2021-09-05T19:51/10.1.4.79 tfos_bi_meta/
  #aws s3 cp --recursive s3://bios-load-snapshots/2021-09-04T16:15/10.1.4.79 tfos_d_33810e9f6f133ad7921f340dbf7f6a18/
  #aws s3 cp --recursive s3://bios-load-snapshots/_system/10.1.4.79 tfos_d_68456c9e88fe3989996355a00319f15b/
}
