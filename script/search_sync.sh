#!/usr/bin/env bash

# use sst to sync index data from source cluster to target cluster
# usage:
#   1. git clone from https://github.com/fishjam/sst
#   2. build code by run `go build` or download latest release
#   3. modify this script file, example: SRC_ES/DST_ES/SYNC_FIXED_INDEXES/SYNC_DYNAMIC_INDEXES etc.
#   4. execute this script to sync data between cluster( source cluster => target )
#      time ./search_sync.sh
#   4. enable in crontab(inc sync every hour, full sync at 00:00:00 )
#      0 * * * * bash -x /home/<username>/scripts/search_sync.sh >> /home/<username>/logs/search_sync/sync.log 2>&1


CURRENT_DIR=$(cd `dirname $0`;pwd)
cd ${CURRENT_DIR}

ENV_TYPE=dev

ES_CURL_HEADER="Content-Type:application/json"
INC_SYNC_FILE=inc_sync.txt

SYNC_TIME_STAMP=0

SRC_ES=http://127.0.0.1:9201
SRC_AUTH=

DST_ES=http://127.0.0.1:19201
DST_AUTH=

SST_BIN=./sst

#log level: 0(all), 1(trace), 2(debug), 3(info), 4(warn), 5(error), 6(disable)
LOG_LEVEL=3

#$1 -- log level
#$2 -- log level string
#$* -- log string
function fun_log_impl(){
  if [[ $1 -ge $LOG_LEVEL ]] ;
  then
    shift

    local LEVEL_STR=$1
    shift

    echo "[`date \"+%Y-%m-%d %H:%M:%S\"`] $LEVEL_STR: $*"
  fi
}

function fun_trace(){
  fun_log_impl 1 "TRACE" "$@"
}

function fun_debug(){
  fun_log_impl 2 "DEBUG" "$@"
}

function fun_info(){
  fun_log_impl 3 "INFO" "$@"
}

function fun_warn(){
  fun_log_impl 4 "WARN" "$@"
}

function fun_error(){
  fun_log_impl 5 "ERROR" "$@"
}

#usage: fun_check_error "$? -eq 0" "build source code"
function fun_check_error(){
    # echo "check " $1
    if [ ! $1 ]; then
        fun_error "$2 error.."
        exit 255
    fi
}

# $1: source index name
# $2: isFullSync(--force delete old and sync all, will not sync the deleted data)
# $3: stamp field if not empty
function fun_sync_single_index(){

  SYNC_INDEX_NAME=$1
  isFullSync=$2
  StampField=$3

  FULL_SYNC_FLAG=

  if [ ${isFullSync} -eq 1 ]; then
    FULL_SYNC_FLAG=--force
  fi  

  STAMP_SYNC_FLAG=

  if [ "${StampField}" != "" ] && [ ${SYNC_TIME_STAMP} -ne 0 ]; then
    STAMP_SYNC_FLAG="--stamp ${StampField}:>=${SYNC_TIME_STAMP}"
  fi

  fun_info "now will sync ${SYNC_INDEX_NAME} , full flags: ${FULL_SYNC_FLAG}"

  # -u "_doc"

  ${SST_BIN} \
    --sync \
    --shards=5 \
    --replicas=1 \
    -c 5000 \
    --log=info \
    ${STAMP_SYNC_FLAG} \
    --source=${SRC_ES} --source_auth=${SRC_AUTH} \
    --dest=${DST_ES} --dest_auth=${DST_AUTH} \
    --src_indexes=${SYNC_INDEX_NAME} \
    --dest_index=${SYNC_INDEX_NAME}

    # --source_proxy=http://10.34.135.37:8888 --dest_proxy=http://10.34.135.37:8888
    fun_check_error "$? -eq 0" "sync es ${SYNC_INDEX_NAME}"
}

# $1 isFullSync
# $2 stamp field
function fun_sync_fixed_indexes(){
  # sst can not support "--src_indexes=*" because of too_long_frame_exception , so sync all index one by one
  SYNC_FIXED_INDEXES=( \
    user_data_without_stamp \
    user_result_with_stamp:lastUpdateTime \
  )

  isFullSync=$1

  for entry in ${SYNC_FIXED_INDEXES[@]} ; do
      local TARGET_INDEX="${entry%%:*}"
      local STAMP_FIELD=""

      if [ "${TARGET_INDEX}" != "${entry}" ] ; then
        STAMP_FIELD="${entry##*:}"
      fi

      fun_info "entry= ${entry}, index=${TARGET_INDEX}, stamp=${STAMP_FIELD}"

      if [ "${STAMP_FIELD}" != "" ]; then
        fun_info "will sync special data for ${STAMP_FIELD}"
      fi

      fun_sync_single_index ${TARGET_INDEX} ${isFullSync} ${STAMP_FIELD}
  done
}

# $1 isFullSync
function fun_sync_dynamic_indexes(){

  # generate index every month, example: user_index-202410, user_index-202411

  SYNC_DYNAMIC_INDEXES=( \
    user_index:lastUpdateTime \
  )

  isFullSync=$1
  
  NOW_MONTH=`date +%Y%m`
  fun_info "now month=${NOW_MONTH}, the want sync dynamic month index suffix is ${NOW_MONTH}"

  for entry in ${SYNC_DYNAMIC_INDEXES[@]} ; do
    local SYNC_INDEX_PREFIX="${entry%%:*}"
    local STAMP_FIELD=""

    if [ "${SYNC_INDEX_PREFIX}" != "${entry}" ] ; then
      STAMP_FIELD="${entry##*:}"
    fi

    SYNC_INDEX="${SYNC_INDEX_PREFIX}-${NOW_MONTH}"
    # fun_info "want sync index ${SYNC_INDEX}"
    fun_sync_single_index ${SYNC_INDEX} ${isFullSync} ${STAMP_FIELD}

    # if want sync the old data, then change this (end condition)
    for (( i=1; i <= 1; i=i+1 )) ; do
      OLD_ES_INDEX_SUFFIX=`date -d"${i} month ago" +%Y%m`
      SYNC_INDEX="${SYNC_INDEX_PREFIX}-${OLD_ES_INDEX_SUFFIX}"
      fun_sync_single_index ${SYNC_INDEX} ${isFullSync} ${STAMP_FIELD}
    done
  done
}

function fun_main(){
  # get start time with seconds, but maybe 00:00:01, so just get the minute
  START_TIME=`date +%T`
  START_MINUTE=${START_TIME:0:5}
  START_DATE_TIME=`date "+%Y-%m-%d %H:%M:%S"`

  if [ "${START_MINUTE}" == "00:00" ]; then
    isFullSync=1
    rm -f ${INC_SYNC_FILE}
  else
    isFullSync=0
  fi 

  # if there is inc_sync.txt, then incremental sync the data, else full sync
  if [ -f ${INC_SYNC_FILE} ]; then
    SYNC_TIME_STAMP=`date -d "1 days ago" "+%s%3N"`
  else
    SYNC_TIME_STAMP=0
  fi

  # try sync fixed indexes
  fun_sync_fixed_indexes ${isFullSync}

  # try sync dynamic indexes
  fun_sync_dynamic_indexes ${isFullSync}

  END_DATE_TIME=`date "+%Y-%m-%d %H:%M:%S"`

  fun_info "isFullSync=${isFullSync}, startDateTime=${START_DATE_TIME}, endDateTime=${END_DATE_TIME}"

  echo $SYNC_TIME_STAMP > ${INC_SYNC_FILE}
}

# call main function to exec sync data between clusters
fun_main $*
exit 0
