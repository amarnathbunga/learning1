#!/bin/bash

echo $(date --iso-8601=seconds)

if /sbin/pidof -o %PPID -x "processors_check.sh" >/dev/null; then
    echo "processors_check.sh already running, killing self"
    exit 0
fi

export PYTHON_BIN=/mnt/vol1/python3.6/bin/python3.6
export BASE_DIR=/mnt/vol1/dh-ads/joshkuduhive
cd ${BASE_DIR}/scripts

###########################################################

ps -ef | grep "JoshRequestProcessorMain" | grep -v grep > /dev/null 2>&1
if [[ "$?" == "0" ]]; then
    echo "requests running"
else
    echo "requests not running $(date)" >> ${BASE_DIR}/processorstatus/requests_`date +\%Y\%m\%d`_cron.log
    nohup sh ${BASE_DIR}/scripts/kudu_req_processing.sh &
fi

###########################################################

#ps -ef | grep "JoshResponseProcessorMain" | grep -v grep > /dev/null 2>&1 
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshResponseProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "ad response processor running"
else
    echo "ad response processor not running $(date)" >> ${BASE_DIR}/processorstatus/ad_response_processor_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/kudu_adresponse_processing.sh
fi

###########################################################

#ps -ef | grep "JoshImpressionProcessorMain" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshImpressionProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "impressions running"
else
    echo "impressions not running $(date)" >> ${BASE_DIR}/processorstatus/impressions_`date +\%Y\%m\%d`_cron.log
    #nohup sh ${BASE_DIR}/scripts/kudu_imp_processing.sh &
    timeout 60 sh ${BASE_DIR}/scripts/kudu_imp_processing.sh
fi

###########################################################

#ps -ef | grep "JoshClickProcessorMain" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshClickProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "clicks running"
else
    echo "clicks not running $(date)" >> ${BASE_DIR}/processorstatus/clicks_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/kudu_clicks_processing.sh
fi

#################################################################

#ps -ef | grep "JoshVastProcessorMain" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshVastProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "vast running"
else
    echo "vast not running $(date)" >> ${BASE_DIR}/processorstatus/vast_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/kudu_vast_processing.sh
fi

##################################################################

#ps -ef | grep "JoshAppEventsProcessorMain" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshAppEventsProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "appevents running"
else
    echo "appevents not running $(date)" >> ${BASE_DIR}/processorstatus/appevents_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/kudu_appevents_processing.sh
fi

###################################################################

#ps -ef | grep " JoshAttributionProcessorMain" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshAttributionProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "attribution running"
else
    echo "attribution not running $(date)" >> ${BASE_DIR}/processorstatus/attribution_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/kudu_attribution_processing.sh
fi

##################################################################

#ps -ef | grep "JoshInstallProcessorMain" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshInstallProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "Install processor running"
else
    echo "Install processor not running $(date)" >> ${BASE_DIR}/processorstatus/install_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/kudu_install_processing.sh
fi

##################################################################

#ps -ef | grep "oshAdInflatedProcessorMain" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "JoshAdInflatedProcessorMain"
if [[ "$?" == "0" ]]; then
    echo "AdInflated processor running"
else
    echo "AdInflated processor not running $(date)" >> ${BASE_DIR}/processorstatus/ad_inflated_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/kudu_ad_inflated_processing.sh
fi

##################################################################

#ps -ef | grep "JoshAdInteractionConsumer" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "in.dailyhunt.adtech.nrt.interactionevents.JoshAdInteractionConsumer"
if [[ "$?" == "0" ]]; then
    echo "JoshAdInteractionConsumer running"
else
    echo "JoshAdInteractionConsumer not running $(date)" >> ${BASE_DIR}/processorstatus/ad_interaction_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/startJoshAdInteractionConsumer.sh
fi

###################################################################
#
#ps -ef | grep "JoshGenericEventInteractionConsumer" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "in.dailyhunt.adtech.nrt.interactionevents.JoshGenericEventInteractionConsumer"
if [[ "$?" == "0" ]]; then
    echo "JoshGenericEventInteractionConsumer running"
else
    echo "JoshGenericEventInteractionConsumer not running $(date)" >> ${BASE_DIR}/processorstatus/josh_generic_event_interactions_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/startJoshGenericEventInteractionConsumer.sh
fi

###################################################################
#
#ps -ef | grep "NrtJoshAdInteractionConsumer" | grep -v grep > /dev/null 2>&1
${PYTHON_BIN} ${BASE_DIR}/scripts/yarn-check-app-running.py "in.dailyhunt.adtech.nrt.interactionevents.NrtJoshAdInteractionConsumer"
if [[ "$?" == "0" ]]; then
    echo "NrtJoshAdInteractionConsumer running"
else
    echo "NrtJoshAdInteractionConsumer not running $(date)" >> ${BASE_DIR}/processorstatus/nrt_josh_ad_interactions_`date +\%Y\%m\%d`_cron.log
    timeout 60 sh ${BASE_DIR}/scripts/startNrtJoshAdInteractionConsumer.sh
fi
#
###################################################################

