#!/bin/bash

# Force Python path to Python3
export PYTHONPATH=/usr/local/lib/python3.6/site-packages

# Run using blank interpretter
/bin/python3 /opt/splunk/etc/apps/az_eventhub_get/bin/az_eventhub_get.py \
    -u   "eaaeew5f-9da2f-40c8-a4f7-5d6914ewrwc3c3"               \
    -p   "[weerwfdsfdsfdfsfsd"            \
    -t   "932erwfs-236d-4890-bed3-60232a849027"                \
    -rs  "427wrwrssdfc7-639d-4b0d-82e3-32fe30b835d9" \
    -ss  "3823ddwrwra-f968-e33c-aa9c-1bwerrw9c44e"   \
    -sn  "nnonpsplunkchkptstore1"      \
    -rg  "n-nonp-ccoe-sharedservices-spl-chkpt-rg-cac-1"               \
    -su  "sp10_splunk_svc"         \
    -sp  "wwwerw87VpYZ"         \
    -ig  "kube_"             \
    -d  f                  \

#-u  -  Splunk Service Principal username
#-p  -  Splunk Service Principal password
#-t  -  Tenant ID
#-rs -  retention_subscription
#-ss -  storage subscription
#-sn -  storage account name
#-rg -  resource group where checkpoint store is stored
#-su -  Splunk Cloud Username
#-sp -  Splunk Cloud Password
#-ig -  Eventhub Regex's to ignore
#-d  -  Debug mode
