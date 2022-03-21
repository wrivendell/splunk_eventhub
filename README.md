# az_eventhub_get
This app is used to scrape EventHub for any changes/updates and rewrites the inputs.conf
file in /opt/splunk/etc/apps/az_eventhub_get/local 

This app contains the props and transforms for index extracting of the service_tier field for [azure:eventhub] sourcetype

App will run by Splunk every x mins (default 20) and on startup, checks in place should prevent multiple runs at once
Output logs for THIS script can be seen in Splunk Cloud by searching index=main sourcetype=eventhub:get
    index and sourcetype can be modified in inputs.conf of this app

Logs are captured in specified intervals every x seconds with multiprocessing
Logs are written to disk at a specified folder and inputs.conf watches that folder/logs
Logs older than <specified> day are automatically removed each time the script is run

az_eventhug_get.sh is pre-generated from rbc-rbcsplunk

 Standalone usage for debug/testing:   python3 az_eventhub_get.py -u "" -p "" -t "" -su "service_account" -sp "password" -ig "kube_" -cpl "/home/splunk/conf/" -lp "/home/splunk/logs/" -d t