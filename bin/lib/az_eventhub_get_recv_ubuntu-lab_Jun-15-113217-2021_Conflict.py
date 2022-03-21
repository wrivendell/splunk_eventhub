#!/usr/bin/env python3
#
##############################################################################################################

### Imports
from . import az_eventhub_get_arguments as arguments
from . import az_eventhub_get_query as query

import stat, os, time, threading, pathlib, re, json, pwd, grp, stat, sys

from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.mgmt.storage import StorageManagementClient
import json
from typing import List, Tuple

### Params set by main ###
event_hub_name = ""
eh_connection_string = ""
eh_unique_name = ""
inactive_timeout_amount = 15
total_timeout_amount = 120
namespace = ""
unique_log_name = ""
log_path_suffix = ""
log_path_full = ""
log_path_file = ""

### Functions
def WriteLogFile(log_file: str, line: str, include_break: bool):
	with open((log_file),'a+') as file:
		file.write("%s" % line)
		if include_break:
			file.write("\n")
	file.close()

def CheckFileSize(log_file: str) -> bool:
	if os.path.exists(log_file):
		if os.path.getsize((log_file)) >= 5000000: #25000000:
			counter = 1
			if arguments.args.debug:
				print("File to be rotated / removed: {}".format(log_file))
			while os.path.exists((log_file)+"_"+str(counter)):
				counter = (counter) + 1
			os.rename((log_file), ((log_file)+"_"+str(counter)))
			os.remove(((log_file)+"_"+str(counter)))
	else:
		if arguments.args.debug:
			print("File {} does not appear to exist: Skipping rotation".format(str(log_file)))

def WriteLogFile(log_file: str, log: str, include_break: bool):
	with open((log_file), 'a+') as file:
		jsonlog = json.dumps(log)
		file.write(jsonlog)
		if include_break:
			file.write("\n")
	file.close()

def Timeout(consumer_client, thread):
	inactive_loop_count = 0
	timeout_loop_count = 0
	if not os.path.exists(log_path_file):
		while not os.path.exists(log_path_file):
			time.sleep(1)
			inactive_loop_count = (inactive_loop_count) + 1
			if (inactive_loop_count) > (inactive_timeout_amount):
				if arguments.args.debug:
					print("Inactive loop = " + str(inactive_loop_count) + " inactive timeout = " + str(inactive_timeout_amount))
					print((unique_log_name)+" - Timed_out waiting for log file to be made")
				consumer_client.close()
				thread.join()
				return
				#sys.exit()
		return
	if os.path.exists(log_path_file):
		inactive_loop_count = 0
		unique_log_size_start = os.path.getsize(log_path_file)
		while timeout_loop_count < ((total_timeout_amount) + 1):
			timeout_loop_count = (timeout_loop_count + 1)
			inactive_loop_count = (inactive_loop_count) + 1
			if inactive_loop_count > (inactive_timeout_amount):
				if arguments.args.debug:
					print((unique_log_name)+" - Timed_out with no activity in file")
				consumer_client.close()
				thread.join()
				#sys.exit()
			else:
				time.sleep(1)
				if not os.path.getsize(log_path_file) == (unique_log_size_start):
					if arguments.args.debug:
						print((unique_log_name)+" - File updated")
					inactive_loop_count = 0
					unique_log_size_start = os.path.getsize(log_path_file)
		else:
			if arguments.args.debug:
				print("Full time alotted, reached for "+(eh_unique_name)+". Closing connection")
			consumer_client.close()
			thread.join()
			#sys.exit()
		return

##### AzureSDK ##### #####
def on_event(partition_context, event):
	# On Each Event, write conf file to Splunk
	eventhub_log = event.body_as_json()

	# Example Logfile name: e0001ss-cmehvp26i7u3c-bind-diagnostics.log
	ehnamespace = str(partition_context.fully_qualified_namespace).split('.')[0]
	logfile = str(arguments.args.log_path + (ehnamespace)[1:].split('-')[0]+"_aeo0_azure-platform/" + str(ehnamespace) + "-" + str(partition_context.eventhub_name) + ".log")

	if "sc-diagnostics" in logfile:
		# Parse out '[ {'
		WriteLogFile((logfile), (eventhub_log), True)
	elif "bind-diagnostics" in logfile:
		# Parse out '[ {'
		WriteLogFile((logfile), (eventhub_log), True)
	else:
	# Grab first JSON object to parse from (the JSON object by default is serialized inside of an empty dict)
	# Parse out	'[ { "records": ['
		header = list(eventhub_log.keys())[0] 
		for log in eventhub_log[str(header)]:
			WriteLogFile((logfile), (log), True)
	partition_context.update_checkpoint(event)

def on_partition_initialize(partition_context):
	print("Partition: {} has been initialized.".format(partition_context.partition_id))

def on_partition_close(partition_context, reason):
	print("Partition: {} has been closed, reason for closing: {}.".format(
		partition_context.partition_id,
		reason
	))

def on_error(partition_context, error):
	if partition_context:
		print("An exception: {} occurred during receiving from Partition: {}.".format(
			partition_context.partition_id,
			error
		))
	else:
		print("An exception: {} occurred during the load balance process.".format(error))
##### AzureSDK ##### #####

def StartCapture():
	uid = pwd.getpwnam("splunk").pw_uid
	gid = grp.getgrnam("splunkmonitoring").gr_gid
	pathlib.Path((log_path_full)).mkdir(parents=True, exist_ok=True)
	os.chmod(log_path_full, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP)
	os.chown(log_path_full, uid, gid)
	CheckFileSize(log_path_file)
	if arguments.args.debug:
		print(eh_unique_name) 
	checkpointstore = BlobCheckpointStore.from_connection_string((query.FetchBlobConnectionString()), eh_unique_name)
	try:
		consumer_client = EventHubConsumerClient.from_connection_string(
			conn_str=eh_connection_string,
			consumer_group='$Default',
			eventhub_name=event_hub_name,
			idle_timeout = 15,
			logging_enable=False,
			#checkpoint_store=checkpointstore,
		)
		thread = threading.Thread(
			target=consumer_client.receive,
			name=(eh_unique_name),
		kwargs={
			"on_event": on_event,
			#"on_partition_initialize": on_partition_initialize,
			#"on_partition_close": on_partition_close,
			"on_error": on_error,
			"starting_position": "-1",  # "-1" is from the beginning of the partition.
		}
		)
		thread.start()
	except KeyboardInterrupt:
		consumer_client.close()
		thread.join()
		print("eventhub_connect_status=Failed eventhub_details="+(eh_unique_name))
		#sys.exit()
	Timeout(consumer_client, thread)
