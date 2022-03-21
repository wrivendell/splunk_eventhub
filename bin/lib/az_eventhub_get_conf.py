#!/usr/bin/env python3
#
##############################################################################################################

### Imports
from . import az_eventhub_get_arguments as arguments

import os, shutil, requests, time, sys, filecmp, re, datetime, logging, yaml, socket

from urllib3.exceptions import InsecureRequestWarning
from requests.auth import HTTPBasicAuth

from typing import List, Tuple
import os, threading, pathlib, re, json, pwd, grp, stat
import json

uid = pwd.getpwnam("splunk").pw_uid
gid = grp.getgrnam("splunkmonitoring").gr_gid

### Functions
def LockFile(action: int): 
	"""
	Lockfile Function
		Currently, the existance of `az_eventhub_get.lock` ensures that the script doesn't run on top of itself - this function moderates the LockFile
	Args:
		action - (int): Action to be taken upon the Lockfile
			1 = create lockfile
			2 = remove lockfile
			3 = check lockfile
	Return:
		Returns status of the lockfile
	"""
	if arguments.args.debug:
		return True
	lock_file = (arguments.args.conf_path_live) + "az_eventhub_get.lock"
	if action == 1:
		# Attempt to create lockfile
		try:
			open((lock_file), 'w').close()
			return True
		except OSError as err:
			# StartupChecks logs specific error and force closes program
			print ("Error: {} - {}.".format(e.filename, e.strerror))
			return False
	elif action == 2:
		# Attempt to remove lockfile
		try:
			os.remove(lock_file)
		except OSError as e:
			print ("Error: {} - {}.".format(e.filename, e.strerror))
	elif action == 3:
		# Test if lockfile exists; or if script has forcibly kept it
		if os.path.exists(lock_file):
			lock_file_age = os.path.getmtime(lock_file)
			if lock_file_age < (time.time() - 4000):
				ExitWithError("lock_file_status=az_eventhub_get.lock was detected as older than an a hour - Forcefully removed")
			else:
				return False
		else:
			return True
	else:
		ExitWithError("lock_file_status=Lock File action not specified")

def StartUpChecks():
	"""
	StartUpChecks Function
		Does the following things
			- Ensures that inputs.conf is able to be written to (Checks for Lockfile)
			- Ensures that all files (inputs.conf, tmp_inputs.conf, etc) is able to be accessed
	"""
	# Ensure inputs.conf is writable to update config and no Lock file present
	if not LockFile(3):
		print("lock_file_status=az_eventhub_get.lock was detected, exiting script")
		sys.exit()
	elif not LockFile(1):
			ExitWithError("lock_file_status=az_eventhub_get.lock couldn't be created, check permissions?")
	if not os.path.exists((arguments.args.conf_path_live)+"inputs.conf"):
		try:
			open(arguments.args.conf_path_live + "inputs.conf", 'w').close()
		except:
			ExitWithError("write_conf_status=Failed, "+(arguments.args.conf_path_live)+"inputs.conf could not be found or created")
	if not os.access((arguments.args.conf_path_live)+"inputs.conf", os.W_OK):
		ExitWithError("write_conf_status=Failed, "+(arguments.args.conf_path_live)+"inputs.conf is not writable")
	try:
		open((arguments.args.conf_path_live)+"tmp_inputs.conf", 'w').close()
	except:
		ExitWithError("write_conf_status=Failed, "+(arguments.args.conf_path_live)+"tmp_inputs.conf is not writable")
	try:
		open((arguments.args.log_path)+"test.file", 'w').close()
		os.remove((arguments.args.log_path)+"test.file")
	except:
		ExitWithError("write_conf_status=Failed, "+(arguments.args.log_path)+" does not appear to be writable")


def WriteConfFile(conf_file: str, lines: List, include_break: bool):
	"""Handles Execution of writing to inputs.conf or tmp_inputs.conf
	Args:
		conf_file (str): Dictionary containing policy info
		lines (list): List containing lines to be writen to Conf file
		include_break (bool): Writes a starting and ending newline (if applicable)
	"""
	with open(conf_file, 'a+') as file:
		for item in lines:
			file.write("{}".format(item))
		if include_break:
			file.write("\n")

def CompareFiles(file1: str, file2: str) -> bool:
	"""Compares two file objects, file1 and file2 - returns True if they are equal, False if they are not
	Args:
		file1 (str): Template file
		file2 (str): File to be compared to
	"""
	return filecmp.cmp(file1, file2)


def CheckYamlMappings(event_hub_name: str) -> str:
	"""Checks external YAML Mapping file (See args.mappings_file)
	if the EventHub Name matches the mapping file - change the sourcetype of the eventhub to the one listed in the mapping file for better clarity
	if the EventHub name does not match, return nothing
	Args:
	event_hub_name (str): EventHub name fetched from *.query library
	"""
	try:
		with open(arguments.args.mappings_file, 'r') as file:
			mappings = yaml.load(file)
			for mapping, name in mappings.items():
				for key in name.keys():
					if event_hub_name == key:
						mapping_name = str(name[key])[2:-2]
						if arguments.args.debug:
							print("EventHub Key Value: {}".format(mapping_name))
						return mapping_name
				# If no keys match mapping file, return nothing and report a warning to Console
				print ("Warning: No Valid Mappings exist for {}; continuing".format(event_hub_name))
				return None
	except Exception as err:
		print ("Error: {} - continuing with YAML Check".format(err.strerror))

def CreateInputsTuple(namespace: str, eventhub: str, connection_string: str):
	"""Creates inputs.conf tuple containing EventHubs and Connection Strings
	Mainly used in query.FetchAllKeys to build a custom Tuple to parse
	Args:
	namespace (str): Eventhub Namespace - used to determine which subscription we are parsing against
	eventhub (str): Eventhub Name - used to parse the type of data we are fetching to determine SourceType
	connection_string (str): Connection String of the individual Eventhub - used to fetch data from the Microsoft Eventhub SDK (see *.recv)
	"""
	unique_log_name = str(namespace + "-" + eventhub)+".log"
	log_path_suffix = str((namespace)[1:].split('-')[0]+"_aeo0_azure-platform/")
	log_path_full = ((arguments.args.log_path)+(log_path_suffix))
	log_path_file = ((arguments.args.log_path)+(log_path_suffix)+(unique_log_name))
	pathlib.Path((log_path_full)).mkdir(parents=True, exist_ok=True)
	os.chmod(log_path_full, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP)
	os.chown(log_path_full, uid, gid)
	inputs_tuple_list.append([(namespace), (eventhub), (connection_string)])

def BuildInputsConf() -> List:
	"""Master function used to build the inputs.conf file
	This does a few things, specifically
		- Creates a inputs.conf entry for the app in order to send diagnostic data to the EventHub
		- grabs each section of Tuple generated for the EventHub, writes a conf file
		- specifies Index (*_aeo0_azure-platform) and transforms pre-generated names to an External mapping YAML (if applicable)
	Args:
		event_hub_name (str): EventHub name fetched from *.query library
	"""
	input_lines = [\
	# This line is the line that runs THIS script, since in same app, it needs to always be present
	"[script:///opt/splunk/etc/apps/az_eventhub_get/bin/az_eventhub_get.sh]\n", \
	"disabled = 0\n", \
	"interval = 300\n", \
	"index = main\n", \
	"sourcetype = eventhub:get\n" \
	]
	WriteConfFile((arguments.args.conf_path_live)+"tmp_inputs.conf", (input_lines), True)
	for eventhub_tuple in inputs_tuple_list:
		eventhub_namespace = (eventhub_tuple[0])
		event_hub_name = (eventhub_tuple[1])
		event_hub_connection_string = (eventhub_tuple[2])
		host_segment = str((arguments.args.log_path).count("/") + 1)
		# External Mapping file (Changes EventHub name Sourcetype to specified Mapping)
		external_mapping = CheckYamlMappings(event_hub_name)
		# Use mapping file ONLY if an external mapping exists, if not append the eventhub name to the sourcetype
		source_type = external_mapping if external_mapping else "azure:" + event_hub_name

		# Strip the first letter out as all tiers go into the same index
		index = (eventhub_namespace)[1:].split('-')[0]+"_aeo0_azure-platform"
		index = (index).lower()
		
		# Build a specific input for the Eventhub we're covering
		input_lines = [\
		# Splunk TA Config doesnt allow '-' in the name
		"[monitor://"+(arguments.args.log_path)+(index)+"/"+(eventhub_namespace)+"-"+(event_hub_name)+".log]\n", \
		"disabled = 0\n", \
		"host = "+(hostname)+"\n", \
		"sourcetype = "+(source_type)+"\n", \
		"index = "+(index)+"\n" \
		]
		WriteConfFile((arguments.args.conf_path_live)+"tmp_inputs.conf", (input_lines), True)
		index_names.add(index)
	return(inputs_tuple_list)

def UpdateConfFile(conf_file_name: str, tmp_conf_file_name: str) -> bool:
	"""Secondary function used to determine changes to inputs.conf file
		when the Conf function runs more then once, we want to compare the original file to the updated file, if we dont detect a change, then ignore changing the file
		Returns a True/False value to parent function
	Args:
		conf_file_name (str): Actual inputs.conf file used by the Eventhub
		tmp_conf_file_Name (str): Temporary Inputs.conf file to compare against
	"""
	if not CompareFiles((arguments.args.conf_path_live)+(conf_file_name)+".conf", ((arguments.args.conf_path_live)+(tmp_conf_file_name))+".conf"):
		print((conf_file_name)+"_conf_status=changed")
		try:
			open((arguments.args.conf_path_live)+"inputs.conf", 'w').close()
		except:
			ExitWithError((arguments.args.conf_path_live)+"inputs.conf__zero_status=Failed, couldn't zero out original conf")
		try:
			shutil.copyfile(((arguments.args.conf_path_live)+(tmp_conf_file_name)+".conf"), ((arguments.args.conf_path_live)+(conf_file_name)+".conf"))
		except Exception as err:
			ExitWithError((conf_file_name)+"_update_status=Failed, could not replace current config")
		for idx in index_names:
			CreateIndex({"username":(arguments.args.splunk_username),"password":(arguments.args.splunk_password)}, (idx), False)
		return True
	else:
		print((conf_file_name)+"_conf_status=unchanged")
		os.remove((arguments.args.conf_path_live)+(tmp_conf_file_name)+".conf")
		return False

def CreateIndex(creds: dict, index_name: str, verify=True):
	""" Creates index on SplunkCloud using Splunk API
	Args:
		creds(dict): SplunkCloud access credentials {"username": "USERNAME", "password": "PASSWORD"}
		index_name(str): index name to create
	Returns:
		status(str): created , existed  or None if operation failed 
	"""
	if not (arguments.args.splunk_username or arguments.args.splunk_password):
		ExitWithError("index_create_status=Failed, no specified Splunk Cloud User or Password")
	else:
		index_to_create = ("index_to_create="+(index_name))
	logging.basicConfig(
	format='%(asctime)s %(message)s',
	level=logging.INFO
	)
	url = "https://sh1.aer0.splunkcloud.com:8089/services/cluster_blaster_indexes/sh_indexes_manager"
	headers = {'Content-Type': 'application/json'}
	payload = [
		('output_mode', 'json'), 
		('name', index_name), 
		('datatype', 'event'), 
		('maxTotalDataSizeMB', '0'),
		('maxGlobalRawDataSizeMB', '0'),
		('maxGlobalDataSizeMB', '0'),
		('frozenTimePeriodInSecs', '10368000') 
		]
	if arguments.args.debug:
		logging.info("Creating Index ")
	requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
	response = requests.request("POST", url, auth=(creds["username"], creds["password"]), headers=headers, data=payload, verify=False)
	if arguments.args.debug:
		print("{}".format(response.content))
	if response.status_code == 201:
		if arguments.args.debug:
			logging.info("Index " + index_name + " has been successfully created")
		return "created"
	elif response.status_code == 500:
		if arguments.args.debug:
			logging.info("Index " + index_name + " already exist in the SplunkCloud")
			print(response.content)
	else: 
		if arguments.args.debug:
			logging.error("Failed to create index: " + index_name + ". Reason: " + response.text)
		return None

def ExitWithError(error: str):
	print("check_event_hub_status=Failed " + error)
	LockFile(2)
	sys.exit()

def ExitWithSuccess(restart: bool):
	print("check_event_hub_status=Success")
	LockFile(2)
	if restart:
		os.system("sudo systemctl restart Splunkd")
	sys.exit()
	
# RUNTIME
hostname = socket.gethostname()
index_names = set()
inputs_tuple_list = []
