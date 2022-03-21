#!/usr/bin/env python3
#
##############################################################################################################

### Imports
import argparse

### Functions
class LoadFromFile (argparse.Action):
	#Class for Loading Arguments in a file if wanted
	def __call__ (self, parser, namespace, values, option_string = None):
		with values as f:
			parser.parse_args(f.read().split(), namespace)

def str2bool(string: str) -> bool:
	#Define common "string values" for bool args to accept "Yes" as well as "True"
	if isinstance(string, bool):
		return(string)
	if string.lower() in ('yes', 'true', 't', 'y', '1'):
		return(True)
	elif string.lower() in ('no', 'false', 'f', 'n', '0'):
		return(False)
	else:
		raise argparse.ArgumentTypeError('Boolean value expected.')

def CheckPositive(value: str) -> int:
	ivalue = int(value)
	if ivalue <= 0:
		raise argparse.ArgumentTypeError("%s is an invalid (below 0) int value" % value)
	return ivalue

def Arguments():
	# Arguments the app will accept
	global parser
	parser = argparse.ArgumentParser()
	parser.add_argument('--file', type=open, action=LoadFromFile)
	parser.add_argument("-u", "--username", nargs='?', required=True, default='eaa2115f-9a2f-40c8-a4f7-5d6914a2c3c3', help="Service Principal Client ID")
	parser.add_argument("-p", "--password", required=True, help="Service Principal Secret")
	parser.add_argument("-t", "--tenant", nargs='?', default='9323b596-236d-4890-bed3-60232a849027', help="Service Principal Tenant ID")
	parser.add_argument("-rs", "--retention_subscription", nargs='?', default='4271e2c7-639d-4b0d-82e3-32fe30b835d9', help="Retenion Subscription")
	parser.add_argument("-ss", "--storage_subscription", nargs='?', default='3823da9a-f968-433c-aa9c-1b623a79c44e', help="Storage Subscription")
	parser.add_argument("-sn", "--storageaccount_name", nargs='?', default='nnonpsplunkchkptstore1', help="Storage Account Name")
	parser.add_argument("-rg", "--resource_group", nargs='?', default='n-nonp-ccoe-sharedservices-spl-rg-cac-1', help="Resource Group where Checkpoint store is located")
	parser.add_argument("-sks", "--saskeysuffix", nargs='?', default='-AuthorizationRuleListen', help="SAS Key Name")
	parser.add_argument("-cpl", "--conf_path_live", required=False, default='/opt/splunk/etc/apps/az_eventhub_get/local/', help="The full path to current inputs.conf eg: /opt/splunk/etc/apps/az_eventhub_get/local/ ", type=lambda x: str(x.replace('\s', ' ').replace('\"\"', '')))
	parser.add_argument("-su", "--splunk_username", nargs='?', default='', required=True, help="Splunk Cloud UN for creating index via API")
	parser.add_argument("-sp", "--splunk_password", nargs='?', default='', required=True, help="Splunk Cloud PW for creating index via API")
	parser.add_argument('-ig','--ignore_list', nargs='*', default='', required=False, help="List of Eventhubs to NOT include in Splunk Monitoring. Separate by spaces. Script uses contains, be as precise as you should for excludes. i.e \"aks\" will exclude ANY Eventhub with \"aks\" in its name.")
	parser.add_argument("-mf", "--mappings_file", default='/opt/splunk/etc/apps/az_eventhub_get/local/mappings.yaml', required=False, help="The full path and filename to where the optional yaml mappings file is for sourcetype default is /opt/splunk/etc/apps/az_eventhub_get/local/mappings.yaml", type=lambda x: str(x.replace('\s', ' ').replace('\"\"', '')))
	parser.add_argument("-pao", "--proc_at_once", type=CheckPositive, nargs='?', const=True, default=23, required=False, help="How many eventhubs should be polled at once by the TA - NOTE-too high can spike cpu")
	parser.add_argument("-it", "--inactive_timeout", type=CheckPositive, nargs='?', const=True, default=20, required=False, help="How long(sec) to wait for a log to come in before moving on to next")
	parser.add_argument("-tt", "--total_timeout", type=CheckPositive, nargs='?', const=True, default=120, required=False, help="When logs are coming in, how long(sec) to keep pulling until we rest this one and move on to next")
	parser.add_argument("-lp", "--log_path", default='/opt/splunk/var/log/azure/eventhub/', required=False, help="The full path to where the logs will get written to disk /opt/splunk/var/log/azure/eventhub/", type=lambda x: str(x.replace('\s', ' ').replace('\"\"', '')))
	parser.add_argument("-lr", "--log_roll", type=CheckPositive, nargs='?', const=True, default=1, help="How many hours to keep old logs?")
	parser.add_argument("-d", "--debug", type=str2bool, nargs='?', const=True, default=False, help="Debug Option to verbosely output commands")

############## RUNTIME
Arguments()
args = parser.parse_args()
