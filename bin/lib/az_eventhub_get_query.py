#!/usr/bin/env python3

# Imports
from . import az_eventhub_get_arguments as arguments
from . import az_eventhub_get_conf as conf

import re
from typing import List, Tuple

from azure.mgmt.eventhub import EventHubManagementClient
from azure.graphrbac import GraphRbacManagementClient
from azure.graphrbac.models import GroupCreateParameters, Application
from azure.common.credentials import UserPassCredentials, ServicePrincipalCredentials
from azure.storage.blob import ContainerClient, BlobServiceClient
from azure.mgmt.storage import StorageManagementClient

# Credentials to Parse
credentials = ServicePrincipalCredentials(
	client_id = arguments.args.username,
	secret = arguments.args.password,
	tenant = arguments.args.tenant,
)
storage_mgmt_client = StorageManagementClient(
	credentials, 
	arguments.args.storage_subscription
)
client = EventHubManagementClient(
	credentials,
	arguments.args.retention_subscription
)
# Fetch SAS Account Keys to Create blobs with
storage_keys = storage_mgmt_client.storage_accounts.list_keys(
	str(arguments.args.resource_group), str(arguments.args.storageaccount_name)
)
accountkey = (str({v.key_name: v.value for v in storage_keys.keys}['key1']))
try:
	blobclient = BlobServiceClient(
		account_url=('https://' + str(arguments.args.storageaccount_name) + '.blob.core.windows.net'), 
		credential=accountkey
		)
	if arguments.args.debug:
		print("Blob Client Fetch Successful")
except Exception as e:
	print(str(e))

### Functions
def FetchResourceGroups() -> List[str]:
	# Fetches all ResourceGroups
	resource_groups = []
	ns_details = client.namespaces.list()
	for ns in ns_details:
		ns_temp = str(ns)
		resource_group_name = re.search('resourceGroups\/(.*?)\/', (ns_temp)).group(1)
		if resource_group_name not in resource_groups:
				resource_groups.append(resource_group_name)
	return(resource_groups)

def GetEnvironmentFlag(resource_group_name: str) -> str:
	return(resource_group_name[0])

def FetchNamespaces(resource_group_name: str) -> List[str]:
	""" Fetches all EventHub namespaces within a single Resource Group
	Returns all namespaces in a specific EventHub
	Args:
		resource_group_name(str): Resource Group where EventHub Namespaces reside
	"""
	namespaces = client.namespaces.list_by_resource_group(resource_group_name)
	return(namespaces)

def FetchEventhubs(resource_group_name: str, namespace_name: str) -> List[str]:
	""" Fetches all Individual Eventhubs within a single Eventhub Namespace
	Returns all eventhubs in a specific Namespace
	Args:
		resource_group_name(str): Resource Group where EventHub Namespaces reside
		namespace_name(str): Namespace Name (This pertains to 1 subscription as per Centralized logging design)
	"""
	eventhubs = client.event_hubs.list_by_namespace(resource_group_name, namespace_name, raw=True)
	return(eventhubs)
	
def FetchSASKeys(resource_group_name: str, namespace_name: str, eventhub_name: str, sas_key_name: str) -> List[str]:
	""" Fetches any keys inside of a single EventHub
	Will return a preformatted list of keys, which you can parse by using .primary_connection_string
	# i.e. Endpoint=sb://n0022-cmehvp26i7u3c.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mysecretkey;EntityPath=ehn-diagnostics
	Args:
		resource_group_name(str): Resource Group where EventHub Namespaces reside
		namespace_name(str): Namespace Name (This pertains to 1 subscription as per Centralized logging design)
		eventhub_name(str): Individual EventHub name (This pertains to 1 type of log as per Centralized Logging design)
		sas_key_name(str): SAS Key Name - e.g. e0001aks-insights-operational-logs-AuthorizationRuleListen - this is the key used to connect to the EventHub SDK for a specific sub
	"""
	keys = client.event_hubs.list_keys(resource_group_name, namespace_name, eventhub_name, sas_key_name)
	return(keys)
	
def FetchSASKeyName(resource_group_name: str, namespace_name: str, eventhub_name: str, sas_key_suffix: str) -> str:
	""" Fetches the SAS Key name based off of the type you give it (E.g. AuthorizationRuleListen)
	Since the Names are programatic, this is used to pre-determine the name
	e.g. e0001aks-insights-operational-logs-AuthorizationRuleListen
		e0001aks = subscription
		insights-operational-logs = eventhub log name
		AuthorizationRuleListen = Authority of Key, in this case Listen
	Args:
		resource_group_name(str): Resource Group where EventHub Namespaces reside
		namespace_name(str): Namespace Name (This pertains to 1 subscription as per Centralized logging design)
		eventhub_name(str): Individual EventHub name (This pertains to 1 type of log as per Centralized Logging design)
		sas_key_suffix(str): SAS Key permission Suffix, defaults to 'AuthorizationRuleListen'
	"""
	# By default, key is 'not found'
	keyname = "Not Found"
	key_details = client.event_hubs.list_authorization_rules(resource_group_name, namespace_name, eventhub_name)
	for key in key_details:
		if sas_key_suffix in str(key):
			keyname = str(key.name)
			break
	return(keyname)

#def FetchBlobConnectionString(accountkey, arguments):
def FetchBlobConnectionString():
	""" Reforms the storage account name and key, and reforms it to a Connection String for Blob Storage Account
	default structure - DefaultEndpointsProtocol=https;AccountName=<blobstoragename>;AccountKey=<accountkey>
	"""
	BlobConnectionString = str('DefaultEndpointsProtocol=https;AccountName=' + str(arguments.args.storageaccount_name) + ';AccountKey=' + str(accountkey) + ';EndpointSuffix=core.windows.net')
	return(BlobConnectionString)

def FetchAllKeys(NamespaceIndex: List[str], resource_group_name: str) -> Tuple[str, str]:
	""" FetchAllKeys function - main function used to fetch individual eventhub keys from a subscription
	This function scrapes all EventHub Namespaces the service principal has access to, and creates/returns two dicts
	- SASKeyList - A list of Connection Strings, Eventhubs, and Namespaces to parse over in BuildInputsConf
	- SasKeyExceptionList - A similar list of problematic Eventhubs, where either an error occured or where the EventHub is found in an 'ignore' list

	Additionally, this function also populates storage accounts for a specific namespace, allowing for Checkpointing to be used in a DR scenario

	Args:
		NamespaceIndex(list): List of all namespaces to fetch from
		resource_group_name(str): Resource group name of log to fetch from - as of now this is 1-1 region and service tier
	"""
	# Fetches all SAS Policy keys for a specific Resource Group
	# Requires an Tuple of all Eventhub SAS keys, along with any exceptions where a key cannot be found for a specific EventHub
	SASKeyList = []
	SASKeyExceptionList = []
	for Namespace in NamespaceIndex:
		if arguments.args.debug:
			print("Fetching Eventhub Info for " + str(Namespace.name) + " in Tier: " + GetEnvironmentFlag(resource_group_name))
		eventhubindex = FetchEventhubs(resource_group_name, Namespace.name)
		loop_count = 0
		for Eventhub in eventhubindex:
			if arguments.args.debug:
				print("Fetching Connection String for " + str(Eventhub.name))
			SASKeyName = FetchSASKeyName(resource_group_name, Namespace.name, Eventhub.name, arguments.args.saskeysuffix)
			if not "Not Found" in SASKeyName:
				try:
					KeyIndex = FetchSASKeys(resource_group_name, Namespace.name, Eventhub.name, SASKeyName)
					SASKeyList.append(str(KeyIndex.primary_connection_string))
					# Create list of eventhub details to be added
					conf.CreateInputsTuple(str(Namespace.name), (str(Eventhub.name)), (str(KeyIndex.primary_connection_string)))
					# attempt to create Blob Container
					try:        
						blobcontainername = (str(Namespace.name) + '-' + str(Eventhub.name))
						container_client = blobclient.get_container_client(blobcontainername)
						container_client.create_container()
					except Exception as e:
						if "ContainerAlreadyExists" in str(e):
							if arguments.args.debug:
								print("Storage Container " + str(blobcontainername) + " already exists, skipping")
						else:
							print(str(e))
							print("event_hub_name="+str(blobcontainername)+" create_blob_status=Unknown Error")
				except Exception as e:
					if str(e) == "Operation returned an invalid status code 'Not Found'":
						print("event_hub_name=" + str(Eventhub.name) + " name_space="+ str(Namespace.name) + " get_key_status=No SAS Keys associated-skipping tier=" + GetEnvironmentFlag(resource_group_name))
						SASKeyExceptionList.append(str(Eventhub.name))
					else:
						print(str(e) + ' keystatus')
						print("event_hub_name="+str(Eventhub.name)+"get_key_status=Unknown Error")
			else:
				print("event_hub_name=" + str(Eventhub.name) + " name_space="+ str(Namespace.name) + "get_key_name=Key not found with " + (arguments.args.saskeysuffix) + " suffix")
	return(SASKeyList, SASKeyExceptionList)

def BuildKeysArray(resource_group_name: str) -> List[str]:
	NamespaceIndex = FetchNamespaces(resource_group_name)
	# Fetch all keys for all namespaces, output to an array
	SASKeyList, Exceptions = FetchAllKeys(NamespaceIndex, resource_group_name)
	return(SASKeyList)

def GetAllSASKeys() -> List[str]:
	resource_groups = FetchResourceGroups()
	SASKeyList = []
	for rg in (resource_groups):
		if arguments.args.debug:
			print("Fetching SAS Keys for " + str(rg))
		SASKeyList = (SASKeyList) + (BuildKeysArray(rg))
	return(SASKeyList)

### Get List and Print it if Running Standalone
if __name__ == "__main__":
	SASKeyList = GetAllSASKeys()
	for Key in SASKeyList:
		print(str(Key))
