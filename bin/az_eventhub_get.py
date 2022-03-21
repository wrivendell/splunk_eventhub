#!/usr/bin/env python3
# ##### pip3 install azure-eventhub-checkpointstoreblob --user
# ##### pip3 install azure-eventhub --user
# ##### pip3 install azure --user
##############################################################################################################

### Imports
from lib import az_eventhub_get_arguments as arguments
from lib import az_eventhub_get_query as query
from lib import az_eventhub_get_conf as conf
from lib import az_eventhub_get_recv as recv
from lib import az_eventhub_get_logroll as logroll
import os, time, pathlib, json, pwd, grp, stat

from multiprocessing import Process, Pool

### Functions
def StartLogChecking(namespace, event_hub_name, eh_connection_string):
	recv.event_hub_name = str(event_hub_name)
	recv.eh_connection_string = str(eh_connection_string)
	recv.eh_unique_name = str( (namespace)+"-"+(event_hub_name) )
	recv.inactive_timeout_amount = (arguments.args.inactive_timeout)
	recv.total_timeout_amount = (arguments.args.total_timeout)
	recv.namespace = str(namespace)
	recv.unique_log_name = (recv.eh_unique_name)+".log"
	recv.log_path_suffix = str((namespace)[1:].split('-')[0]+"_aeo0_azure-platform/")
	recv.log_path_full = ((arguments.args.log_path)+(recv.log_path_suffix))
	recv.log_path_file = ((arguments.args.log_path)+(recv.log_path_suffix)+(recv.unique_log_name))
	recv.StartCapture()

def StartPool():
	jobs = []
	for param_list in inputs_tuple_list:
		#param_list 0 = namespace, 1 = eh_name, 2 = eh_connection_string
		p = Process( target=StartLogChecking, name=str( (param_list[0])+"-"+(param_list[1]) ), args=(param_list) )
		jobs.append(p)
		p.daemon = True
		p.start()
		### Next, wait until a job finishes before starting another
		while len(jobs) >= (arguments.args.proc_at_once):
			if arguments.args.debug:
				print("Still "+str(len(jobs))+" processing, waiting!")
			time.sleep(0.5)
			for j in jobs:
				if j.is_alive():
					pass
				else:
					jobs.remove(j)
		else:
			if arguments.args.debug:
				print("Now only "+str(len(jobs))+" processing, adding new!")
			continue
	time.sleep(10)
	while len(jobs) > 0:
		if arguments.args.debug:
			print(("Still waiting for {} jobs to complete".format(str(len(jobs)))))
		time.sleep(10)
		for j in jobs:
			if j.is_alive():
				pass
			else:
				jobs.remove(j)
	else:
		if arguments.args.debug:
			print("All Eventhubs processed for "+str(arguments.args.total_timeout)+" seconds each")
		conf.ExitWithSuccess((conf_changed))

# RUNTIME
conf.StartUpChecks()
print("az_eventhub_get_started=True")
SASKeyList = query.GetAllSASKeys()
if not SASKeyList:
	conf.ExitWithError("check_event_hub_status=Failed getting SASKeys")
else:
	if arguments.args.debug:
		print("All log files "+str(arguments.args.log_roll)+" hours or older are being removed now")
	logroll.RemoveOldLogFiles((arguments.args.log_path), (arguments.args.log_roll))
	inputs_tuple_list = conf.BuildInputsConf()
	if arguments.args.debug:
		print("Updating Conf File now")
	conf_changed = conf.UpdateConfFile("inputs", "tmp_inputs")
	if arguments.args.debug:
		print("Starting Log receiver workers for "+str(len(inputs_tuple_list))+":")
		for i in inputs_tuple_list:
			print(i[0])
			print(i[1])
			print("\n")
	StartPool()
