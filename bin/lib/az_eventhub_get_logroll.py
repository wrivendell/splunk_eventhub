#!/usr/bin/env python3

from . import az_eventhub_get_arguments as arguments
import csv, datetime, time, os

def RemoveOldLogFiles(directory, hours):
	""" Removes all unrequired Log Files in /opt/splunk/var directory that are older then a certain date
	
	Additionally, removes latent connection strings caused by Eventhub terminating servicebus subscriber connection
	Args:
		directory(dict): Splunk eventhub /var/ directory - defaults to /opt/splunk/var/log/azure/eventhub/
		hours(str): Number of days the retention period should be for - Defaults to 1 day
	"""
	# fetch list of every file in /var/ Eventhub Directory
	allfiles_list = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser((directory))) for f in fn]
	print("Trimming Log Files")
	for f in allfiles_list:
		if (IsLogFileOld((f), ((hours)))):
			print((f)+" will be deleted")
			# Remove temp logfile in $SPLUNK_HOME/var/log/azure/*
			os.remove(f)
	# Remove latent connection strings
	for filename in os.listdir('/tmp'):
		if filename.endswith(".crt"):
			os.remove('/tmp/' + str(filename))
	print("Logfiles removed")

def IsLogFileOld(file, hoursold):
	""" Determines if a log file is dictated to be 'old' - (I.e. if the log file is older then the retention period)
	Returns True is logfile is old, otherwise returns False
	Args:
		file(str): Eventhub Filename
		hoursold(str): Number of hours the retention period should be for - Defaults to 3 Hours
	"""
	#Age Format: YYYY-MM-DD HR:HH
	current_fileage = (time.strftime('%Y-%m-%d %H', time.gmtime(os.path.getmtime(file))))
	purge_age = (datetime.datetime.now() - datetime.timedelta(hours=(hoursold))).strftime("%Y-%m-%d %H")
	if (current_fileage) < (purge_age):
		return True
	else:
		return False