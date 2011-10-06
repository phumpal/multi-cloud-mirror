#!/usr/bin/python

'''

multi-cloud-mirror.py
(c) Copyright 2011 Joe Masters Emison
Licensed under the Mozilla Public License (MPL) v1.1
Full license terms available at http://www.mozilla.org/MPL/MPL-1.1.html

multi-cloud-mirror provides an easy, multi-threaded way of synchronizing
a bucket at Amazon S3 to a container at Rackspace Cloud Files, or vice
versa.

'''

#######################################################################
### Imports
#######################################################################
import boto
import cloudfiles
import smtplib
import os
import sys
import time
import datetime
import argparse
import ConfigParser
import multiprocessing
from boto.exception import S3ResponseError, S3PermissionsError, S3CopyError
from cloudfiles.errors import ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl, ContainerNotPublic, AuthenticationFailed, AuthenticationError, NoSuchObject, InvalidObjectName, InvalidMetaName, InvalidMetaValue, InvalidObjectSize, IncompleteSend
from ConfigParser import NoSectionError, NoOptionError, MissingSectionHeaderError, ParsingError


#######################################################################
### Log function and log setup
#######################################################################
emailMsg = ""
LOG_CRIT=3
LOG_WARN=2
LOG_INFO=1
LOG_DEBUG=0
def logItem(msg, level):
   global emailMsg
   if level >= 0:
      if args.debug: print msg
      if level >= 1:
         emailMsg += msg + "\n"

#######################################################################
### Copy function 
#######################################################################
def copyWorker(srcService, srcBucketName, myKeyName, destBucketName, tmpFile):
   if srcService == "s3":
      # we can stream from S3 to Cloud Files, saving us from having to write to disk
      srcBucket  = s3Conn.get_bucket(srcBucketName)
      destBucket = cfConn.get_container(destBucketName)
      # with S3, we must request the key singly to get its metadata:
      fullKey = srcBucket.get_key(myKeyName)
      # get content-type
      myContentType = fullKey.content_type
      if myContentType is None: myContentType = "application/octet-stream";
      # initialize new object at Cloud Files
      newObj = destBucket.create_object(myKeyName)
      newObj.content_type = myContentType
      newObj.size = fullKey.size
      # stream the file from S3 to Cloud Files
      newObj.send(fullKey)
   elif srcService == "cf":
      # because of the way S3 and boto work, we have to save to a local file first, then upload to Cloud Files
      # note that maximum file size (as of this writing) for Cloud Files is 5GB, and we expect 6+GB free on the drive
      cfConn.get_container(srcBucketName).get_object(myKeyName).save_to_filename(tmpFile)
      destBucket = s3Conn.get_bucket(destBucketName)
      newObj = None
      try:
         newObj = destBucket.new_key(myKeyName)
      except S3ResponseError:
         # key may exist; just get it instead:
         newObj = destBucket.get_key(myKeyName)
      newObj.set_contents_from_filename(tmpFile,replace=True)
      os.remove(tmpFile)


#######################################################################
### Parse Arguments
#######################################################################
parser = argparse.ArgumentParser(description='Multi-Cloud Mirror Script')
parser.add_argument('--process', dest='numProcesses',type=int, default=4,
                    help='number of simultaneous file upload threads to run')
parser.add_argument('--maxsize', dest='maxFileSize',type=int, default=5368709120,
                    help='maximium file size to sync, in bytes (files larger than this size will be skipped)')
parser.add_argument('--from', help='email address from which to send the status email; must be specified to receive message', dest='emailDest')
parser.add_argument('--to', dest='emailSrc',
                    help='email address(es) (comma-separated) to which to send the status email; must be specificed to recieve message')
parser.add_argument('--subject', help='subject of the status email', dest='emailSubj',
                    default="[Multi-Cloud Mirror] Script Run at %s" % (str(datetime.datetime.now())))
parser.add_argument('--tmpfile', dest='tmpFile',
                    help='temporary file used for writing when sending from cf to s3', default='/mnt/cloudfile')
parser.add_argument('--debug', dest='debug', default=False, help='turn on debug output')
parser.add_argument('sync', metavar='"s3://bucket->cf://container"', nargs='+',
                    help='a synchronization scenario, of the form "s3://bucket->cf://container" or "cf://container->s3://bucket"')
args = parser.parse_args()
tmpFile = args.tmpFile

#######################################################################
### Open connections to S3 and Cloud Files; initialize
#######################################################################
logItem("Multi-Cloud Mirror Script started at %s" % (str(datetime.datetime.now())), LOG_INFO)      
logItem("Connecting to cloud service providers", LOG_DEBUG)
try:
   ## boto reads from /etc/boto.cfg (or ~/boto.cfg)
   s3Conn = boto.connect_s3()
   ## the cloud files library doesn't automatically read from a file, so we handle that here:
   cfConfig = ConfigParser.ConfigParser()
   cfConfig.read('/etc/cloudfiles.cfg')
   cfConn = cloudfiles.get_connection(cfConfig.get('Credentials','username'), cfConfig.get('Credentials','api_key'))
except (NoSectionError, NoOptionError, MissingSectionHeaderError, ParsingError), err:
   logItem("Error in reading Cloud Files configuration file (/etc/cloudfiles.cfg): %s" % (err), LOG_CRIT)
   sys.exit(1)
except (S3ResponseError, S3PermissionsError), err:
   logItem("Error in connecting to S3: [%d] %s" % (err.status, err.reason), LOG_CRIT)
   sys.exit(1)
except (ResponseError, InvalidUrl, AuthenticationFailed, AuthenticationError), err:
   logItem("Error in connecting to CF: %s" % (err), LOG_CRIT)
   sys.exit(1)
# initialize the multiprocessing pool
pool = multiprocessing.Pool(args.numProcesses)
jobs = []

#######################################################################
### Cycle Through Requested Synchronizations
#######################################################################
jobcount = 0
for scenario in args.sync:
   [fromBucket, toBucket] = scenario.split('->')
   srcService = fromBucket[:2].lower()
   destService = toBucket[:2].lower()
   logItem("\nScenario: %s; (from: %s, to: %s)" % (scenario, srcService, destService), LOG_INFO)
   
   #######################################################################
   ### Validate Inputs
   #######################################################################
   if srcService not in ['cf','s3']:
      logItem("Source service not recognized.", LOG_WARN)
      continue
   if destService not in ['cf','s3']:
      logItem("Destination service not recognized.", LOG_WARN)
      continue
   if srcService == destService:
      logItem("Same-cloud mirroring not supported.", LOG_WARN)
      continue

   #######################################################################
   ### Connect to the proper buckets
   #######################################################################
   srcBucketName  = fromBucket[5:]
   destBucketName = toBucket[5:]

   try:
      if srcService == 's3':
         srcList        = s3Conn.get_bucket(srcBucketName).list()
         destList       = cfConn.get_container(destBucketName).get_objects()
      elif srcService == 'cf':
         srcList  = cfConn.get_container(srcBucketName).get_objects()
         destList = s3Conn.get_bucket(destBucketName).list()
   except (S3ResponseError, S3PermissionsError), err:
      logItem("Error in connecting to S3 bucket %s: [%d] %s" % (s3BucketName, err.status, err.reason), LOG_WARN)
      continue
   except (ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl, ContainerNotPublic, AuthenticationFailed, AuthenticationError), err:
      logItem("Error in connecting to CF container %s: %s" % (cfBucketName, err), LOG_WARN)
      continue
   
   logItem("Source bucket: %s, Destination bucket: %s" % (srcBucketName, destBucketName), LOG_DEBUG)

   #######################################################################
   ### Set up a dict of files at the destination
   #######################################################################
   filesAtDestination = {}
   for dKey in destList:
      if hasattr(dKey,'key'):
         myKeyName = dKey.key
      else:
         myKeyName = dKey.name
      filesAtDestination[myKeyName] = dKey.etag.replace('"','')
   
   #######################################################################
   ### Iterate through files at the source to see which ones to copy
   #######################################################################
   for sKey in srcList:
      # Get the proper key name
      if hasattr(sKey,'key'):
         myKeyName = sKey.key
      else:
         myKeyName = sKey.name
      
      # skip S3 "folders", since Cloud Files doesn't support them
      if myKeyName[-1] == '/': continue

      # skip files that are too large
      if (sKey.size > args.maxFileSize):
         logItem("Skipping %s because it is too large (%d bytes)" % (myKeyName, sKey.size), LOG_WARN)
         continue

      logItem("Found %s at source" % (myKeyName), LOG_DEBUG)

      # Copy if MD5 (etag) values are different, or if file does not exist at destination
      doCopy = False;
      try:
         if filesAtDestination[myKeyName] != sKey.etag.replace('"',''):
            # the file is at the destination, but the md5sums do not match, so overwrite
            doCopy = True
            logItem("...Found at destination, but md5sums did not match, so it will be copied", LOG_DEBUG)
      except KeyError:
         doCopy = True
         logItem("...Not found at destination, so it will be copied", LOG_DEBUG)
   
      if doCopy:
         # add copy job to pool         
         jobcount = jobcount + 1
         job = pool.apply_async(copyWorker, (srcService, srcBucketName, myKeyName, destBucketName, tmpFile + str(jobcount)))
         job_dict = dict(job=job, myKeyName=myKeyName, srcService=srcService, srcBucketName=srcBucketName, destBucketName=destBucketName)
         jobs.append(job_dict)

      # if we did not need to copy the file, log it:
      else:
         logItem("...Found at destination and md5sums match, so it will not be copied", LOG_DEBUG)


#######################################################################
### Loop through jobs, waiting for them to end
#######################################################################
allFinished = False
while not allFinished:
   # Check the status of the jobs.
   if jobs:
      logItem("Checking status of %d remaining copy tasks at %s" % (len(jobs), str(datetime.datetime.now())), LOG_DEBUG)
      for job_dict in jobs:
         job = job_dict['job']
         if job.ready():
            # If the job finished but failed, note the exception
            if not job.successful():
               try:
                  job.get() # This will re-raise the exception.
               except (S3ResponseError, S3PermissionsError, S3CopyError), err:
                  logItem("Error in copying %s to/from S3 bucket %s: [%d] %s" % (job_dict['myKeyName'], job_dict['s3BucketName'], err.status, err.reason), LOG_WARN)
                  jobs.remove(job_dict)
                  continue
               except (ResponseError, NoSuchContainer, InvalidContainerName, InvalidUrl, ContainerNotPublic, AuthenticationFailed, AuthenticationError,
                       NoSuchObject, InvalidObjectName, InvalidMetaName, InvalidMetaValue, InvalidObjectSize, IncompleteSend), err:
                  logItem("Error in copying %s to/from to CF container %s: %s" % (job_dict['myKeyName'], job_dict['cfBucketName'], err), LOG_WARN)
                  jobs.remove(job_dict)
                  continue         
               except:
                  # even if we have an unknown error, we still want to exit
                  jobs.remove(job_dict)
            else:
               logItem("Copied %s to destination\n" % (job_dict['myKeyName']), LOG_INFO)
               
               jobs.remove(job_dict)

   # Exit when there are no jobs left.
   if not jobs:
      allFinished = True
   else:
      time.sleep(5)

logItem("Multi-Cloud Mirror Script ended at %s" % (str(datetime.datetime.now())), LOG_INFO)

#######################################################################
### Send status email if we have a from and to email address
#######################################################################
if (args.emailDest is not None and args.emailSrc is not None):
   s = smtplib.SMTP('localhost')                                                       
   s.sendmail(args.emailSrc, args.emailDest.split(','), "From: %s\nTo: %s\nSubject: %s\n\n%s" % 
              (args.emailSrc, args.emailDest, args.emailSubj, emailMsg))
   s.quit()
   logItem("\nReport emailed to %s (from %s):\n----------------------\n%s\n----------------------\n" 
           % (args.emailDest, args.emailSrc, emailMsg), LOG_DEBUG)
   


