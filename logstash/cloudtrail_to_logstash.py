#!/usr/bin/env python
"""
A script for watching for new cloudtrail logs
and backfilling old ones into logstash.

Monitor mode:
    Watches an SQS Queue for notifications of new cloudtrail
    logs.

    Example:
        ./cloudtrail_to_logstash.py --logstash-host=justindev.local:12346 monitor --queue-name=cloudtrail-sqs

Backfill mode:
    Downloads and processes s3 keys from date range specified

    Example:
        ./cloudtrail_to_logstash.py --logstash-host=justindev.local:12346 backfill --start=20141001 --end=20141011 --bucket=cloudtrail


Needs an IAM role as follows
{
   "Statement": [
     {
       "Sid": "1",
       "Action": [
         "sqs:GetQueueUrl",
         "sqs:Receivemessage",
         "sqs:DeleteMessage"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:sqs:us-east-1:<ACCOUNT-ID>:<SQS QUEUE>"
     },
     {
       "Sid": "2",
       "Action": [
          "s3:GetObject",
          "s3:ListBucket"
       ],
       "Effect": "Allow",
       "Resource": [ "arn:aws:s3:::<BUCKET NAME>/*", "arn:aws:s3:::<BUCKET NAME>"]
     }
   ]
 }
"""
import os
import gzip
import json
import socket
import argparse
import logging
import boto.sqs
from StringIO import StringIO
from boto.sqs.message import RawMessage
from time import sleep
from datetime import timedelta
from datetime import datetime
from collections import defaultdict


def process_messages(results):
    """
    Return a dictionary of list of s3 keys from SQS messages
    Format: dict(s3_bucket) = [key1 .. keyN]
    @param messages: str
    """
    s3_keys = defaultdict(list)
    for result in results:
        try:
            body = result.get_body()
            message = json.loads(result.get_body())['Message']
            logging.debug(message)
        except KeyError, ValueError:
            logging.error('Error reading message: %s', body)
            continue

        try:
            msg = json.loads(message)
        except ValueError:
            logging.error('Error decoding message attribute')
            continue
        s3_bucket = msg['s3Bucket']
        s3_key = msg['s3ObjectKey'][0]

        s3_keys[s3_bucket].append(s3_key)

    return s3_keys


def process_s3_keys(s3_keys):
    """
    Download s3_keys, extract, decompress and
    return their content

    @param s3_keys: dict(list)
    """

    s3_conn = boto.connect_s3()
    results = []

    for s3_bucket, keys in s3_keys.iteritems():
        for s3_key in keys:
            logging.info('Processing {0}'.format(s3_key))
            bucket = s3_conn.get_bucket(s3_bucket, validate=False)
            key = bucket.get_key(s3_key)
            gzip_contents = StringIO(key.get_contents_as_string())
            content = gzip.GzipFile(fileobj=gzip_contents).read()
            results.append(content)
    return results


def send_to_logstash(messages, logstash_host, logstash_type):
    """
    Send a message to logstash TCP input
    @param logstash_host: str
    @param logstash_type: str
    @param message: list(str)
    """
    logstash_host = logstash_host.split(':')

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((logstash_host[0], int(logstash_host[1])))

    for message in messages:
        msg = json.loads(message)

        logging.info('Sending {0} events to {1}'.format(
            len(msg['Records']), logstash_host))
        # Format each record for logstash
        for record in msg['Records']:
            record['@timestamp'] = record['eventTime']
            record['@version'] = 1
            record['type'] = logstash_type
            logging.debug(json.dumps(record))
            s.send(json.dumps(record) + '\n')

    s.close()
    logging.info('Finished sending events')


def backfill(args):
    """
    Backfills cloudtrail data from args.start to args.end

    We generate a list of all possible S3 keys between
    args.start and args.end, then process each S3 key.

    Cloudtrail Path format:
    /prefix_name/AWSLogs/Account ID/CloudTrail/region/YYYY/MM/DD/file_name.json.gz
    """
    s3_conn = boto.connect_s3()

    try:
        bucket = s3_conn.get_bucket(args.bucket)
    except boto.exception.S3ResponseError:
        logging.exception("Couldn't get {0} s3 bucket".format(args.bucket))
        return -1

    try:
        start = datetime.strptime(args.start, "%Y%m%d")
        end = datetime.strptime(args.end, "%Y%m%d")
    except ValueError:
        logging.error("Couldn't parse start/end dates")
        return -1

    if end <= start:
        logging.error('Backfill start date must be before end date')
        return -1

    # generate a list of all the possible s3 keys
    s3_start_path = os.path.join(args.prefix, 'AWSLogs/')

    if args.account_id:
        account_ids = [s3_start_path + args.account_id + '/CloudTrail/']
    else:
        account_ids = [
            x.name for x in bucket.list(s3_start_path, delimiter='/')]

    for account_id in account_ids:
        if args.region:
            regions = [account_id + args.region + '/']
        else:
            regions = [x.name for x in bucket.list(
                account_id + 'CloudTrail/', delimiter='/')]

        for region in regions:
            # calculate all possible days between start and end
            days = [start.strftime('%Y/%m/%d/')]
            day_inc = start + timedelta(days=+1)
            while day_inc != end:
                days.append(day_inc.strftime('%Y/%m/%d/'))
                day_inc = day_inc + timedelta(days=+1)

            for day in days:
                logging.info('Starting day {0}'.format(day))
                for k in bucket.list(region + day):
                    results = process_s3_keys({args.bucket: [k.name]})
                    send_to_logstash(results, args.logstash_host, args.type)


def monitor(args):
    """
    Monitors SQS queue for new cloudtrail logs
    """
    sqs_conn = boto.connect_sqs()
    sqs_queue = sqs_conn.get_queue(args.queue_name)

    if not sqs_queue:
        logging.error('Queue was not found')
        return -1

    sqs_queue.set_message_class(RawMessage)
    while True:
        # read items off queue,
        # if queue is empty, go into sleep time
        sqs_messages = sqs_queue.get_messages(args.num_messages)

        # Since cloudtrail isn't realtime
        # just sleep for a minute inbetween reading off
        # all queue items
        if not sqs_messages:
            logging.info('Queue empty - sleeping 60 seconds')
            sleep(60)
            continue

        s3_keys = process_messages(sqs_messages)
        if s3_keys:
            results = process_s3_keys(s3_keys)

            send_to_logstash(results, args.logstash_host, args.type)

            # Delete messages from queue when done
            sqs_queue.delete_message_batch(sqs_messages)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--logfile', type=str,
                        help='If specified will log to destination instead of stdout')
    parser.add_argument('--logstash-host', type=str,
                        help='Format: localhost:12345')
    parser.add_argument('--type', type=str, default='cloudtrail',
                        help='Logstash type attribute to set')

    subparser = parser.add_subparsers(title='subcommands', dest="subcommand")

    monitor_parser = subparser.add_parser('monitor',
                                          help='listen for new cloudtrail events')
    monitor_parser.add_argument('--queue-name', type=str, required=True,
                                help='The SQS name to listen for events')
    monitor_parser.add_argument('--num-messages', type=int, default=1,
                                help='Number of items to fetch off queue')
    monitor_parser.set_defaults(func=monitor)

    backfill_parser = subparser.add_parser(
        'backfill', description='Backfills cloudtrail logs from the date range specified in --start and --end.  The range is [start, end)',
        help='backfill old cloudtrail events from s3 bucket')
    backfill_parser.add_argument('--prefix', type=str, default='',
                                 help='Prefix for S3 bucket set during cloudtrail setup')
    backfill_parser.add_argument('--bucket', type=str, required=True,
                                 help='S3 bucket where cloudtrail logs are stored')
    backfill_parser.add_argument('--region', type=str,
                                 help='Filter logs only from this region')
    backfill_parser.add_argument('--account-id', type=str,
                                 help='Filter logs for only this account id. Useful for cases where you aggregate multiple accounts into one cloudtrail bucket.  Default behavior is to iterate over all account ids found.')
    backfill_parser.add_argument('--start', type=str, required=True,
                                 help='Starting date in format %%Y%%m%%d, eg 20141021')
    backfill_parser.add_argument('--end', type=str, required=True,
                                 help='Ending date in format %%Y%%m%%d, eg 20141022. Note ending date is exclusive')
    backfill_parser.set_defaults(func=backfill)

    args = parser.parse_args()

    if args.logstash_host and ':' not in args.logstash_host:
        logging.error('logstash host format is hostname:port')
        return -1

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    log_format = '%(asctime)s - %(levelname)s - %(module)s - %(message)s'
    logging.basicConfig(console=True, level=level, format=log_format)
    if args.logfile:
        logging.basicConfig(console=False, format=log_format,
                            filename=args.logfile)

    # Test connection to logstash first
    logstash_host = args.logstash_host.split(':')

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((logstash_host[0], int(logstash_host[1])))
    except:
        logging.exception('Error connecting to logstash server')
        return -1

    args.func(args)

if __name__ == '__main__':
    main()
