Logstash
===========

   * cloudtrail_to_logstash.py:
          Script to push cloudtrail logs into logstash.
          Has two modes, monitor and backfill.  Backfill allows you
          to specify a date range of logs you wish to fill in from an S3 bucket.
          Monitor will listen to an SQS queue for new cloudtrail log events,
          and push them into logstash.
