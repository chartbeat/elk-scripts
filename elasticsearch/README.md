Elasticsearch
===========

   * rolling_restart.py:
          Perform a rolling restart on the ES nodes specified.
          The script will toggle routing allocation on/off during
          the restart and ensure the cluster is healthy before
          moving to the next node

   * sync_templates.py:
          Given a directory of .json files, this script will
          sync the templates to the ES node specified.  Templates
          will be named the same as the filename stripped of the
          .json extension.
