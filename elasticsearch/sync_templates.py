#!/usr/bin/env python
#
# Script to sync ES templates to a node
# Name of template will be the filename striped of '.json'
#
import argparse
import requests
import os
import json

from requests.exceptions import ConnectionError


def validate_json(dir_name):
    """
    @param dir_name: str, directory containing template json files
    return: True if all files in @dir_name are valid JSON
    """
    try:
        templates = os.listdir(dir_name)
    except OSError:
        print 'Specify a valid directory'
        return False

    if not templates:
        print 'No templates found in directory'
        return False

    for template in templates:
        with open(os.path.join(dir_name, template), 'r') as fh:
            try:
                print 'Validating {0}'.format(template)
                json.load(fh)
            except:
                print '{0} contains invalid JSON'.format(template)
                return False
    return True


def sync_templates(act, dir_name, hostname, port):
    """
    @param act: bool
    @param dir_name: str
    @param hostname: str
    @param port: int
    """
    template_endpoint = "http://{0}:{1}/_template".format(hostname, port)

    # fetch current list of templates
    try:
        req_templates = requests.get(template_endpoint)
    except ConnectionError:
        print "Could not connect to ES"
        return -1

    cur_templates = req_templates.json().keys()
    new_templates = os.listdir(dir_name)

    print "Current templates: {0}".format(','.join(cur_templates))
    for template in new_templates:
        # Notify if adding any new templates
        if template.rstrip('.json') not in cur_templates:
            print 'Adding a new template {0}'.format(template)

    cont = raw_input('You sure you wish to continue? (y/n): ')

    if cont.lower() == 'y':
        if act:
            # sync
            for templ in new_templates:
                with open(os.path.join(dir_name, templ), 'r') as fh:
                    print 'Syncing {0}'.format(templ)
                    result = requests.put(
                        '{0}/{1}'.format(template_endpoint, templ.rstrip('.json')), data=fh.read())
                    if not result.json().get('acknowledged', False):
                        print 'Error applying {0}'.format(templ)
    else:
        print 'Aborting...'
        return -1


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument('--act', action='store_true', default=False)
    parser.add_argument(
        '--dir_name', help='Specify directory of templates to sync', required=True)
    parser.add_argument('--hostname', required=True, help='ES node to sync to')
    parser.add_argument('--port', default=9200,
                        help='Port number of ES server')

    args = parser.parse_args()

    if validate_json(args.dir_name):
        sync_templates(args.act, args.dir_name, args.hostname, args.port)


if __name__ == "__main__":
    main()
