#!/usr/bin/env python
import argparse
import requests

from requests.exceptions import ConnectionError
from time import sleep, time
from fabric.api import sudo, env
from fabric.decorators import task
from fabric.colors import red, yellow, green
from fabric.tasks import execute
from fabric.utils import abort
from fabric.contrib.console import confirm


def setup_fabric(hosts):
    """
    @param hosts: list, hosts to perform actions on
    """
    assert(type(hosts) == list)
    # disable SSH strict host key checking
    env.disable_known_hosts = True
    env.hosts = hosts
    env.parallel = False
    print 'Performing rolling restart on {0}'.format(hosts)


def data_node_count(port):
    """
    return: number of data nodes in cluster
    """
    try:
        req = requests.get(
            'http://{0}:{1}/_cluster/health'.format(env.host, port))
    except ConnectionError:
        print red("Couldn't reach {0} to get data node count".format(env.host))
        return -1
    return req.json()['number_of_data_nodes']


def cluster_status(port):
    """
    Returns the status of the ES cluster
    """
    req = requests.get('http://{0}:{1}/_cluster/health'.format(env.host, port))
    status = req.json()['status']
    print 'Cluster status: ',
    if status == 'green':
        print green(status, bold=True)
    elif status == 'yellow':
        print yellow(status, bold=True)
    elif status == 'red':
        print red(status, bold=True)
    return status


def toggle_routing(port, enable=True):
    """
    Toggles the ES setting that controls shards being shuffled around
    on detection of node failure.

    @param enable: bool
    @param port: int
    """
    mode = 'all'
    if not enable:
        mode = 'none'

    es_req_data = '{{ "transient": {{ "cluster.routing.allocation.enable": "{0}" }} }}'.format(
        mode)
    es_req = requests.put(
        'http://{0}:{1}/_cluster/settings'.format(env.host, port), data=es_req_data)

    if es_req.status_code != 200:
        print red('Error toggling routing allocation {0}'.format(es_req.text))
        return -1

    print 'Successfully set routing allocation to: {0}'.format(mode)


@task
def rolling_restart(act, service_name, port):
    start = time()
    expected_data_nodes = data_node_count(port)

    # Check health of cluster before beginning
    if cluster_status(port) != 'green':
        abort(
            'Cluster is not healthy, please ensure the cluster is healthy before continuing')

    if act:
        # Disable Routing
        if toggle_routing(port, enable=False) == -1:
            abort('Aborting due to error disabling routing allocation')

        # Restart Node
        sudo('service {0} restart'.format(service_name))

        while data_node_count(port) != expected_data_nodes:
            # Wait for node to start back up
            print 'Sleeping 15 seconds and waiting for node to come back online'
            sleep(15)

        # Re-enable allocation
        if toggle_routing(port, enable=True) == -1:
            abort('Aborting due to error enabling routing allocation')

        # Wait for cluster to fully heal
        while cluster_status(port) != 'green':
            print 'Cluster not green yet, sleeping 60 seconds...'
            sleep(60)
        end = time()
        print 'Node restart took: {0} seconds'.format(end - start)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hosts', type=str, required=True)
    parser.add_argument('--service-name', type=str, required=True)
    parser.add_argument('--port', type=int, default=9200)
    parser.add_argument('--act', action='store_true')

    args = parser.parse_args()

    hosts = [host for host in args.hosts.split(',')]

    if not args.act:
        print red('Performing DRY RUN')

    setup_fabric(hosts)
    if confirm('This process can take a long time, you should run this within a screen session, do you wish to continue?'):
        execute(rolling_restart, args.act, args.service_name, args.port)

if __name__ == '__main__':
    main()
