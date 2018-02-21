from cluster import Cluster, set_cluster
from test import Test
from errors import TestError
import sys

def run():
    """Runs the test framework."""
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action', help="The action to execute")

    setup_parser = subparsers.add_parser('setup', help="Setup a test cluster")
    setup_parser.add_argument('name', help="The cluster name")
    setup_parser.add_argument('--nodes', '-n', type=int, default=3, help="The number of nodes in the cluster")
    setup_parser.add_argument('--subnet', '-s', default='172.18.0.0/16', help="The subnet in which to create the cluster")
    setup_parser.add_argument('--gateway', '-g', default=None, help="The IPv4 gateway for the master subnet")

    teardown_parser = subparsers.add_parser('teardown', help="Tear down a test cluster")
    teardown_parser.add_argument('name', help="The cluster name")

    run_parser = subparsers.add_parser('run', help="Run a test")
    run_parser.add_argument('test', help="The test to run")
    run_parser.add_argument('--cluster', '-c', help="The cluster on which to run the test")

    args = parser.parse_args()

    try:
        if args.action == 'setup':
            Cluster(args.name).setup(args.nodes, args.subnet)
        elif args.action == 'teardown':
            cluster = Cluster(args.name)
            cluster.teardown()
            cluster.cleanup()
        elif args.action == 'run':
            set_cluster(args.cluster)
            try:
                Test(args.test).run()
            except AssertionError, e:
                sys.exit(1)
            else:
                sys.exit(0)
    except TestError, e:
        print str(e)
        sys.exit(1)
    else:
        sys.exit(0)

def _import_colorizor():
    from colorama import init
    init()

def _import_tests():
    import testtest

if __name__ == '__main__':
    _import_colorizor()
    _import_tests()
    run()
