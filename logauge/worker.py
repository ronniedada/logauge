import logging
import sys
import yaml

from twisted.spread import pb
from twisted.internet import reactor

from core.common import setup_logging


def _get_info(root):
    """Get info from controller
    Info includes:
        - system
        - workload

    """
    def2 = root.callRemote('info')
    def2.addCallback(_send_worker, root)


def _send_worker(system_workload, root):

    system, workload = system_workload

    logger = logging.getLogger('worker')

    if system == 'core':
        from core.worker import Worker
        worker = Worker(logger, workload)
    elif system == 'splunk':
        from splunk.worker import Worker
        worker = Worker(logger, workload)
    else:
        raise Exception('system=%s is not supported' % system)
    root.callRemote('accept_worker', worker)


def main():
    """Start up worker.

    Start worker and connect to controller

    usage: logauge-worker serveraddr:serverport
    """
    setup_logging()
    logger = logging.getLogger('worker')
    logger.handlers[0].doRollover()

    addr, port = sys.argv[1].trim().split(':')

    factory = pb.PBClientFactory()
    reactor.connectTCP(addr, int(port), factory)
    def1 = factory.getRootObject()
    def1.addCallback(_get_info)
    reactor.run()

if __name__ == '__main__':
    main()