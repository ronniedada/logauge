import logging
import sys
import yaml

from twisted.spread import pb
from twisted.internet import reactor

from core.common import setup_logging
from core.controller import Controller


def main():
    """Start up controller.

    usage: logauge-controller port system workload_file num_workers
    """
    setup_logging()
    logging.getLogger('results').handlers[0].doRollover()
    logger = logging.getLogger('controller')
    logger.handlers[0].doRollover()

    port = int(sys.argv[1])
    system = sys.argv[2]
    workload_file = sys.argv[3]
    num_workers = int(sys.argv[4])

    with open('workloads/%s' % workload_file, 'r') as f:
        workload = yaml.load(f)

    if not workload:
        raise Exception('Unable to parse workload_file=%s' % workload_file)

    controller = Controller(logger, system, workload, num_workers)
    reactor.listenTCP(port, pb.PBServerFactory(controller))


if __name__ == '__main__':
    main()