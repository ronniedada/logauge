from twisted.spread import pb
from twisted.internet import reactor, defer

from common import save_reults

class Controller(pb.Root):

    def __init__(self, logger, system, workload, num_workers):
        self.logger = logger
        self.system = system
        self.workload = workload
        self.num_workers = num_workers
        self.workers = {}

    def remote_info(self):
        return self.system, self.workload

    def remote_accept_worker(self, worker):
        if not worker:
            self.logger.fatal('Received empty worker, ignored')
            return

        worker_id = len(self.workers)
        self.workers[worker_id] = worker
        worker.callRemote('setid', worker_id)

        l = len(self.workers)
        if self.num_workers == l:
            self.logger('Connected by %s workers, proceed to test' % l)

        self.initialize()

    def broadcast_command(self, method, args, next_step, failure_msg):
        deferreds = [self.workers[worker_id].callRemote(method, args)
                     for worker_id in self.workers.keys()]

        defer.gatherResults(deferreds, consumeErrors=True).addCallbacks(
            next_step, self.failed, errbackArgs=(failure_msg))

    def failed(self, results, failure_msg="Call Failed"):
        for (success, error), (worker_id) in zip(results, self.workers):
            if not success:
                raise Exception("worker=%s, error=%s, failure_msg=%s" %
                                (worker_id, error, failure_msg))

    def initialize(self):
        self.logger('Initializing...')
        self.broadcast_command(
            "initialize", ("initialize"), self.gendata, "Failed to initialize")

    def gendata(self, results):
        self.logger.info('Initialization finished, results=%s' % results)
        save_reults(results)
        self.logger.info('Generating data...')
        self.broadcast_command(
            "gendata", ("gendata"), self.forward, "Failed to generate data")

    def forward(self, results):
        self.logger.info('Datagen finished, results=%s' % results)
        save_reults(results)
        self.logger.info('Forwarding data...')
        self.broadcast_command(
            "forward", ("forward"), self.collect_results, "Failed to forward data")

    def collect_results(self, results):
        self.logger.info('Forward finished, results=%s' % results)
        save_reults(results)
        self.logger.info('Terminating workers')
        self.terminate()

    def terminate(self):
        for worker in self.workers.values():
            worker.callRemote("terminate").addErrback(
                self.failed, "Termination Failed")
        reactor.callLater(1, reactor.stop)

if __name__ == "__main__":
    controller = Controller()
    reactor.run()