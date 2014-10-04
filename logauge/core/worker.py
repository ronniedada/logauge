
from twisted.spread import pb
from twisted.internet import reactor

class Worker(pb.Referenceable):

    def __init__(self, logger, workload):
        self.logger = logger
        self.workload = workload
        self.id = None

    def __str__(self):
        return "worker=%s" % self.id

    def remote_setid(self, id):
        self.id = id

    def remote_initialize(self, args):
        return "%s initialized" % self

    def remote_gendata(self, args):
        return "%s generated data" % self

    def remote_forward(self, args):
        return "%s forwarded" % self

    def remote_terminate(self):
        reactor.callLater(0.5, reactor.stop)
        return "%s terminated" % self
