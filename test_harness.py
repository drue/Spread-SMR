import sys
from test_replication import Hasher, Pumper
from sync.replication import ActionId
from StringIO import StringIO

from twisted.internet import reactor
from twisted.web.xmlrpc import Proxy, XMLRPC, Binary, Fault
from twisted.web.server import Site

import logging

class TestHarness(XMLRPC):
    def xmlrpc_setup(self, group, new=False):
        self.group = group
        self.h = Hasher(group)
        self.log = StringIO()
        self.h.log.addHandler(logging.StreamHandler(self.log))
        self.h.log.setLevel(logging.DEBUG)
        if new:
            self.h.startNewGroup()
        print ">>> setup ", group, self.h.serverId
        return self.h.serverId

    def xmlrpc_get_log(self):
        return Binary(self.log.getvalue())
    
    def xmlrpc_intro_server(self, server_id):
        df = self.h.introduceNewServer(server_id)
        def encode(res):
            action_id, redCut = res
            print ">>> introduced server", server_id
            return (action_id.encode(), redCut)
        df.addCallback(encode)
        return df

    def xmlrpc_introduced(self, introaction, introcut):
        introaction = ActionId().decode(introaction)
        self.h.startMainProtocol(introaction, introcut)
        print ">>> introduced at", introaction
        return 0

    def xmlrpc_count(self):
        return self.h.count

    def xmlrpc_hash(self):
        return Binary(self.h.sha.digest())

    def xmlrpc_pump(self, count, size=20):
        self.p = Pumper(count, size, self.h)
        return 0
    
if __name__ == '__main__':
    s = TestHarness()
    port = 7070
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    reactor.listenTCP(port, Site(s))
    reactor.run()
