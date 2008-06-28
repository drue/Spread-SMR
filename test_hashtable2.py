import hashtable
import sys
from twisted.internet import reactor
from twisted.web.xmlrpc import Proxy, XMLRPC, Binary, Fault
from twisted.web.server import Site

class xdict(XMLRPC, object):
    def __init__(self, rgroup, dbpath, port, intro= '', pname=''):
        XMLRPC.__init__(self)
        self.d = hashtable.ReplicatedDict(rgroup, dbpath, port, intro, pname)

    def xmlrpc_get(self, key):
        return self.d[key]

    def xmlrpc_set(self, key, value):
        self.d[key] = value
        return 0
    
                           
if __name__ == "__main__":
    args = sys.argv[1:]
    ## test group dbpath port pname <introurl>
    if len(args) == 5:
        intro = args[4]
    else:
        intro = ''
    d = xdict(args[0], args[1], int(args[2])+1, intro, args[3])
    reactor.listenTCP(int(args[2]), Site(d))
    reactor.run()
