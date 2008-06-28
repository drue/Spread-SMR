import sys
import random

import unittest
from twisted.internet import reactor, protocol


from sync import twispread
from sync import dummyspread as spread
import spread as rspread


class Storer(protocol.AbstractDatagramProtocol):
    def __init__(self):
        self.store = []

    def datagramReceived(self, msg, addr=None):
        self.store.append(msg)

        
class Simple(unittest.TestCase):
    def setUp(self):
        self.l = []
        self.group = 'test-' + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')
        for i in range(3):
            e = Storer()
            p = reactor.listenWith(twispread.Port, protocol=e)
            p.join_group(self.group)
            self.l.append(e)

    def testIt(self):
        p = self.l[0]
        self.words = ['foo', 'bar', 'bang']
        for i in xrange(6):
            reactor.iterate(0.01)

        for w in self.words:
            p.transport.write(w, self.group)

        for i in xrange(10):
            reactor.doIteration(0.01)
        
        for p in self.l:
            p.store = [msg for msg in p.store if type(msg)== spread.RegularMsgType]
            self.failUnlessEqual(len(p.store), len(self.words))

        for i in xrange(len(self.words)):
            for p in self.l:
                self.failUnlessEqual(p.store[i].message, self.words[i])

if __name__ =="__main__":
    unittest.main()

    
