import unittest
from twisted.internet import reactor, protocol, defer
from StringIO import StringIO

import sha
import sys
import random

from sync import replication, twispread
#import dummyspread
#replication.spread = dummyspread
#replication.twispread.spread = dummyspread

import logging
import timing

SPREAD = '/usr/local/sbin/spread'

class Dropper(replication.Replication):
    def __init__(self, replication_group, df, target = 0):
        self.count = 0
        self.target = target
        self.df = df
        replication.Replication.__init__(self, replication_group)

    def deliverAction(self, action_string):
        self.count +=1
        if count == self.target:
            self.df.callback(self.count)
        
class Hasher(replication.Replication):
    def __init__(self, replication_group, max_messages = 0):
        self.sha = sha.sha()
        self.max = max_messages
        self.df = defer.Deferred()
        self.count = 0
        replication.Replication.__init__(self, replication_group)
        
    def deliverAction(self, action_string):
        self.sha.update(action_string)
        self.count += 1
        if self.max:
            self.max -= 1
            if self.max == 0:
                self.df.callback(self.sha.digest())

class Pumper:
    def __init__(self, rounds, size, store):
        self.rounds = rounds
        self.size = size
        self.done = False
        self.store = store
        self.sha = sha.sha()
        self.rand = open('/dev/urandom','rb').read
        self.go()

    def go(self):
        s = self.rand(self.size)
        self.sha.update(s)
        self.store.newAction(s)
        self.rounds -= 1
        if self.rounds:
            reactor.callLater(0.1, self.go)
        
class Storer(replication.Replication):
    def __init__(self, replication_group, max_messages = 0):
        replication.Replication.__init__(self, replication_group)
        self.store = []
        self.max = max_messages
        self.df = defer.Deferred()

    def deliverAction(self, action_string):
        self.store.append(action_string)
        self.log.debug("delivered: %s", action_string)
        if len(self.store) == self.max:
            self.df.callback(self.store)
            
    def introduceNewServer(self, server_id):
        replication.Replication.introduceNewServer(self, server_id)
        self.introdf = defer.Deferred()
        return self.introdf
        
    def serverIntroduced(self, server_id, action_id, redCut):
        self.introdf.callback((action_id, redCut))
        
class Basic(unittest.TestCase):
    def setUp(self):
        self.l = []
        self.group = 'test-' + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')
        h = Storer(self.group)
        reactor.listenWith(twispread.MultiPort, protocol=h)
        h.startNewGroup()
        self.l.append(h)
        for i in xrange(10):
            reactor.iterate(0.01)
        self.a = self.b = self.x = None

    def tearDown(self):
        for h in self.l:
            h.close()
            
    def pumpFive(self, p):
        x = ['one', 'two', 'three', 'four', 'five']
        p.newAction(x[0])
        p.newAction(x[1])
        p.newAction(x[2])
        p.newAction(x[3])        
        p.newAction(x[4])
        self.x = x
        return x

    def test1(self):
        p = self.l[0]
        p.max = 5
        x = self.pumpFive(p)
        for i in xrange(10):
            reactor.iterate(0.01)
        self.failUnlessEqual(x, p.store)
            
    def test2(self):
        ## add another
        self.a = self.l[0]
        self.b = Storer(self.group)
        reactor.listenWith(twispread.MultiPort, protocol=self.b)
        self.a.log.addHandler(logging.StreamHandler())
        self.a.log.setLevel(logging.DEBUG)        
        df = self.a.introduceNewServer(self.b.serverId)
        df.addCallback(self.done2)
        self.b.max=5
        for i in xrange(30):
            reactor.iterate(0.05)
        self.assertEqual(self.a.store, self.x)
        self.assertEqual(self.a.store, self.b.store)

    def done2(self, result):
        introaction, introcut = result
        self.b.startMainProtocol(introaction, introcut)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        self.x = self.pumpFive(self.a)
        

    def test3(self):
        ## add another that has to catch up
        ## add another
        self.a = self.l[0]
        self.b = Storer(self.group)
        reactor.listenWith(twispread.MultiPort, protocol=self.b)
        #self.b.log.addHandler(logging.StreamHandler())
        #self.b.log.setLevel(logging.DEBUG)        
        df = self.a.introduceNewServer(self.b.serverId)
        df.addCallback(self.done3)
        self.a.df.addCallback(self.done31)
        self.b.max = 5

        for i in xrange(100):
            reactor.iterate(0.01)
        self.assertEqual(len(self.x), len(self.a.store))
        self.assertEqual(self.a.store, self.b.store)

    def done3(self, result):
        introaction, introcut = result
        #self.a.log.addHandler(logging.StreamHandler())
        #self.a.log.setLevel(logging.DEBUG)        
        self.b.startMainProtocol(introaction, introcut)
        self.b.transport.leave_group(self.b.group)
        self.a.max=5
        self.pumpFive(self.a)

    def done31(self, data):
        self.b.transport.join_group(self.b.group, self.b)
        return data

class Quinteroo(unittest.TestCase):
    def setUp(self):
        self.l = []
        self.group = 'test-' + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')
        h = Hasher(self.group)
        reactor.listenWith(twispread.MultiPort, protocol=h)
        h.startNewGroup()
        self.l.append(h)
        for i in xrange(4):
            h = Hasher(self.group)
            reactor.listenWith(twispread.MultiPort, protocol=h)
            self.l.append(h)

        for n in self.l:
            n.stream = StringIO()
            n.log.addHandler(logging.StreamHandler(n.stream))
            n.log.setLevel(logging.DEBUG)

        self.tointro = list(self.l[1:])

        for h in self.tointro:
            df = self.l[0].introduceNewServer(h.serverId)
            df.addCallback(self.introd, h)
            for i in xrange(50):
                reactor.iterate(0.01)
        
    def introd(self, result, hasher):
        introaction, introcut = result
        hasher.startMainProtocol(introaction, introcut)
            
    def test1(self):
        #self.l[0].log.addHandler(logging.StreamHandler())
        #self.l[0].log.setLevel(logging.DEBUG)
        for x in self.l:
            x.df = defer.Deferred()
            x.max = 10

        p = Pumper(10, 512, self.l[0])

        for i in xrange(30):
            reactor.iterate(0.1)

        self.assertEqual([0] * 5, [x.max for x in self.l])
        for x in self.l:
            self.assertEqual(x.max, 0)
            self.assertEqual(p.sha.digest(), x.sha.digest())

    """def test2(self):
        for n in self.l:
            n.stream = StringIO()
            n.log.addHandler(logging.StreamHandler(n.stream))
            n.log.setLevel(logging.DEBUG)
        #self.l[-1].log.addHandler(logging.StreamHandler())
        #self.l[-1].log.setLevel(logging.DEBUG)
        p = Pumper(100, 20, self.l[0])
        def disconnect(n):
            if not n.disconnected:
                for x in self.l:
                    x.ltconf = True
                    x.state.handleEvent(replication.Trans_conf, None)
                n.disconnected = True
                n.transport.leave_group(n.group)
        def connect(n):
            if n.disconnected:
                for x in self.l:
                    if not x.ltconf:
                        x.ltconf = True
                        x.state.handleEvent(replication.Trans_conf, None)
                n.disconnected = False
                n.transport.join_group(n.group)

        for x in self.l:
            x.disconnected = False
        i = 2
        for z in xrange(15):
            n = [random.random() for x in xrange(2)]
            n.sort()
            p = random.choice(self.l[1:])
            reactor.callLater(n[0] * i * 20, disconnect, p)
            reactor.callLater(n[1] * i * 20, connect, p)

        try:
            while [n for n in self.l if n.count < 100]:
                reactor.iterate(0.1)
        except KeyboardInterrupt:
            self.dumpLogs()
            
        for i in self.l:
            self.assertEqual([n.count for n in self.l], [100, 100, 100, 100, 100])
        for i in self.l:
            self.assertEqual(self.l[0].sha.digest(), i.sha.digest())
"""
    
    def test3(self, disconnects=5):
        #self.l[-1].log.addHandler(logging.StreamHandler())
        #self.l[-1].log.setLevel(logging.DEBUG)
        p = Pumper(100, 20, self.l[0])
        p = Pumper(100, 20, self.l[1])        
        def disconnect(n):
            if not n.disconnected:
                for x in self.l:
                    x.ltconf = True
                    x.state.handleEvent(replication.Trans_conf, None)
                n.disconnected = True
                n.transport.leave_group(n.group)
        def connect(n):
            if n.disconnected:
                for x in self.l:
                    if not x.ltconf:
                        x.ltconf = True
                        x.state.handleEvent(replication.Trans_conf, None)
                n.disconnected = False
                n.transport.join_group(n.group, n)

        for x in self.l:
            x.disconnected = False
        i = 2
        for z in xrange(disconnects):
            n = [random.random() for x in xrange(2)]
            n.sort()
            p = random.choice(self.l[2:])
            reactor.callLater(n[0] * i * 4, disconnect, p)
            reactor.callLater(n[1] * i * 4, connect, p)

        try:
            while [n for n in self.l if n.count < 200]:
                reactor.iterate(0.01)
        except KeyboardInterrupt:
            self.dumpLogs()
        try:
            for i in self.l:
                self.assertEqual([n.count for n in self.l], [200, 200, 200, 200, 200])
            for i in self.l:
                self.assertEqual(self.l[0].sha.digest(), i.sha.digest())
        except AssertionError, e:
            self.dumpLogs()
            raise e
            
    def dumpLogs(self):
        for n in self.l:            
            n.log.debug("%s, %s", n.count, n.greenQueue)
            n.stream.seek(0)
            f=open('/tmp/%s' % n.serverId, 'w')
            for line in n.stream.readlines():
                f.write(line)
            f.close()
            
    def test4(self):
        self.test3(disconnects=10)

    def test5(self):
        self.test3(disconnects=20)

    def test6(self):
        self.test3(disconnects=40)

    def test7(self):
        self.test3(disconnects=80)

    def test8(self):
        self.test3(disconnects=160)
        
"""
class Perf(unittest.TestCase):
    def setUp(self):
        self.group = 'test-' + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')
        df =  defer.Deferred()
        a = Dropper(self.group, df, 1000)
        b = Dropper(self.group, defer.Deferred(), 1000)
        c = Dropper(self.group, defer.Deferred(), 1000)

        df.addCallback(reactor.stop)
        a.startNewGroup()
        for i in xrange(10):
            reactor.iterate(0.01)

        a.introduceNewServer(b.serverId)
        a.introduceNewServer(c.serverId)

        reactor.run()

    def test9(self):
        pass

"""

        
if __name__ =="__main__":
    unittest.main()
