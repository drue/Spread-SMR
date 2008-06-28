import unittest
from sync import hashtable
import random
from twisted.internet import reactor
import logging

class BasicTests(unittest.TestCase):
    def setUp(self):
        self.group = 'test-' + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')
        self.a = hashtable.ReplicatedDict(self.group, ':memory:', random.randint(10000,64000))
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)        
        self.b = hashtable.ReplicatedDict(self.group, ':memory:', random.randint(10000,64000), 'localhost:%s' % self.a.port)

    def tearDown(self):
        self.a.close()
        self.b.close()
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        
    def test1(self):
        a = self.a
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        a['foo'] = 'bar'
        reactor.iterate(0.01)
        self.assertEqual(a.keys(), ['foo'])
        self.assertEqual(a['foo'], 'bar')

    def test2(self):
        self.test1()
        a = self.a
        b = self.b
        
        for i in xrange(10):
            reactor.iterate(0.01)

        x, y = a.keys(), b.keys()
        x.sort()
        y.sort()
        self.assertEqual(x, y)
        for k in a.keys():
            self.assertEqual(a[k], b[k])

    def test3(self):
        #self.a.log.addHandler(logging.StreamHandler())
        #self.a.log.setLevel(logging.DEBUG)        
        self.test2()
        self.b['ding'] = 'dong'
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)
        reactor.iterate(0.01)        
        self.assertEqual(self.a['ding'], self.b['ding'])
        
if __name__=="__main__":
    unittest.main()
    reactor.stop()
    
