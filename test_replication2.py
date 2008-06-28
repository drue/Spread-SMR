import unittest
import os
import random
import xmlrpclib
import sha
import time

port = 7070
machines = ['uno','dos','tres','quatro','cinco']
base = 'drue.bittorrent.com'

cluster = []
for m in machines:
    cluster.append("%s.%s" % (m, base))
    
class Partitioner:
    def __init__(self, num_members = 5):
        self.n = num_members
        self.p, self.stdout = os.popen2('spmonitor', 'w')


    def partition(self, np):
        self.p.write("1\n")
        for x in np:
            self.p.write("%s\n" % x)
        self.p.write("2\n")

    def clear_partition(self):
        self.p.write("4\n")


class Tests(unittest.TestCase):
    def setUp(self):
        self.l = []
        self.group = 'test-' + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')
        for m in cluster:
            self.l.append(xmlrpclib.ServerProxy("http://%s:%s/RPC2" % (m, port)))

        self.l[0].setup(self.group, True)
        for m in self.l[1:]:
            name = m.setup(self.group)
            action, cut = self.l[0].intro_server(name)
            m.introduced(action, cut)

    def test1(self):
        null = sha.sha().digest()
        for m in self.l:
            self.assertEqual(null, m.hash().data)
            
    def test2(self):
        self.l[0].pump(10)
        while self.l[0].count() != 10:
            time.sleep(0.5)

        self.assertEqual([m.count() for m in self.l], [10]*5)
            
        h = self.l[0].hash().data
        for m in self.l[1:]:
            self.assertEqual(h, m.hash().data)

    def test3(self, n=1000):
        time.sleep(1)
        self.l[0].pump(n)
        while self.l[0].count() != n:
            time.sleep(0.5)

        h = self.l[0].hash().data
        for m in self.l[1:]:
            self.assertEqual(m.hash().data, h)


        
if __name__ =="__main__":
    unittest.main()
