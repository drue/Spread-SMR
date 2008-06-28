import unittest
import sqlqueue
from pysqlite2 import dbapi2 as sqlite

class TestSqlQueue(unittest.TestCase):
    def setUp(self):
        self.db = sqlite.connect(":memory:")
        self.q = sqlqueue.SqliteQueue('test', self.db)

    def test1(self):
        self.assertEqual(len(self.q), 0)
        self.q.push('foo')
        self.assertEqual(len(self.q), 1)
        self.assertEqual(self.q.pop(), 'foo')
        self.assertEqual(len(self.q), 0)

    def test2(self):
        l = []
        f = open('/dev/urandom','rb').read
        for x in xrange(100):
            z = f(20)
            l.insert(0, z)
            self.q.push(z)
        self.assertEqual(len(self.q), len(l))

        for x in xrange(100):
            self.assertEqual(self.q.pop(), l.pop())
        self.assertEqual(len(self.q), len(l))
        self.assertEqual(len(self.q), 0)

    def test3(self):
        l = []
        f = open('/dev/urandom','rb').read
        for x in xrange(100):
            z = f(20)
            l.insert(0, z)
            self.q.push(z)
        self.assertEqual(len(self.q), 100)

        for x in self.q:
            self.assertEqual(x, l.pop())

    def test4(self):
        l = []
        f = open('/dev/urandom','rb').read
        for x in xrange(100):
            z = f(20)
            l.insert(0, z)
            self.q.push(z)

        l.reverse()
        count = 0
        for x in reversed(self.q):
            self.assertEqual(x, l.pop())
            count += 1
        self.assertEqual(count, 100)

    def test5(self):
        l = []
        f = open('/dev/urandom','rb').read
        for x in xrange(100):
            z = f(20)
            l.insert(0, z)
            self.q.push(z)

        for i in xrange(1000):
            z = 0
            for x in reversed(self.q):
                self.assertEqual(x, l[z])
                z+=1
            z = 99
            for x in self.q:
                self.assertEqual(x, l[z])
                z-= 1
            self.assertEqual(z, -1)
                                     
        
    
if __name__ == '__main__':
    unittest.main()
