import unittest

from pysqlite2 import dbapi2 as sqlite
from sync import sqldict

class basic(unittest.TestCase):
    def setUp(self):
        self.db = sqlite.connect(':memory:')
        self.d = sqldict.SqliteDict('test', self.db)
        
    def test1(self):
        self.d['foo'] = 'bar'
        self.assertEqual(self.d['foo'], 'bar')

    def test2(self):
        self.assertEqual(len(self.d), 0)
        self.d['ding'] = 'dong'
        self.assertEqual(len(self.d), 1)
        self.d['ding'] = 'dang'        
        self.assertEqual(len(self.d), 1)
        self.assertEqual(self.d['ding'], 'dang')

    def test3(self):
        self.d['ding'] = 'dang'        
        self.assertEqual(len(self.d), 1)
        del(self.d['ding'])
        self.assertEqual(len(self.d), 0)        

    def test4(self):
        t = {'a':1,'b':2,'c':3}
        self.d.update(t)

        self.assertEqual(len(self.d), len(t))
        
        for k in t.keys():
            self.assertEqual(self.d[k], t[k])

        a = self.d.keys()
        b = t.keys()
        a.sort();b.sort()
        self.assertEqual(a, b)

        a = self.d.values()
        b = t.values()
        a.sort();b.sort()
        self.assertEqual(a, b)

        a = self.d.items()
        b = t.items()
        a.sort();b.sort()
        self.assertEqual(a, b)

        a = []
        for k in self.d.iterkeys():
            a.append(k)
        a.sort()
        b = t.keys()
        b.sort()
        self.assertEqual(a, b)

        a = []
        for k,v in self.d.iteritems():
            a.append((k,v))
        a.sort()
        b = t.items()
        b.sort()
        self.assertEqual(a, b)

        self.d.clear()
        self.assertEqual(len(self.d), 0)
        for k in t.keys():
            self.assertFalse(self.d.has_key(k))
            try:
                x = self.d[k]
            except KeyError:
                pass
            else:
                assert False

if __name__ == '__main__':
    unittest.main()
