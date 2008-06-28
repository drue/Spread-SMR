from pysqlite2 import dbapi2 as sqlite
import UserDict
import pickle
from StringIO import StringIO

class SqliteDict(UserDict.DictMixin, object):
    def __init__(self, name, db, **kwargs):
        self.name = 'SqliteDict_' + name
        self.db = db
        c = self.cursor
        try:
            c.execute('select count(key) from %s where 1=1' % self.name)
            c.fetchall()
        except:
            self.createTable()
            
        if len(kwargs):
            self.update(kwargs)

    def _get_cursor(self):
        return self.db.cursor()
    cursor = property(_get_cursor)
    
    def createTable(self):
        c = self.cursor
        c.execute('create table %s (hash number, key blob, value blob)' % self.name)
        c.execute('create unique index %s_index on %s(hash)' % (self.name, self.name))

    def _decode_item(self, item):
        return pickle.loads(item)
    def _decode_items(self, a, b):
        return pickle.loads(a), pickle.loads(b)

    def _encode_item(self, item):
        return sqlite.Binary(pickle.dumps(item, -1))
    def _encode_items(self, a , b):
        return sqlite.Binary(pickle.dumps(a, -1)), sqlite.Binary(pickle.dumps(b, -1))
    
    def __repr__(self):
        c = self.cursor
        c.execute('select key, value from %s where 1=1' % self.name)
        s = StringIO()
        s.write('{')
        for k,v in c.fetchall():
            x, y = self._decode_items(k, v)
            s.write(repr(x))
            s.write(': ')
            s.write(repr(y))
            s.write(', ')
        s.write("}")
        return s.getvalue()
                 
##    def __cmp__(self, dict):
##        if isinstance(dict, UserDict):
##            return cmp(self.data, dict.data)
##        else:
##            return cmp(self.data, dict)

    def __len__(self):
        c = self.cursor
        c.execute('select count(key) from %s where 1=1' % self.name)
        x = c.fetchall()[0][0]
        return x
    
    def __getitem__(self, key):
        c = self.cursor
        c.execute('select value from %s where key=?' % (self.name), (self._encode_item(key),))
        try:
            v = self._decode_item(c.fetchall()[0][0])
        except IndexError:
            raise KeyError
        else:
            return v
        
    def __setitem__(self, key, item):
        c = self.cursor
        k, v = self._encode_items(key, item)
        try:
            c.execute('insert into %s values (?, ?, ?)' % (self.name), (hash(key), k, v))
        except sqlite.OperationalError:
            c.execute('update %s set value=? where hash=?' % self.name, (v, hash(key)))
        
    def __delitem__(self, key):
        c = self.cursor
        c.execute('delete from %s where hash=?' % self.name, (hash(key),))

    def clear(self):
        c = self.cursor
        c.execute('delete from %s where 1=1' % self.name)
        
    def keys(self):
        c = self.cursor
        c.execute ('select key from %s where 1=1' % self.name)
        return [self._decode_item(x[0]) for x in c.fetchall()]
    
    def items(self):
        c = self.cursor
        c.execute ('select key, value from %s where 1=1' % self.name)
        return [self._decode_items(x, y) for x, y in c.fetchall()]

    def iteritems(self):
        def gen():
            c = self.cursor
            c.execute('select key, value from %s where 1=1' % self.name)
            for x,y in c.fetchall():
                yield self._decode_items(x, y)
        return gen()
            
    def iterkeys(self):
        def gen():
            c = self.cursor
            c.execute('select key from %s where 1=1' % self.name)
            for k in c.fetchall():
                yield self._decode_item(k[0])
        return gen()
    
    def itervalues(self):
        def gen():
            c = self.cursor
            c.execute('select value from %s where 1=1' % self.name)
            for v in c.fetchall():
                yield self._decode_item(v[0])
        return gen()
            
    def values(self): return [x for x in self.itervalues()]

    def has_key(self, key):
        c = self.cursor
        c.execute('select key from %s where hash=?' % self.name, (hash(key),))        
        return len(c.fetchall()) > 0
