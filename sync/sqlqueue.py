from pysqlite2 import dbapi2 as sqlite
import pickle

class SqliteQueue(object):
    def __init__(self, name, db):
        self.name = "SqliteQueue_%s" % name
        self.db = db
        c = db.cursor()
        self.head = 0
        self.tail = -1

        try:
            c.execute('select min(num), max(num) from %s where 1=1' % self.name)
        except:
            self.createTable()
        else:
            if len(self):
                self.head, self.tail = c.fetchall()[0]

    def createTable(self):
        c = self.db.cursor()
        c.execute('create table %s (num int, value blob)' % self.name)
        c.execute('create unique index %s_index on %s(num)' % (self.name, self.name))
        
    def _decode(self, item):
        return pickle.loads(item)

    def _encode(self, item):
        return sqlite.Binary(pickle.dumps(item, -1))

    def __len__(self):
        c = self.db.cursor()
        c.execute('select count(num) from %s where 1=1' % self.name)
        x = c.fetchall()[0][0]
        return x
        
    def push(self, item):
        c = self.db.cursor()
        self.tail += 1
        c.execute('insert into %s values (?, ?)' % self.name, (self.tail, self._encode(item)))

    def pop(self):
        if self.head == self.tail + 1:
            raise ValueError, "empty queue"
        c = self.db.cursor()
        c.execute('select value from %s where num=?' % self.name, (self.head,))
        v = c.fetchall()
        if not len(v):
            raise IndexError, 'empty queue'
        else:
            v = self._decode(v[0][0])
        c.execute('delete from %s where num=?' % self.name, (self.head,))
        self.head += 1
        return v
    
    def __getitem__(self, i):
        c = self.db.cursor()
        if i >= 0:
            c.execute('select value from %s where num=?' % self.name, (self.head + i,))
        else:
            c.execute('select value from %s where num=?' % self.name, (self.tail + 1 + i,))
        l = c.fetchall()
        if not l:
            raise IndexError, 'empty queue'
        return self._decode(l[0][0])

    def __iter__(self):
        def iter():
            c = self.db.cursor()
            c.execute('select value from %s where 1=1 order by num' % self.name)
            for i in c.fetchall():
                yield self._decode(i[0])
        return iter()

    def reversed(self):
        def iter():
            c = self.db.cursor()
            c.execute('select value from %s where 1=1 order by num desc' % self.name)
            for i in c.fetchall():
                yield self._decode(i[0])
        return iter()
        
    def __repr__(self):
        s = '['
        c = self.db.cursor()
        c.execute('select value from %s where 1=1 order by num' % self.name)
        for x in c.fetchall():
            i =  self._decode(x[0])
            s+= '%s, ' % i
        if len(self):
            if not len(s) > 1:
                print ">>>> qerr", self.head, self.tail
            s=s[:-2]
        s+= ']'
        return s
            
    def head(self):
        return self[0]

    def tail(self):
        return self[-1]
    
    def clear(self):
        c = self.db.cursor()
        c.execute('delete from %s where 1=1' % self.name)
        self.head = 0
        self.tail = -1
