"""A more or less complete user-defined wrapper around dictionary objects."""

import clusterer
import UserDict
import sqldict
from pysqlite2 import dbapi2 as sqlite
from bencode import bencode, bdecode

Set, Del = xrange(2)


class ReplicatedDict(UserDict.DictMixin):
    def __init__(self, replication_group, db_path, rpc_port, intro_server='', private_name=''):
        self.db = sqlite.connect(db_path)
        self.data = sqldict.SqliteDict(private_name, self.db)
        self.port = rpc_port
        self.group = replication_group
        self.handler = clusterer.XMLRPC_Clusterer(self, self.group, self.db, rpc_port, intro_server, private_name)

    # clusterer delegate methods
    def snapshot(self):
        data = {}
        for k in self.data.keys():
            data[k] = self.data[k]
        return bencode(data)

    def processSnapshot(self, snapshot):
        d = bdecode(snapshot)
        self.data.clear()
        self.data.update(d)
        
    def deliverAction(self, action):
        d = bdecode(action)
        if d['a'] == Set:
            self.data[d['k']] = d['v']
        elif d['a'] == Del:
            del(self.data[d['k']])
            
    def broadcastAction(self, action, key, value=None):
        d = {'a':action, 'k':key}
        if value is not None:
            d['v'] = value
        self.handler.newAction(bencode(d))
        
    def close(self):
        self.handler.close()

    def __setitem__(self, key, item): self.broadcastAction(Set, key, item)
    def __delitem__(self, key): self.broadcastAction(Del, key)
    def __getitem__(self, key):
        return self.data[key]
    def keys(self):
        return self.data.keys()
          
