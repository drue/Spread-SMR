import os
import shutil

import replication
from bencode import bencode,bdecode
from twisted.web import xmlrpc, server
from twisted.internet import reactor, protocol, defer

class NotImplemented(Exception):
    pass

class Delegate(object):
    def deliverAction(self, action):
        raise NotImplemented

    def snapshot(self):
        raise NotImplemented

    def processSnapshot(self, action):
        raise NotImplemented

class ProcessReader(protocol.ProcessProtocol):
    def __init__(self):
        self.df = defer.Deferred()
        self.data = ''
    
    def connectionMade(self):
        self.transport.closeStdin()

    def outReceived(self, data):
        self.data += data

    def outConnectionLost(self):
        pass

    def processEnded(self, reason):
        self.df.callback(self.data)

class ProcessWriter(protocol.ProcessProtocol):
    def __init__(self, data):
        self.data = data
        self.df = defer.Deferred()

    def connectionMade(self):
        self.transport.write(self.data)
        self.transport.closeStdin()

    def errReceived(self, data):
        pass
    
    def outReceived(self, data):
        pass
        
    def processEnded(self, reason):
        self.df.callback(None)
    
class XMLRPC_Clusterer(xmlrpc.XMLRPC, object):
    __slots__ = ('group','name','port', 'delegate', 'rep')
    def __init__(self, delegate, spread, replication_group, dbpath, port, private_name='', intro_server=''):
        # if intro_server is None, then try to form a new replication group if the database is empty
        self.group = replication_group
        self.port = port
        self.delegate = delegate
        xmlrpc.XMLRPC.__init__(self)
        self.tport = reactor.listenTCP(port, server.Site(self))
        self.rep = replication.Replication(self.group, dbpath, private_name=private_name)
        self.rep.makeConnection(spread)
        self.rep.deliverAction = self.delegate.deliverAction
        self.rep.connect_to_db()
        if not intro_server and not self.rep.redCut:
            self.rep.startNewGroup()
            self.initializeDatabase(self.rep.db)
        elif self.rep.redCut:
            self.rep.recoverFromCrash()
            self.initializeDatabase(self.rep.db)
        else:
            self.rep.db.close()
            self.join_cluster(intro_server)
        self.name = self.rep.private_name
        
    def newAction(self, data):
        self.rep.newAction(data)

    def deliverAction(self, action):
        self.delegate.deliverAction(action)

    def initializeDatabase(self, conn):
        self.db = conn
        self.delegate.initializeDatabase(conn)
        
    def xmlrpc_join_cluster(self, group, server_id):
        if group != self.group:
            return xmlrpc.Fault(200, "I'm not a member of that group")
        df = self.rep.introduceNewServer(server_id)
        df.addCallback(self.send_snapshot)
        return df

    def xmlrpc_leave_cluster(self, server_id):
        return self.leave_cluster(server_id)

    def send_snapshot(self, result):
        df = self.delegate.snapshot()
        df.addCallback(self._send_snapshot, result)
        return df
    
    def _send_snapshot(self, data, result):
        action_id, redCut = result
        return (action_id.encode(), redCut, xmlrpc.Binary(data))

    def join_cluster(self, hostport):
        s = xmlrpc.Proxy("http://%s/RPC2" % (hostport))
        def done(result):
            action_id, redCut, snapshot = result
            action_id = replication.ActionId().decode(action_id)
            df = self.delegate.processSnapshot(snapshot.data)
            def start(res, self, action_id, redCut):
                self.rep.startMainProtocol(action_id, redCut)
                self.initializeDatabase(self.rep.db)
            df.addCallback(start, self, action_id, redCut)
        df = s.callRemote("join_cluster", self.group, self.rep.serverId)
        df.addCallback(done)

    def leave_cluster(self, server_id=None):
        if not server_id:
            server_id = self.rep.serverId
        return self.rep.serverLeaving(server_id)

    def close(self):
        self.rep.close()
        self.tport.stopListening()
    
        
