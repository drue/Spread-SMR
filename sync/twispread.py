import spread

from twisted.internet import abstract, base, protocol

### Twisted wrapper for connection to spread daemon
### uses a datagram protocol
class Port(base.BasePort):
    def __init__(self, protocol, spread_name="4803@localhost", private_name="", reactor=None):
        self.spread_name = spread_name
        self.private_name = private_name
        self.reactor = reactor
        self.protocol = protocol
        self.connected = 0

    def startListening(self):
        self.mbox = spread.connect(self.spread_name, self.private_name)
        self.connected = 1
        if self.protocol:
            self.protocol.makeConnection(self)
        self.startReading()
        
    def fileno(self):
        return self.mbox.fileno()

    def doRead(self):
        msg = self.mbox.receive()
        self.protocol.datagramReceived(msg, None)
            
    def write(self, datagram, group, msg_type=0, srvc_type=spread.SAFE_MESS):
        return self.mbox.multicast(srvc_type, group, datagram, msg_type)

    def join_group(self, group):
        self.mbox.join(group)

    def leave_group(self, group):
        self.mbox.leave(group)
        
    def stopListening(self):
        self.stopReading()
        self.reactor.callLater(0, self.connectionLost)
        
    def connectionLost(self, reason=None):
        base.BasePort.connectionLost(self, reason)
        if self.protocol:
            self.protocol.doStop()
        self.connected=0
        self.mbox.disconnect()
        del self.mbox


class MultiPort(Port):
    def __init__(self, protocol, spread_name="4803@localhost", private_name="", reactor=None):
        Port.__init__(self, protocol, spread_name, private_name, reactor)
        self.groups = {}
        
    def join_group(self, group, protocol):
        assert group not in self.groups
        self.groups[group] = protocol
        Port.join_group(self, group)

    def leave_group(self, group):
        del(self.groups[group])
        Port.leave_group(self, group)

    def doRead(self):
        msg = self.mbox.receive()
        if type(msg) == spread.RegularMsgType:
            for group in msg.groups:
                try:
                    proto = self.groups[group]
                except KeyError:
                    pass
                else:
                    proto.datagramReceived(msg, None)
        else:
            try:
                proto = self.groups[msg.group]
            except KeyError:
                pass
            else:
                proto.datagramReceived(msg, None)
            
    def startListening(self):
        Port.startListening(self)
        # if we were already connected to some groups, we should rejoin them
        groups = self.groups
        self.groups = {}
        for proto in groups.values():
            proto.makeConnection(self)
            
