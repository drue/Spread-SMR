import spread
import os
from spread import *
import random

groups = {}
group_ids = {}

def nextGID(group):
    gid = group_ids.get(group, 0)
    gid += 1
    group_ids[group] = gid
    return gid

def randName():
    return "dummy-" + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')

class DummyRegMsg(object):
    __slots__ = ['msg_type', 'sender', 'groups', 'message']
    def __init__(self, msg_type, sender, sent_groups, message):
        self.msg_type = msg_type
        self.sender = sender
        self.groups = sent_groups
        self.message = message
RegularMsgType = DummyRegMsg

class DummyMemMsg(object):
    def __init__(self, reason, group, group_id, members=None, extra=None):
        self.reason = reason
        self.group = group
        self.group_id = group_id
        self.members = members
        self.extra = extra
MembershipMsgType = DummyMemMsg

class DummyMBox(object):
    def __init__(self, name, private_name, priority, membership):
        self.name = name
        self.private_group = private_name
        self.priority = priority
        self.membership = membership
        self.q = []
        self.fds = os.pipe()
        
    def queueMsg(self, msg, trans=False):
        if not trans:
            self.q.insert(0, msg)
        else:
            self.q.append(msg)
        os.write(self.fds[1], 'X')

    def resetQueue(self):
        if self.q:
            os.read(self.fds[0], len(self.q))
            self.q = []

    def disconnect(self):
        pass

    def fileno(self):
        return self.fds[0]

    def join(self, group):
        l = groups.get(group, [])
        assert self not in l
        o = list(l)
        l.append(self)
        groups[group] = l

        for p in o:
            if len(p.q):
                p.queueMsg(DummyMemMsg(0, group, 0, [n.private_group for n in l]), trans=True)
        for p in l:
            p.queueMsg(DummyMemMsg(spread.CAUSED_BY_JOIN, group, nextGID(group),
                                      [n.private_group for n in l], self.name))

    def leave(self, group):
        l = groups[group]
        assert self in l
        l.remove(self)
        groups[group] = l
        self.resetQueue()
        gid = nextGID(group)
        for p in l:
            if len(p.q):
                p.queueMsg(DummyMemMsg(0, group, 0, [n.private_group for n in l]), trans=True)
            p.queueMsg(DummyMemMsg(spread.CAUSED_BY_LEAVE, group, gid,
                                      [n.private_group for n in l], self.name))
        self.queueMsg(DummyMemMsg(spread.CAUSED_BY_LEAVE, group, gid, [], self.name))


    def receive(self):
        os.read(self.fds[0], 1)
        return self.q.pop()

    def poll(self):
        return len(self.q)

    def multicast(self, service_type, group, message, message_type=0):
        l = groups.get(group, [])
        if self in l:
            for p in l:
                p.queueMsg(DummyRegMsg(message_type, self.name, [group], message))
    

def connect(name="", private_name="", priority=0, membership=1):
    if not private_name:
        private_name = randName()
    return DummyMBox(private_name, private_name, priority, membership)

