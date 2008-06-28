## This is an implementation of the system described in "On the Path from
## Total Order to Database Replication" by Yair Amir and Ciprian Tutu
## more info at http://www.dsn.jhu.edu/publications.html

from twisted.internet import protocol, reactor, defer
import twispread
from bencode import bencode, bdecode
import spread
import sets
import logging
from copy import copy
import random

from pysqlite2 import dbapi2 as sqlite
import sqldict
import sqlqueue

#colors
Red, Yellow, Green = range(3)

#spread message types
ACTION, STATE, CPC = range(3)

#events
Action, Reg_conf, Trans_conf, State_mess, CPC_mess, Client_req = range(6)

#action types
UserAction, ServerJoin, ServerLeave, Resend, LastResend = range(5)


class ActionId(object):
    def initWithVars(self, server_id, action_index):
        self.server_id = server_id
        self.action_index = action_index
        return self
    
    def encode(self):
        return bencode({'server_id':self.server_id, 'action_index':self.action_index})

    def decode(self, data):
        d = bdecode(data)
        self.server_id = d['server_id']
        self.action_index = d['action_index']
        return self

    def __repr__(self):
        return "<ActionID %s:%s>" % (self.server_id, self.action_index)

    def __str__(self):
        return self.__repr__()
    
    def __cmp__(self, o):
        n = cmp(self.server_id, o.server_id)
        if n == 0:
            return cmp(self.action_index, o.action_index)
        return n

    def __hash__(self):
        return hash(self.encode())        

    
class Action_message(object):
    def initWithVars(self, action_type, action_id, green_line, action, color=Red):
        self.type = action_type
        self.action_id = action_id
        self.green_line = green_line
        self.action = action
        self.color = color
        return self

    def initWithMessage(self, msg):
        d = bdecode(msg.message)
        return self.initWithVars(d['type'], ActionId().decode(d['action_id']),
                                 ActionId().decode(d['green_line']), d['action'], d['color'])

    def setColor(self, ncolor):
        self.color = ncolor
        
    def encodeAsMessage(self):
        d = dict(self.__dict__)
        d['action_id'] = d['action_id'].encode()
        d['green_line'] = d['green_line'].encode()        
        return bencode(d)

    def __cmp__(self, o):
        return cmp(self.action_id, o.action_id)

    def __str__(self):
        return "<action %s>" % self.action_id

    def __repr__(self):
        return str(self)
    
class State_message(object):
    def initWithVars(self, server_id, conf_id, red_cut, green_line,
                attempt_index, prim_component, vulnerable, yellow):
        self.server_id = server_id
        self.conf_id = conf_id
        self.red_cut = red_cut
        self.green_line = green_line
        self.attempt_index = attempt_index
        self.prim_component = prim_component
        self.vulnerable = vulnerable
        self.yellow = yellow
        return self
    
    def initWithMessage(self, msg):
        d = bdecode(msg.message)
        d['yellow']['set'] = [ActionId().decode(x) for x in d['yellow']['set']]
        return self.initWithVars(d['server_id'], d['conf_id'], d['red_cut'],
                                 ActionId().decode(d['green_line']), d['attempt_index'], d['prim_component'],
                                 d['vulnerable'], d['yellow'])

    def encodeAsMessage(self):
        y = dict(self.yellow)
        y['set'] = [x.encode() for x in y['set']]
        d = {'server_id' : self.server_id,
             'conf_id' : self.conf_id,
             'red_cut' : self.red_cut,
             'green_line' : self.green_line.encode(),
             'attempt_index' : self.attempt_index,
             'prim_component': self.prim_component,
             'vulnerable' : self.vulnerable,
             'yellow' : y
             }
        s = bencode(d)
        return s

    def __cmp__(self, o):
        return cmp(self.server_id, o.server_id)

class CPC_message(object):
    def initWithVars(self, server_id, conf_id):
        self.server_id = server_id
        self.conf_id = conf_id
        return self
    
    def encodeAsMessage(self):
        return bencode(self.__dict__)

    def initWithMessage(self, msg):
        d = bdecode(msg.message)
        self.server_id = d['server_id']
        self.conf_id = d['conf_id']
        return self
    
class State(object):
    def __init__(self, parent):
        self.parent = parent
        self.log = logging.getLogger(self.parent.log.name + "." + self.__class__.__name__)

    def switchedToState(self):
        # initialize or do whatever
        pass

    def handleEvent(self, event, msg):
        pass

    def bufferAction(self, action_string):
        self.parent.bufferAction(action_string)

    def sendNow(self, action_string):
        action = self.parent.createAction(action_string)
        self.parent.sync()
        self.parent.sendAction(action)

    def markRed(self, msg):
        msg = Action_message().initWithMessage(msg)
        return self.parent.markRed(msg)

    def markGreen(self, msg):
        msg = Action_message().initWithMessage(msg)
        return self.parent.markGreen(msg)

    def markYellow(self, msg):
        msg = Action_message().initWithMessage(msg)
        return self.parent.markYellow(msg)
        
    def switchState(self, new_state, **kwargs):
        self.parent.switchState(new_state, **kwargs)

    
### actual states
        
class NonPrim(State):
    def handleEvent(self, event, msg):
        if event == Action:
            self.markRed(msg)
        elif event == Reg_conf:
            if self.parent.newCluster and len(msg.members) > 1:
                self.log.critical("Starting new group but %s members are already in there. %s.", len(msg.members), msg.members)
                self.parent.transport.leave_group(self.parent.group)
                return
            elif self.parent.newCluster:
                self.parent.newCluster = False
            self.parent.updateConf(msg)
            self.switchState(ExchangeStates)
        elif event == Client_req:
            self.sendNow(msg)
        else:
            self.parent.log.debug("Ignoring event %s", event)
        
class RegPrim(State):
    def handleEvent(self, event, msg):
        if event == Action:
            self.markGreen(msg)
        elif event == Trans_conf:
            self.switchState(TransPrim)
        elif event == Client_req:
            self.sendNow(msg)
        else:
            self.parent.log.debug("Ignoring event %s", event)


class TransPrim(State):
    def handleEvent(self, event, msg):
        if event == Action:
            self.markYellow(msg)
        elif event == Reg_conf:
            self.parent.updateConf(msg)
            self.parent.vulnerable['status'] = False
            self.parent.yellow['status'] = True
            self.switchState(ExchangeStates)
        elif event == Client_req:
            self.bufferAction(msg)
        else:
            self.parent.log.debug("Ignoring event %s", event)
        

class ExchangeStates(State):
    index = None
    def handleEvent(self, event, msg):
        if event == Trans_conf:
            self.switchState(NonPrim)
        elif event == State_mess:
            m = State_message().initWithMessage(msg)
            if m.conf_id == self.parent.conf['conf_id']:
                if m.server_id in self.parent.stateIndex:
                    self.parent.stateMessages.remove(self.parent.stateIndex[m.server_id])
                self.parent.stateMessages.append(m)
                self.parent.stateIndex[m.server_id] = m
            if len(self.parent.stateMessages) == len(self.parent.conf['set']):
                self.endExchangeStates()
                self.switchState(ExchangeActions, index=self.index)
        elif event == Action:
            m = self.markRed(msg)
            if m.action_id.action_index == self.parent.redCut[m.action_id.server_id]:
                self.parent.sendStateMessage()
        elif event == Client_req:
            self.bufferAction(msg)
        else:
            self.parent.log.debug("Ignoring event %s", event)
        


    def endExchangeStates(self):
        sm = self.parent.stateMessages
        if [s for s in sm if s.green_line != sm[0].green_line]:
            #xxx check if most updated server, then start sending
            sm.sort()
            self.log.debug("showdown: %s", [(s.server_id, s.green_line) for s in sm])
            equals = []
            winning = True
            for s in sm:
                try:
                    if self.parent.green_line == s.green_line:
                        equals.append(s)
                    elif s.green_line.action_index > self.parent.greenLines[s.green_line.server_id].action_index:
                        winning = False
                        break
                except KeyError:
                    winning = False
                    break
            equals.sort()
            if not winning:
                self.log.debug("not winning")
            if winning and equals[0].server_id == self.parent.serverId:
                self.log.debug("the equals are %s, %s", [x.server_id for x in equals], self.parent.greenQueue)
                # retransmit all greens that need retransmitting
                n = 0
                got = []
                for action in self.parent.greenQueue.reversed():
                    self.log.debug("checking action %s", action.action_id)
                    x = 0
                    l = [o for o in sm if o not in got]
                    if not l:
                        break
                    for o in l:
                        if action.action_id == o.green_line:
                            got.append(o)
                            self.log.debug("%s has action %s", o.server_id, action.action_id)
                        else:
                            # nope, resend it
                            x += 1
                    if x:
                        n -= 1
                self.index = n
                self.log.debug("I %s am the most updated server, sending last %s actions", self.parent.serverId, self.index)
                if self.index != 0:
                    self.log.debug("%s", self.parent.greenQueue)
                    assert abs(self.index) <= len(self.parent.greenQueue)
                    if self.index == -1:
                        last = True
                    else:
                        last = False
                    self.parent.resendAction(self.parent.greenQueue[self.index], last)
        return
    
    def switchedToState(self):
        self.parent.clearStateMessages()
        self.parent.sendStateMessage()
        
class ExchangeActions(State):
    index = None
    count = 0
    def handleEvent(self, event, msg):
        if event == Action:
            action = Action_message().initWithMessage(msg)
            if action.type != Resend and action.type != LastResend:
                return
            # update what we think everyones state will be after processing this message
            for s in self.parent.stateMessages:
                if s.red_cut[action.action_id.server_id] == action.action_id.action_index - 1:
                    s.red_cut[action.action_id.server_id] = action.action_id.action_index
            state = self.parent.stateIndex[msg.sender]
            # mark color
            if self.green and action.color == Green and (action.action_id.server_id not in self.parent.greenLines or action.action_id.action_index == self.parent.greenLines[action.action_id.server_id].action_index + 1):
                action.color = Red
                self.parent.markGreen(action)
            elif action.action_id in state.yellow:
                self.parent.markYellow(action)
            else:
                self.parent.markRed(action)
            # check if it is our turn to send another red
            if not self.green and action.type == Resend and self.leader == self.parent.serverId:
                if action.action_id.action_index < self.parent.redCut[action.action_id.server_id]:
                    last = False
                    if action.action_id.action_index + 1 == self.parent.redCut[action.action_id.server_id] and not self.scount:
                        last = True
                    self.parent.resendAction(self.parent.redIndex[ActionId().initWithVars(action.action_id.server_id, action.action_id.action_index + 1)], last)
                else:
                    if self.scount:
                        self.sendNextActionGroup()
            # check if we should send the next green
            elif self.green and msg.sender == self.parent.serverId and action.type==Resend and self.index + 1 != 0:
                self.index = self.index + 1
                last = False
                if self.index == -1:
                    last = True
                self.parent.resendAction(self.parent.greenQueue[self.index], last)
            # red check
            if action.type == LastResend:
                if self.green:
                    self.green = False
                self.checkRedResend()
        elif event == Trans_conf:
            self.switchState(NonPrim)
        elif event == Client_req:
            self.bufferAction(msg)
        elif event == State_mess:
            m = State_message().initWithMessage(msg)
            if m.conf_id == self.parent.conf['conf_id']:
                self.parent.stateMessages.remove(self.parent.stateIndex[m.server_id])
                self.parent.stateMessages.append(m)
                self.parent.stateIndex[m.server_id] = m
                # if we thought there were greens to resend but really all the missing greens were marked yellow
                # then once everyone marks their yellows green then we can proceed to reds
                if self.green and not [x for x in self.parent.stateMessages if x.green_line != self.parent.stateMessages[0].green_line]:
                    self.checkRedResend()
                
        else:
            self.parent.log.debug("Ignoring event %s", event)

    def checkRedResend(self):
        self.countRed()
        if self.count == 0:
            self.parent.endOfRetrans()
        elif self.leader == self.parent.serverId:
            if not self.scount:
                self.log.debug(">>> ERRR count %s, scount %s", self.count, self.scount)
            self.sendNextActionGroup()
                         

    def countRed(self):
        self.log.debug(">>> Start Red Count %s", [(s.server_id, s.red_cut) for s in self.parent.stateMessages])
        count = 0
        leader = None
        scount = {}
        
        for server in self.parent.stateMessages:
            c = 0
            sc = {}
            self.log.debug("checking %s", server.server_id)
            for s in self.parent.redCut:
                lowest = server.red_cut[s]
                for x in self.parent.stateMessages:
                    if x.red_cut[s] < lowest:
                        lowest = x.red_cut[s]
                n = server.red_cut[s] - lowest
                if n > 0:
                    sc[s] = lowest
                    c += n
            self.log.debug("has %s more messages", c)
            if not leader or c > count or (c == count and server.server_id < leader):
                leader = server.server_id
                count = c
                scount = sc
        self.count = count
        self.leader = leader
        self.scount = scount
        self.log.debug("Red count is %s %s %s", self.leader, self.count, self.scount)
        
    def switchedToState(self, index=None):
        self.index = index
        self.green = False
        self.count = 0
        if [s for s in self.parent.stateMessages if self.parent.stateMessages[0].green_line != s.green_line]:
            self.log.debug( '>>> going green')
            self.green = True
        else:
            self.green = False
        if not self.green:
            self.checkRedResend()
            
    def sendNextActionGroup(self):
        s = self.scount.keys()[0]
        low = self.scount.pop(s)
        last = False
        if low + 1 == self.parent.redCut[s] and not self.scount:
            last = True
        try:
            self.parent.resendAction(self.parent.redIndex[ActionId().initWithVars(s, low + 1)], last)
        except KeyError:
            l = [`a` for a  in self.parent.redIndex.keys()]
            l.sort()
            self.log.debug(">>> tried to send action not in eindex %s %s %s", s, low + 1, l)
            raise KeyError
        
class Construct(State):
    def switchedToState(self):
        self.cpc = 0
        
    def handleEvent(self, event, msg):
        if event == Trans_conf:
            self.switchState(No, cpc=self.cpc)
        elif event == CPC_mess:
            cpc = CPC_message().initWithMessage(msg)
            if cpc.conf_id == self.parent.conf['conf_id']:
                self.cpc += 1
                if self.cpc == len(self.parent.conf['set']):
                    self.parent.install()
                    self.switchState(RegPrim)
                    self.parent.handleBufferedRequests()
        elif event == Client_req:
            self.bufferAction(msg)
        else:
            self.parent.log.debug("Ignoring event %s", event)

class No(State):
    def switchedToState(self, cpc=None):
        self.cpc = cpc
        
    def handleEvent(self, event, msg):
        if event == Reg_conf:
            self.parent.updateConf(msg)
            self.parent.vulnerable['status'] = False
            self.switchState(ExchangeStates)
        elif event == CPC_mess:
            self.cpc += 1
            if self.cpc == len(self.parent.conf['set']):
                self.switchState(Un)
        elif event == Client_req:
            self.bufferAction(msg)            
        else:
            self.parent.log.debug("Ignoring event %s", event)
        
class Un(State):
    def handleEvent(self, event, msg):
        if event == Reg_conf:
            self.parent.updateConf(msg)
            self.switchState(ExchangeStates)
        elif event == Action:
            self.parent.install()
            self.markYellow(msg)
            self.switchState(TransPrim)
        elif event == Client_req:
            self.bufferAction(msg)
        else:
            self.parent.log.debug("Ignoring event %s", event)

    
class Replication(protocol.AbstractDatagramProtocol):
    __doc__ = """ def __init__(self, replication_group, data_pathlogger=None):
    replication_group = spread group name to operate on
    data_path = path to file where you want replication state to be stored, ":memory:" means don't touch the disk
    """
    def __init__(self, replication_group, dbpath=":memory:", private_name='', logger=None):
        self.group = replication_group
        self.joined = False
        self.dbpath = dbpath
        self.log = logger
        self.actionIndex = 0
        self.conf = {'conf_id':0, 'set':[]}
        self.attemptIndex = 0
        self.primComponent = {'prim_index':0, 'attempt_index':0, 'set':[]}
        self.actionBuffer = []
        self.redCut = {}
        self.greenLines = {}
        self.green_line = None
        self.stateMessages = []
        self.stateIndex = {}
        self.vulnerable = {'status':False, 'prim_index':0, 'attempt_index':0, 'set':[], 'bits':[]}
        self.yellow = {'status':False, 'set':[]}
        self.state = None
        self.states = {}
        self.ltconf = False
        self.intros = {}
        self.exits = []
        self.newCluster = False
        self.closed = False
        self.private_name = private_name
        
    def connect_to_db(self, reset=False):
        self.db = sqlite.connect(self.dbpath)
        self.state_store = sqldict.SqliteDict('replication_state', self.db)
        self.greenQueue = sqlqueue.SqliteQueue('replication_greenQueue', self.db)
        self.redIndex = sqldict.SqliteDict('replication_redIndex', self.db)
        self.ongoingQueue = sqlqueue.SqliteQueue('replication_ongoingQueue', self.db)
        if reset:
            self.reset_db()
        try:
            name = self.state_store['private_name']
            if self.private_name and name != self.private_name:
                raise NameError, "Private name doesn't match name stored in database"
        except KeyError:
            if not self.private_name:
                self.private_name = 'r-' + ''.join([chr(random.randint(0,255)) for x in range(4)]).encode('hex')
            self.state_store['private_name'] = self.private_name
        self.private_name = self.state_store['private_name']

        if self.state_store.has_key('primComponent'):
            self.primComponent = self.state_store['primComponent']
            self.actionIndex = self.state_store['actionIndex']
            self.greenLines = self.state_store['greenLines']
            self.green_line = self.state_store['green_line']
            self.yellow = self.state_store['yellow']
            self.vulnerable = self.state_store['vulnerable']
            self.redCut = self.state_store['redCut']

    def reset_db(self):
        self.state_store.clear()
        self.greenQueue.clear()
        self.redIndex.clear()
        self.ongoingQueue.clear()        
        
    def sync(self):
        self.state_store['primComponent'] = self.primComponent
        self.state_store['actionIndex'] = self.actionIndex
        self.state_store['greenLines'] = self.greenLines
        self.state_store['green_line'] = self.green_line
        self.state_store['yellow'] = self.yellow
        self.state_store['vulnerable'] = self.vulnerable
        self.state_store['redCut'] = self.redCut        
        self.db.commit()

    def startNewGroup(self):
        # only use this when starting the first server in a new cluster
        self.newCluster = True
        self.startMainProtocol()

    def introduceNewServer(self, server_id):
        action = self.createAction(server_id, ServerJoin)
        self.sendAction(action)
        self.intros[server_id] = defer.Deferred()
        return self.intros[server_id]

    def serverLeaving(self, server_id):
        action = self.createAction(server_id, ServerLeave)
        self.sendAction(action)
        return True

    def serverIntroduced(self, server_id, action_id, redCut):
        self.intros[server_id].callback((action_id, redCut))

    def close(self):
        self.closed = True
        if self.transport:
            self.transport.stopListening()
        
    def makeConnection(self, transport):
        self.transport = transport
        self.numPorts = 1
        self.serverId = transport.mbox.private_group
        self.log = logging.getLogger("Replication." + self.serverId)
        self.states[RegPrim] = RegPrim(self)
        self.states[TransPrim] = TransPrim(self)
        self.states[ExchangeStates] = ExchangeStates(self)
        self.states[ExchangeActions] = ExchangeActions(self)        
        self.states[Construct] = Construct(self)
        self.states[No] = No(self)
        self.states[Un] = Un(self)
        self.states[NonPrim] = NonPrim(self)
        if self.joined:
            self.recoverFromCrash()
            
    def startMainProtocol(self, action_id = None, redCut = None):
        if self.closed:
            return
        self.connect_to_db(reset=True)
        if not action_id:
            action_id = ActionId().initWithVars(self.serverId, 0)
        if redCut:
            self.redCut = redCut
        self.log.debug(">>> starting main protocol %s %s", action_id, redCut)
        self.redCut[self.serverId] = self.actionIndex
        self.green_line = action_id
        self.switchState(NonPrim)
        self.transport.join_group(self.group, self)
        self.joined = True
        
    def datagramReceived(self, msg, addr=None):
        if isinstance(msg, spread.RegularMsgType):
            if msg.msg_type == ACTION:
                event = Action
            elif msg.msg_type == STATE:
                self.log.debug( ">>> got state msg %s", msg.sender)
                event = State_mess
            elif msg.msg_type == CPC:
                self.log.debug( ">>> got cpc msg %s", msg.sender)
                event = CPC_mess
            else:
                ## unknown message!
                raise TypeError("Unknown message type %s" % msg.msg_type)
        elif isinstance(msg, spread.MembershipMsgType):
            ## membership message
            if msg.reason == 0:
                self.log.debug( ">>> got transconf msg %s", msg.members)
                event = Trans_conf
                self.ltconf = True
            else:
                self.log.debug( ">>> got regconf msg %s %s", msg.members, msg.group_id)
                if not self.ltconf:
                    self.state.handleEvent(Trans_conf, msg)
                else:
                    self.ltconf = False
                event = Reg_conf
                
        self.state.handleEvent(event, msg)
        
    def switchState(self, new, **kwargs):
        self.log.debug( '>>> switchedState %s %s', new, self.state)
        self.state = self.states[new]
        self.state.switchedToState(**kwargs)
        
    def updateConf(self, msg):
        self.conf['conf_id'] = `msg.group_id`
        self.conf['set'] = msg.members
        
    def clearStateMessages(self):
        self.stateMessages = []
        self.stateIndex = {}

    def expireGreen(self, pc):
        if isinstance(self.state, RegPrim) and self.primComponent['prim_index'] == pc:
            g = self.greenQueue.pop()
    
    def markGreen(self, action):
        self.markRed(action)
        if action.color != Green:
            """
            try:
                if action.action_id.action_index <= self.greenLines[action.action_id.server_id].action_index:
                    self.log.debug(">>> already in green queue!! %s %s", action, self.greenLines[action.action_id.server_id])
                    raise AssertionError
            except KeyError:
                pass
            """
            self.log.debug(">>> Marking Green %s", action.action_id)
            if action.action_id not in self.redIndex:
                self.log.debug(">>> green marking an action not in redindex %s %s %s", action, self.redCut[action.action_id.server_id], self.greenQueue)
                raise AssertionError

            if action.type == ServerJoin and action.action not in self.greenLines:
                self.log.debug( ">>> got ServerJoin %s", action.action)
                self.greenLines[action.action] = ActionId().initWithVars(action.action, 0)
                self.redCut[action.action] = 0
                if action.action_id.server_id == self.serverId:
                    self.serverIntroduced(action.action, action.action_id, dict(self.redCut))
            elif action.type == ServerLeave and action.action in self.greenLines:
                self.log.debug( ">>> got ServerLeave %s, %s", action.action, self.serverId)
                del self.greenLines[action.action]
                del self.redCut[action.action]
                self.primComponent['set'] = tuple([x for x in self.primComponent['set'] if x != action.action])
                if action.action == self.serverId:
                    ## XXX all done, stop listening, quit the cluster
                    self.close()
                elif len(self.primComponent['set']) == len(self.redCut) and len(self.redCut) == len(self.conf['set']):
                    # mark white
                    self.greenQueue.clear()
            elif action.type == UserAction or action.type == Resend or action.type == LastResend:
                self.deliverAction(action.action)
            self.log.debug(">>> delivered %s:%s", action.action_id.server_id, action.action_id.action_index)
            if self.yellow['set'] and action.action_id in self.yellow['set']:
                self.yellow['set'].remove(action.action_id)
            del(self.redIndex[action.action_id])
            action.color = Green
            self.green_line = action.action_id
            self.greenLines[action.action_id.server_id] = action.action_id
            self.greenQueue.push(action)
            if isinstance(self.state, RegPrim):
                if len(self.primComponent['set']) == len(self.redCut) and len(self.conf['set']) == len(self.redCut):
                    reactor.callLater(30, self.expireGreen, self.primComponent['prim_index'])
                self.sync()
        else:
            self.log.debug("didn't mark %s green bacause color is %s", action.action_id, action.color)
        return action
    
    def markYellow(self, action):
        self.markRed(action)
        if self.redIndex.has_key(action.action_id):
            self.yellow['set'].append(action.action_id)
            self.log.debug(">>> Marking Yellow %s", action.action_id)
        return action

    def markRed(self, action):
        if self.redCut[action.action_id.server_id] == action.action_id.action_index - 1:
            self.log.debug(">>> Marking Red %s resend:%s", action.action_id, action.type == Resend)            
            self.redCut[action.action_id.server_id] += 1
            self.redIndex[action.action_id] = action
            action.color = Red
            if action.action_id.server_id == self.serverId:
                n = self.ongoingQueue.pop()
                assert n.action_id.action_index == action.action_id.action_index
        return action
    
    ###  this one is used by protocols to introduce a new action into the system
    def newAction(self, action_string):
        self.state.handleEvent(Client_req, action_string)

    ## action has been ordered, subclasses or delegates should decode the string and commit the change to the database
    def deliverAction(self, action_string):
        assert not "Implemented"

    
    # these are internal
    def bufferAction(self, action_string):
        self.actionBuffer.append(action_string)
        
    def createAction(self, action_string, action_type=UserAction):
        action = Action_message()
        self.actionIndex += 1
        action.initWithVars(action_type, ActionId().initWithVars(self.serverId, self.actionIndex),
                            self.green_line, action_string)
        self.ongoingQueue.push(action)
        return action
    
    def sendAction(self, action):
        self.log.debug('Sending Action %s:%s', action.action_id.server_id, action.action_id.action_index)
        self.transport.write(action.encodeAsMessage(), self.group, msg_type=ACTION, srvc_type=spread.SAFE_MESS)
        
    def resendAction(self, action, last = False):
        self.log.debug('Resending Action %s:%s', action.action_id.server_id, action.action_id.action_index)
        if not last:
            action.type = Resend
        else:
            action.type = LastResend
        self.transport.write(action.encodeAsMessage(), self.group, msg_type=ACTION, srvc_type=spread.FIFO_MESS)

    def sendCPC(self):
        msg = CPC_message().initWithVars(self.serverId, self.conf['conf_id']).encodeAsMessage()
        self.transport.write(msg, self.group, msg_type=CPC, srvc_type=spread.SAFE_MESS)

    def sendStateMessage(self):
        self.log.debug(">>> Sending StateMessage %s", self.green_line)
        s = State_message()
        s.initWithVars(self.serverId, self.conf['conf_id'],
                       self.redCut, self.green_line, self.attemptIndex,
                       self.primComponent, self.vulnerable, self.yellow)
        self.transport.write(s.encodeAsMessage(), self.group, msg_type=STATE, srvc_type=spread.FIFO_MESS)
         
    def retransIfTime(self):
        pass

    def computeKnowledge(self):
        newPrim = self.stateMessages[0].prim_component
        for s in self.stateMessages[1:]:
            if s.prim_component['prim_index'] > newPrim['prim_index']:
                newPrim = s.prim_component
            elif s.prim_component['prim_index'] == newPrim['prim_index'] and s.prim_component['attempt_index'] > newPrim['attempt_index']:
                newPrim = s.prim_component
        self.primComponent = newPrim

        updatedGroup = [s for s in self.stateMessages if s.prim_component['prim_index'] == newPrim['prim_index']]
        validGroup = [s for s in self.stateMessages if s.yellow['status']]
        self.attemptIndex = max([s.attempt_index for s in updatedGroup])
        
        if validGroup:
            self.yellow['status'] = True
            yellows = reduce(lambda a,b: a&b, [sets.Set(x.yellow['set']) for x in validGroup])
            self.yellow['set'] = list(yellows)
            self.yellow['set'].sort()
        else:
            self.yellow['status'] = False


        self.log.debug('>>> computing vulnerability %s', [(s.server_id, s.vulnerable) for s in self.stateMessages])
        for s in self.stateMessages:
            s.vulnerable['set'].sort()

        ### xxx need to use copies here
        ostates = [copy(s) for s in self.stateMessages if s.vulnerable['status']]
        ostateIndex = {}
        for s in ostates:
            ostateIndex[s.server_id] = s
            
        for s in ostates:
            try:
                if (s.server_id not in newPrim['set'] or
                    [x for x in s.vulnerable['set'] if (ostateIndex[x].vulnerable['status'] or
                                                           ostateIndex[x].vulnerable['prim_index'] != s.vulnerable['prim_index'] or
                                                           ostateIndex[x].vulnerable['attempt_index'] != s.vulnerable['attempt_index'])]):
                    self.log.debug("invulning %s" % s.server_id)
                    self.stateIndex[s.server_id].vulnerable['status'] = False
            except KeyError:
                self.stateIndex[s.server_id].vulnerable['status'] = False
                self.log.debug("invulning2 %s" % s.server_id)                
        
        ostates = [copy(s) for s in self.stateMessages if s.vulnerable['status']]
        ostateIndex = {}
        for s in ostates:
            bits = list(s.vulnerable['bits'])
            for x in s.vulnerable['set']:
                if ostateIndex[x].vulnerable['status']:
                    for z in range(len(bits)):
                        bits[z] = bits[z] or self.stateIndex[x].vulnerable['bits'][z]
            if s.server_id == self.serverId:
                self.stateIndex[s.server_id].vulnerable['bits'] = bits
            if not [x for x in bits if not x]:
                self.stateIndex[s.server_id].vulnerable['status'] = False
                self.log.debug("invulning3 %s" % s.server_id)
        self.vulnerable = self.stateIndex[self.serverId].vulnerable
        
    def endOfRetrans(self):
        self.computeKnowledge()
        if self.isQuorum():
            self.attemptIndex += 1
            self.vulnerable['status'] = True
            self.vulnerable['prim_index'] = self.primComponent['prim_index']
            self.vulnerable['attempt_index'] = self.attemptIndex
            self.vulnerable['set'] = self.conf['set']
            self.vulnerable['bits'] = [False] * len(self.stateMessages)
            self.sync()
            self.sendCPC()
            self.switchState(Construct)
        else:
            self.sync()
            self.handleBufferedRequests()
            self.switchState(NonPrim)
            
    def isQuorum(self):
        l = [(x.server_id, x.vulnerable['attempt_index'], x.vulnerable['prim_index']) for x in self.stateMessages if x.vulnerable['status']]
        if l:
            self.log.debug( ">>> no quorum, vulnerable %s %s", l, [(s.server_id, s.vulnerable) for s in self.stateMessages])
            return False
        if not self.primComponent['set'] or 1.0 * len(self.conf['set']) / len(self.primComponent['set']) > 0.5:
            return True
        return False
    
    def handleBufferedRequests(self):
        actions = []
        for msg in self.actionBuffer:
            action = self.createAction(msg)
            actions.append(action)
        self.sync()
        if actions:
            self.log.debug(">>> sending buffered requests...")
        for action in actions:
            self.sendAction(action)
        self.actionBuffer = []
        
    def install(self):
        if self.yellow['status']:
            for action_id in copy(self.yellow['set']):
                self.markGreen(self.redIndex[action_id])
        self.yellow['status'] = False
        self.yellow['set'] = []
        self.primComponent['prim_index'] += 1
        self.primComponent['attempt_index'] = self.attemptIndex
        self.primComponent['set'] = self.vulnerable['set']
        self.attemptIndex = 0
        r = self.redIndex.values()
        r.sort()
        for action in r:
            self.markGreen(action)
        self.sync()
        if len(self.primComponent['set']) == len(self.redCut):
            ### marking white
            self.greenQueue.clear()
        
    def recoverFromCrash(self):
        self.switchState(NonPrim)
        self.connect_to_db()
        for action in self.ongoingQueue:
            self.markRed(action)
        self.sync()
        self.transport.join_group(self.group, self)
        self.joined = True
