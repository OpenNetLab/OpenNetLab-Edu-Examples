from typing import Dict, List, Tuple, Any, DefaultDict
from dataclasses import dataclass
from collections import defaultdict as dd
from enum import Enum

from onl.device import SingleDevice
from onl.sim import Environment, Store, ProcessGenerator
from onl.packet import Packet


class Fate(Enum):
    DECIDED = 1
    PENDING = 2
    FORGOTTEN = 3


@dataclass
class Instance:
    instance: int


@dataclass
class PrepareRequest(Instance):
    proposal: int


@dataclass
class PrepareReply(Instance):
    ok: bool
    proposal: int
    value: str


@dataclass
class AcceptRequest(Instance):
    proposal: int
    value: str


@dataclass
class AcceptReply(Instance):
    ok: bool


@dataclass
class DecidedRequest(Instance):
    sender: int
    done_seq: int
    value: str


@dataclass
class DecidedReply(Instance):
    pass


@dataclass
class State:
    # highest prepare request proposal number received by the acceptor for that instance
    np: int
    # highest accept request proposal number
    na: int
    # value of highest accepted proposal
    va: str


@dataclass
class ProposeInfo:
    # used to ignore deprecated reply
    packet_id: int
    # used for synchronization
    finish_channel: Store
    # used for prepare phase
    maxna: int = 0
    v1: str = ""
    prepare_count: int = 0
    prepared: bool = False
    # used for accept phase
    accept_count: int = 0
    accepted: bool = False

    def reset(self):
        self.maxna = 0
        self.v1 = ""
        self.prepare_count = 0
        self.prepared = False
        self.accept_count = 0
        self.accepted = False


class Paxos(SingleDevice):
    def __init__(
        self,
        env: Environment,
        peers: List[str],
        me: int,
        phase_restart_time: float,
        debug: bool = False,
    ):
        self.env = env
        self.peers = peers
        self.me = me
        self.id = peers[me]
        self.phase_restart_time = phase_restart_time
        self.debug = debug
        self.packet_id = 0

        self.prepare_stores: Dict[str, Store] = dict()
        self.accept_stores: Dict[str, Store] = dict()

        self.max_seq_seen = 0
        self.done_seqs: List[int] = list()
        self.values: Dict[int, str] = dict()
        self.accept_state: Dict[int, State] = dict()
        self.propose_data: Dict[int, ProposeInfo] = dict()

    def run(self, env):
        pass

    def start(self, seq: int, v: str):
        """The application wants paxos to start aggrement on instance seq, with
        proposed value v.

        start() returns right away; the application will call status() to find
        if/when aggrement is reached.
        """
        if seq > self.max_seq_seen:
            self.max_seq_seen = seq
        self.propose(seq, v)

    def propose(self, seq: int, v: str):
        def propose_timeout() -> ProcessGenerator:
            yield self.env.timeout(self.phase_restart_time)
            # already done
            if seq not in self.propose_data:
                return
            temp = self.propose_data[seq]
            if not (temp.prepared and temp.accepted):
                temp.reset()
                self.propose(seq, v)

        n = self.accept_state[seq].np + 1
        self.broadcast_prepare_request(seq, n)
        self.env.process(propose_timeout())

        temp = self.propose_data[seq]
        yield temp.finish_channel.get()
        assert temp.prepared
        self.broadcast_accept_request(seq, n, v)

        yield temp.finish_channel.get()
        assert temp.accepted
        self.broadcast_decide_request(seq, v)

    def do_mem_shrink(self) -> int:
        """The application on this machine is done with all instances <= seq,
        all deprecated states and values should be deleted.
        """
        mins = min(self.done_seqs)
        for seq in self.accept_state.keys():
            if seq <= mins:
                del self.accept_state[seq]
                del self.values[seq]
        return mins + 1

    def min(self) -> int:
        """Return one more than the minimum amoung z_i, where z_i is the
        highest number ever passed to done() on peer i. A peer z_i is -1 if it
        has never called done().
        """
        return self.do_mem_shrink() + 1

    def status(self, seq: int) -> Tuple[Fate, str]:
        """the application wants to know whether this peer thinks an instance
        has been decided, and if so what the agreed value is. status() should
        just inspect the local peer state; it should not contact other Paxos
        peers.
        """
        if seq < self.min():
            return Fate.FORGOTTEN, ""
        if seq in self.values:
            return (Fate.DECIDED, self.values[seq])
        return Fate.PENDING, ""

    def is_decided(self, seq: int) -> bool:
        f, _ = self.status(seq)
        return f == Fate.DECIDED

    def new_packet(self, payload: Any, dst: str = "") -> Packet:
        self.packet_id += 1
        pkt = Packet(
            self.env.now,
            size=40,
            packet_id=self.packet_id,
            src=self.id,
            dst=dst,
            payload=payload,
        )
        return pkt

    def broadcast_prepare_request(self, seq: int, n: int):
        request = PrepareRequest(seq, n)
        assert self.out
        pkt = self.new_packet(request)
        self.out.put(pkt)
        self.propose_data[seq] = ProposeInfo(pkt.packet_id, Store(self.env))
        self.recv_prepare(pkt)

    def broadcast_accept_request(self, seq: int, n: int, v: str):
        request = AcceptRequest(seq, n, v)
        assert self.out
        assert seq in self.propose_data
        pkt = self.new_packet(request)
        self.out.put(pkt)
        self.recv_accept(pkt)

    def broadcast_decide_request(self, seq: int, v: str):
        request = DecidedRequest(seq, self.me, self.done_seqs[self.me], v)
        pkt = self.new_packet(request)
        assert self.out
        self.out.put(pkt)
        self.recv_decided_request(pkt)

    def send_packet_to(self, payload: Any, peer: str):
        if peer == self.id:
            return
        pkt = self.new_packet(payload, peer)
        assert self.out
        self.out.put(pkt)

    def recv_prepare(self, packet: Packet):
        """
        if n > n_p
          n_p = n
          reply prepare_ok(n, n_a, v_a)
        else
          reply prepare_reject
        """
        request: PrepareRequest = packet.payload
        seq = request.instance
        n = request.proposal
        if seq > self.max_seq_seen:
            self.max_seq_seen = request.instance
        state = self.accept_state[seq]
        if n > state.np:
            state.np = n
            reply = PrepareReply(seq, True, state.na, state.va)
        else:
            reply = PrepareReply(seq, False, 0, "")
        self.send_packet_to(reply, packet.src)

    def recv_accept(self, packet: Packet):
        """
        if n >= n_p
          n_p = n
          n_a = n
          v_a = v
          reply accept_ok(n)
        else
          reply accept_reject
        """
        request: AcceptRequest = packet.payload
        seq = request.instance
        n = request.proposal
        v = request.value
        state = self.accept_state[seq]
        if n >= state.np:
            state.np = n
            state.na = n
            state.va = v
            reply = AcceptReply(seq, True)
        else:
            reply = AcceptReply(seq, False)
        self.send_packet_to(reply, packet.src)

    def recv_prepare_reply(self, packet: Packet):
        reply: PrepareReply = packet.payload
        seq = reply.instance
        temp = self.propose_data[seq]
        if reply.ok:
            if reply.proposal > temp.maxna:
                temp.maxna = reply.proposal
                temp.v1 = reply.value
            temp.prepare_count += 1
            if temp.prepare_count > len(self.peers) / 2:
                temp.prepared = True
        else:
            state = self.accept_state[seq]
            if reply.proposal > state.np:
                state.np = reply.proposal

    def recv_accept_reply(self, packet: Packet):
        reply: AcceptReply = packet.payload
        seq = reply.instance
        temp = self.propose_data[seq]
        if reply.ok:
            temp.accept_count += 1
            if temp.accept_count > len(self.peers) / 2:
                temp.accepted = True

    def recv_decided_request(self, packet: Packet):
        request: DecidedRequest = packet.payload
        self.values[request.instance] = request.value
        if self.done_seqs[request.sender] < request.done_seq:
            self.done_seqs[request.sender] = request.done_seq

    def put(self, packet):
        payload = packet.payload
        packet_id = packet.packet_id

        assert packet.dst == "" or packet.dst == self.me
        assert issubclass(type(payload), Instance)

        seq = payload.instance
        temp = self.propose_data[seq]
        if packet_id != temp.packet_id:
            self.dprint(f"ignore deprecated {type(payload)} from {packet.src}")
            return

        if type(payload) == PrepareRequest:
            self.recv_prepare(packet)
        elif type(payload) == AcceptRequest:
            self.recv_accept(packet)
        elif type(payload) == PrepareReply:
            self.recv_prepare_reply(packet)
        elif type(payload) == AcceptReply:
            self.recv_accept_reply(packet)
        elif type(payload) == DecidedRequest:
            self.recv_decided_request(packet)

    def dprint(self, s):
        if self.debug:
            print(f"[{self.id}](time: {self.env.now:.2f})", end=" -> ")
            print(s)
