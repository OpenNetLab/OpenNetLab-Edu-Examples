from typing import Deque
from collections import deque
from onl.packet import Packet
from onl.device import Device, OutMixIn
from onl.sim import Environment, Store
from onl.utils import Timer


class GBNSender(Device, OutMixIn):
    def __init__(
        self,
        env: Environment,
        seqno_width: int,
        timeout: float,
        window_size: int,
        message: str,
        debug: bool = False,
    ):
        self.env = env
        # the bits of the sequence number, which decides the sequence
        self.seqno_width = seqno_width
        # number range and window size of GBN
        self.seqno_range = 2**self.seqno_width
        self.window_size = window_size
        assert self.window_size <= self.seqno_range - 1
        self.timeout = timeout
        self.debug = debug
        self.message = message
        # the sequence number of the next character to be sent
        self.seqno = 0
        # the absolute index of the next character to be sent
        self.absno = 0
        # sequence number of first packet in outbound buffer
        self.seqno_start = 0
        # packet buffer to save the packets that havn't been acknowledged by receiver
        self.outbound: Deque[Packet] = deque()
        # use `self.finish_channel.put(True)` to terminate the sending process
        self.finish_channel: Store = Store(env)
        # A timer. Call the timeout_callback function when timeout occurs
        self.timer = Timer(
            self.env,
            self.timeout,
            auto_restart=True,
            timeout_callback=self.timeout_callback,
        )
        self.proc = env.process(self.run(env))

    def new_packet(self, seqno: int, data: str) -> Packet:
        return Packet(time=self.env.now, size=40, packet_id=seqno, payload=data)

    def send_available(self):
        while len(self.outbound) < self.window_size and self.absno < len(self.message):
            packet = self.new_packet(self.seqno, self.message[(self.absno+1) % len(self.message)])
            self.send_packet(packet)
            self.seqno = (self.seqno + 1) % self.seqno_range
            self.absno += 1
            self.outbound.append(packet)
        self.timer.restart(self.timeout)

    def timeout_callback(self):
        self.dprint("timeout")
        for pkt in self.outbound:
            self.send_packet(pkt)

    def send_packet(self, packet: Packet):
        self.dprint(f"send {packet.payload} on seqno {packet.packet_id}")
        assert self.out
        self.out.put(packet)

    def run(self, env: Environment):
        self.send_available()
        yield self.finish_channel.get()

    def put(self, packet: Packet):
        """Receiving acknowledgement packet from receiver"""
        ackno = packet.packet_id
        if self.is_valid_ackno(ackno):
            num = (ackno + self.seqno_range - self.seqno_start) % self.seqno_range + 1
            for _ in range(num):
                self.outbound.popleft()
                self.seqno_start = (self.seqno_start + 1) % self.seqno_range
        self.send_available()

        if len(self.outbound) == 0 and self.absno == len(self.message):
            self.finish_channel.put(True)

    def is_valid_ackno(self, ackno):
        if ackno < 0 and ackno >= self.seqno_range:
            return False
        for pkt in self.outbound:
            if pkt.packet_id == ackno:
                return True
        return False

    def dprint(self, s):
        if self.debug:
            print(f"[sender](time: {self.env.now:.2f})", end=" -> ")
            print(s)
