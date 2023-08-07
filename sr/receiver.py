import json
from pathlib import Path
from typing import Optional, List
from onl.packet import Packet
from onl.device import Device, OutMixIn
from onl.sim import Environment


class SRReceiver(Device, OutMixIn):
    def __init__(
        self,
        env: Environment,
        seqno_width: int,
        window_size: int,
        debug: bool = False,
    ):
        self.env = env
        # the bits of the sequence number, which decides the sequence
        # number range and window size of selective repeat
        self.seqno_width = seqno_width
        self.seqno_range = 2**self.seqno_width
        self.window_size = window_size
        assert self.window_size <= self.seqno_range // 2
        self.seqno_start = 0
        self.message = ""
        self.recv_window: List[Optional[Packet]] = [None] * self.window_size
        self.recv_start = 0
        self.debug = debug

    def new_packet(self, ackno: int) -> Packet:
        return Packet(time=self.env.now, size=40, packet_id=ackno)

    def put(self, packet: Packet):          
        seqno = packet.packet_id
        data = packet.payload
        lwnd_start = (
            self.seqno_start + self.seqno_range - self.window_size
        ) % self.seqno_range
        rwnd_start = self.seqno_start
        if self.is_valid_seqno(rwnd_start, self.window_size, self.seqno_range, seqno):
            dist = (seqno + self.seqno_range - self.seqno_start) % self.seqno_range
            self.recv_window[(self.recv_start + dist) % self.window_size] = packet
            while self.recv_window[self.recv_start] is not None:
                cached_pkt = self.recv_window[self.recv_start]
                assert cached_pkt
                self.message += cached_pkt.payload
                self.recv_window[self.recv_start] = None
                self.recv_start = (self.recv_start + 1) % self.window_size
                self.seqno_start = (self.seqno_start + 1) % self.seqno_range
            ack_pkt = self.new_packet(seqno)
            assert self.out
            self.out.put(ack_pkt)
            self.dprint(f"send ack {self.seqno_start}")
        elif self.is_valid_seqno(lwnd_start, self.window_size, self.seqno_range, seqno):
            ack_pkt = self.new_packet(seqno)
            assert self.out
            self.out.put(ack_pkt)
            self.dprint(f"send ack {self.seqno_start}")
        else:
            self.dprint(f"discard {data} on invalid seqno: {seqno}")

    def is_valid_seqno(self, start: int, winsize: int, array_size: int, target: int):
        dist = (target + array_size - start) % array_size
        return 0 <= dist < winsize


    def dprint(self, s: str):
        if self.debug:
            print(f"[receiver](time: {self.env.now:.2f})", end=" -> ")
            print(s)
