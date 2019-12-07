from dataclasses import dataclass
import struct
from collections import namedtuple


_RQ = namedtuple("RQ", ("filename", "mode"))


def _decode_rq(pkt):
    fname, mode = pkt.rstrip(b"\0").split(b"\0", 2)
    return _RQ(fname, mode)


def _encode_rq(rq):
    return rq.filename + b"\0" + rq.mode + b"\0"


@dataclass
class ReadRequest:
    "Read Request (RRQ)"
    opcode = 1
    filename: bytes
    mode: bytes

    @classmethod
    def decode(cls, pkt):
        (opcode,) = struct.unpack("!h", pkt[0:2])
        assert opcode == cls.opcode

        parsed = _decode_rq(pkt[2:])
        return cls(filename=parsed.filename, mode=parsed.mode)

    def encode(self):
        return struct.pack("!h", self.opcode) + _encode_rq(
            _RQ(self.filename, self.mode)
        )


@dataclass
class WriteRequest:
    "Write Request (WRQ)"
    opcode = 2
    filename: bytes
    mode: bytes

    @classmethod
    def decode(cls, pkt):
        (opcode,) = struct.unpack("!h", pkt[0:2])
        assert opcode == cls.opcode

        parsed = _decode_rq(pkt[2:])
        return cls(filename=parsed.filename, mode=parsed.mode)

    def encode(self):
        return struct.pack("!h", self.opcode) + _encode_rq(
            _RQ(self.filename, self.mode)
        )


@dataclass
class Data:
    "Data (DATA)"
    opcode = 3
    block_num: int
    data: bytes

    # This is the maximum size of a data packet's data buffer per packet.
    max_data_size = 512 - 4

    @classmethod
    def decode(cls, pkt):
        (opcode, block_num) = struct.unpack("!hh", pkt[0:4])
        assert opcode == cls.opcode

        data = pkt[4:]
        return cls(block_num=block_num, data=data)

    def encode(self):
        return struct.pack("!hh", self.opcode, self.block_num) + self.data


@dataclass
class Acknowledgement:
    "Acknowledgement (ACK)"
    opcode = 4
    block_num: int

    @classmethod
    def decode(cls, pkt):
        (opcode, block_num) = struct.unpack("!hh", pkt)
        assert opcode == cls.opcode

        return cls(block_num=block_num)

    def encode(self):
        return struct.pack("!hh", self.opcode, self.block_num)


@dataclass
class Error:
    "Error (ERROR)"
    opcode = 5
    error_code: int
    message: bytes

    @classmethod
    def decode(cls, pkt):
        (opcode, error_code) = struct.unpack("!hh", pkt[0:4])
        assert opcode == cls.opcode

        pkt = pkt[4:].rstrip(b"\0")
        return cls(error_code=error_code, message=pkt)

    def encode(self):
        return struct.pack("!hh", self.opcode, self.error_code) + self.message + b"\0"

    # Special builders
    @classmethod
    def timeout_error(cls):
        return cls(error_code=0, message="Timeout occurred during transport")


_MESSAGES = {
    ReadRequest.opcode: ReadRequest,
    WriteRequest.opcode: WriteRequest,
    Data.opcode: Data,
    Acknowledgement.opcode: Acknowledgement,
    Error.opcode: Error,
}


def decode(pkt):
    raw_opcode = pkt[0:2]
    (opcode,) = struct.unpack("!h", raw_opcode)
    return _MESSAGES[opcode].decode(pkt)
