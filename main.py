#!/usr/bin/env python3

import asyncio
import signal
import messages
import itertools
import contextlib


class TFTPWriteAdapter:
    # TFTP specifies 512 byte data messages
    BLOCK_SIZE = 512

    _UNSPECIFIED = object()

    def __init__(self, dgram_protocol, retries=5, timeout=3.0):
        self.proto = dgram_protocol
        self.block_num = 1
        self.last_block = bytes()
        self.retries = retries
        self.timeout = timeout

    async def _do_write(self, buf, retries, timeout):
        # Write the data
        # Wait for ack before returning to the caller
        loop = asyncio.get_event_loop()
        for _ in range(retries):
            try:
                fut = loop.create_future()
                self.proto.set_receiver(fut)
                self.proto.transport.sendto(buf)
                return await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.TimeoutError:
                pass

        # Failed.  Write error message and throw a communication error
        msg = messages.Error.timeout_error()
        self.proto.send(msg.encode())
        self.close()

        # TODO: Better exception
        raise Exception("Timeout occurred")

    async def _write_block(self, buf, retries=_UNSPECIFIED, timeout=_UNSPECIFIED):
        retries = self.retries if retries is self._UNSPECIFIED else retries
        timeout = self.timeout if timeout is self._UNSPECIFIED else timeout
        msg = messages.Data(self.block_num, buf)
        raw_response = await self._do_write(msg.encode(), retries, timeout)
        response = messages.decode(raw_response)

        return response

    async def write(self, buf, retries=_UNSPECIFIED, timeout=_UNSPECIFIED):

        for part in bytegroups(buf, self.BLOCK_SIZE):
            while True:
                res = await self._write_block(buf, retries, timeout)
                if res.block_num == self.block_num - 1:
                    # Received previous ack instead of the expected one.
                    continue
                elif res.block_num == self.block_num:
                    # Received expected ack.
                    break
                else:
                    # Received unexpected ack
                    raise Exception("Invalid packet received")

            self.block_num += 1
            self.last_block = buf

    async def close(self, retries=_UNSPECIFIED, timeout=_UNSPECIFIED):
        # If the last block sent was a full block, then send an empty one
        # indicating file completion
        if len(self.last_block) == self.BLOCK_SIZE:
            await self.write(bytes(), retries, timeout)

        self.proto.close()


def bytegroups(iterable, size):
    it = iter(iterable)
    while True:
        piece = bytes(itertools.islice(it, size))
        if len(piece) > 0:
            yield piece
        else:
            return


class TFTPFileSenderProtocol(asyncio.DatagramProtocol):
    def __init__(self, addr, port, closer):
        self.addr = addr
        self.port = port
        self.receiver = None
        self.closer = closer

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # if addr != (self.addr, self.port):
        #     # Random inbound packet not from our tftp peer
        #     return
        if self.receiver is None or self.receiver.done():
            print(
                "Error: Received a message, but no receiver is available.  Discarding."
            )
            print(f"Discarded message: {repr(data)}")
            return
        self.receiver.set_result(data)

    def connection_lost(self, exc):
        if exc is None:
            return
        print(f"Lost connection: {exc}")

    def error_received(self, exc):
        print(f"Error received: {exc}")

    def set_receiver(self, fut):
        self.receiver = fut

    def close(self):
        self.transport.close()


class TFTPFileReceiverProtocol(asyncio.DatagramProtocol):
    pass


async def create_write_connection(rrq, protocol, ip, port):
    loop = asyncio.get_event_loop()
    closer = loop.create_future()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: TFTPFileSenderProtocol(ip, port, closer), remote_addr=(ip, port)
    )
    writer = TFTPWriteAdapter(protocol)
    async with contextlib.closing(writer):
        with open(rrq.filename, "rb") as fobj:
            await writer.write(fobj.read())


class TFTPServerProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        print(f"Received {repr(data)} from {addr}")
        msg = messages.decode(data)
        print(f"Parsed as a {repr(msg)}")
        if msg.opcode == messages.ReadRequest.opcode:
            asyncio.ensure_future(create_write_connection(self, msg, *addr))
            # asyncio.get_event_loop().call_soon(
            #     create_write_connection, self, msg, *addr
            # )

    def connection_lost(self, exc):
        if exc is None:
            return
        print(f"Lost connection: {exc}")

    def error_received(self, exc):
        print(f"Received error: {exc}")


async def main():
    print("Starting UDP server")

    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        TFTPServerProtocol, local_addr=("127.0.0.1", 9999)
    )

    finisher = loop.create_future()

    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, lambda: finisher.set_result(None))

    try:
        await finisher
    finally:
        transport.close()


asyncio.run(main())
