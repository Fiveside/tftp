#!/usr/bin/env python3

import asyncio
import signal
import messages
import itertools
import contextlib


class TFTPWriteAdapter:
    def __init__(self, dgram_protocol):
        self.proto = dgram_protocol
        self.block_num = 1

    async def _do_write(self, buf, loop, retries, timeout):
        # Write the data
        # Wait for ack before returning to the caller
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

    async def write(self, buf, retries=5, timeout=3.0):
        loop = asyncio.get_event_loop()

        # TFTP specifies 512 byte data messages
        for part in bytegroups(buf, 512):
            msg = messages.Data(self.block_num, part)
            raw_response = await self._do_write(msg.encode(), loop, retries, timeout)
            response = messages.decode(raw_response)

            # TODO: errors here instead of the assert.
            assert response.block_num == self.block_num

            self.block_num += 1

    def close(self):
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
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port
        self.receiver = None

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
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: TFTPFileSenderProtocol(ip, port), remote_addr=(ip, port)
    )
    writer = TFTPWriteAdapter(protocol)
    with contextlib.closing(writer):
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
