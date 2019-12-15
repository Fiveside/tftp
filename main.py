#!/usr/bin/env python3

import asyncio
import signal
import messages
import contextlib


class TFTPStepwiseRetryAdapter:
    def __init__(self, dgram_protocol, retries=5, timeout=3.0):
        self.proto = dgram_protocol
        self.retries = retries
        self.timeout = timeout

    async def send(self, buf):
        """Send a packet and wait for a response."""
        # Write the data
        # Wait for ack before returning to the caller
        loop = asyncio.get_event_loop()
        for _ in range(self.retries):
            try:
                fut = loop.create_future()
                self.proto.set_receiver(fut)
                self.proto.transport.sendto(buf)
                return await asyncio.wait_for(fut, timeout=self.timeout)
            except asyncio.TimeoutError as timeout_error:
                pass

        # Failed.  Write error message and throw a communication error
        msg = messages.Error.timeout_error()
        self.proto.send(msg.encode())
        self.close()

        # TODO: Better exception
        raise timeout_error

    def emit(self, buf):
        """Send a packet and do not wait for a response."""
        self.proto.transport.sendto(buf)

    async def close(self):
        self.proto.close()


class TFTPBufferedDataSender:
    def __init__(self, sender):
        self.proto = sender
        self.block_num = 1
        self.send_buffer = bytes()

    async def send(self, buf):
        self.send_buffer.write(buf)
        while len(self.send_buffer) > messages.Data.max_data_size:
            block = self.send_buffer[0 : messages.Data.max_data_size]
            self.send_buffer = self.send_buffer[messages.Data.max_data_size :]
            await self._send_block(block)

    async def _send_block(self, buf):
        while True:
            msg = messages.Data(self.block_num, buf)
            raw_response = await self.proto.send(msg.encode())
            res = messages.decode(raw_response)
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

    async def close(self):
        # Send the last of the bits inside the send buffer, or an empty data
        # packet if the buffer is empty.
        await self._send_block(self.send_buffer)
        self.send_buffer = bytes()

        await self.proto.close()


class TFTPDataReceiver:
    def __init__(self, sender):
        self.proto = sender
        self.block_num = 0
        self.eof_reached = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.eof_reached:
            raise StopAsyncIteration
        return await self.read()

    async def read(self):
        if self.eof_reached:
            return b""

        msg = messages.Acknowledgement(self.block_num)
        raw = await self.proto.send(msg.encode())
        res = messages.decode(raw)

        assert res.block_num == self.block_num + 1
        self.block_num = res.block_num

        print(f"Read {len(res.data)} bytes of data from client")

        if not res.is_full_block:
            await self.finalize()

        return res.data

    async def finalize(self):
        print("Read EOF detected, finalizing.")
        self.eof_reached = True
        msg = messages.Acknowledgement(self.block_num)
        self.proto.emit(msg.encode())

    async def close(self):
        await self.proto.close()


@contextlib.asynccontextmanager
async def aclosing(thing):
    yield thing
    await thing.close()


class TFTPTransferProtocol(asyncio.DatagramProtocol):
    def __init__(self, addr, port, closer):
        self.addr = addr
        self.port = port
        self.receiver = None
        self.closer = closer

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
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
        self.closer.set_exception(exc)

    def set_receiver(self, fut):
        self.receiver = fut

    def close(self):
        if not self.closer.done():
            self.closer.set_result(None)


async def send_file_to_client(rrq, ip, port):
    loop = asyncio.get_event_loop()
    closer = loop.create_future()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: TFTPTransferProtocol(ip, port, closer), remote_addr=(ip, port)
    )
    with contextlib.closing(transport), contextlib.closing(protocol):
        sender = TFTPBufferedDataSender(TFTPStepwiseRetryAdapter(protocol))
        async with aclosing(sender):
            with open(rrq.filename, "rb") as fobj:
                for buf in fobj:
                    await sender.send(buf)

    # Make sure protocol disposition was graceful
    await closer


async def read_file_from_client(wrq, ip, port):
    loop = asyncio.get_event_loop()
    closer = loop.create_future()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: TFTPTransferProtocol(ip, port, closer), remote_addr=(ip, port)
    )
    with contextlib.closing(transport), contextlib.closing(protocol):
        reader = TFTPDataReceiver(TFTPStepwiseRetryAdapter(protocol))
        async with aclosing(reader):
            with open(wrq.filename, "wb") as fobj:
                async for buf in reader:
                    fobj.write(buf)

    # Make sure protocol disposition was graceful
    await closer


class TFTPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.jobs = list()
        self.job_task = asyncio.ensure_future(self.periodically_prune_jobs())

    async def periodically_prune_jobs(self):
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                self.jobs = self.running_jobs()
                await asyncio.sleep(5)

    def running_jobs(self):
        return [x for x in self.jobs if not x.done()]

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        print(f"Received {repr(data)} from {addr}")
        msg = messages.decode(data)
        print(f"Parsed as a {repr(msg)}")
        if msg.opcode == messages.ReadRequest.opcode:
            job = send_file_to_client(msg, *addr)
        elif msg.opcode == messages.WriteRequest.opcode:
            job = read_file_from_client(msg, *addr)
        else:
            print(f"Message not appropriate for server port")
            return

        self.jobs.append(asyncio.ensure_future(job))

    def connection_lost(self, exc):
        if exc is None:
            return
        print(f"Lost connection: {exc}")

    def error_received(self, exc):
        print(f"Received error: {exc}")

    async def close(self):
        self.job_task.cancel()
        await asyncio.gather(*self.running_jobs())


async def main():
    print("Starting UDP server")

    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        TFTPServerProtocol, local_addr=("127.0.0.1", 9999)
    )

    signal_finisher = loop.create_future()

    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, lambda: signal_finisher.set_result(None))

    await signal_finisher
    transport.close()
    await protocol.close()


asyncio.run(main())
