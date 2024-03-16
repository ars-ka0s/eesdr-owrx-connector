import argparse
import asyncio
import importlib.metadata
import signal
import sys

from eesdr_tci import tci
from eesdr_tci.listener import Listener
from eesdr_tci.tci import TciCommandSendAction, TciStreamType

def eprint(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr, flush=True)

class SocketServer:
    def __init__(self, kind, port, handler, verbose):
        self.kind = kind
        self.port = port
        self.handler = handler
        self.verbose = verbose

    async def serve(self):
        print(f'Starting {self.kind} server on {self.port}', flush=True)
        server = await asyncio.start_server(self.handler, None, self.port)
        if self.verbose:
            addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
            print(f'{self.kind} ready on {addrs}', flush=True)
        await server.serve_forever()

    def start(self):
        return asyncio.create_task(self.serve())

class ControlServer(SocketServer):
    def __init__(self, port, verbose, keystore, ks_handlers):
        self.keystore = keystore
        self.ks_handlers = ks_handlers
        SocketServer.__init__(self, 'Control', port, self.handle_control, verbose)

    async def handle_control(self, reader, writer):
        peer = writer.get_extra_info('peername')
        print(f'New control connection from {peer}', flush=True)
        try:
            while True:
                data = await reader.readuntil(b'\n')
                msg = data.decode('utf-8').strip()
                if self.verbose:
                    print(f'Control message received {msg}', flush=True)
                if ':' not in msg:
                    continue
                k, v = msg.split(':', 2)
                if k not in self.keystore:
                    continue
                try:
                    iv = int(v)
                    if k == 'samp_rate' and iv not in Connector.SAMPLERATES:
                        eprint('Invalid sample rate received on control channel. Ignored!')
                        continue
                    self.keystore[k] = iv
                    if self.verbose:
                        print('New values', self.keystore, flush=True)
                    self.ks_handlers[k]()
                except ValueError:
                    continue
        except Exception as e:
            print('Error in control connection:', e, flush=True)

class IqServer(SocketServer):
    def __init__(self, port, verbose, demand_iq, iq_packets):
        self.demand_iq = demand_iq
        self.iq_packets = iq_packets
        self.active_clients = 0
        SocketServer.__init__(self, 'IQ', port, self.handle_iq, verbose)

    async def handle_iq(self, _reader, writer):
        peer = writer.get_extra_info('peername')
        print(f'New IQ connection from {peer}', flush=True)
        self.active_clients += 1
        self.demand_iq.set()
        try:
            while True:
                data = await self.iq_packets.get()
                writer.write(data)
                await writer.drain()
                self.iq_packets.task_done()
        except Exception as e:
            print('Error in IQ connection:', e, flush=True)
        finally:
            self.active_clients -= 1
            if self.active_clients == 0:
                self.demand_iq.clear()

class Connector:
    SAMPLERATES = [48000, 96000, 192000, 384000]

    def __init__(self):
        self.args = None
        self.keystore = {'center_freq': 14200000, 'samp_rate': 96000}
        self.ks_handlers = {'center_freq': self.update_center, 'samp_rate': self.update_rate}
        self.tci_listener = None
        self.tci_ready = None
        self.tasks = []
        self.demand_iq = None
        self.iq_packets = None
        self.shutdown = False

    def update_rate(self):
        self.tci_listener.send_nowait(tci.COMMANDS['IQ_SAMPLERATE'].prepare_string(
            TciCommandSendAction.WRITE,
            params=[self.keystore['samp_rate']]
        ))

    def update_center(self):
        self.tci_listener.send_nowait(tci.COMMANDS['DDS'].prepare_string(
            TciCommandSendAction.WRITE,
            rx=self.args.receiver,
            params=[self.keystore['center_freq']]
        ))

    async def tci_check_response(self, command, rx, subrx, param):
        del subrx
        if command == 'IQ_SAMPLERATE' and param != self.keystore['samp_rate']:
            eprint('IQ_SAMPLERATE received that does not match desired command')
        if command == 'DDS' and rx == self.args.receiver and param != self.keystore['center_freq']:
            eprint('DDS received that does not match desired center frequency')

    async def tci_receive_data(self, packet):
        self.iq_packets.put_nowait(packet.data)

    async def tci_interface(self):
        print(f'Opening TCI connection to {self.args.device}', flush=True)
        self.tci_listener = Listener(f'ws://{self.args.device}')

        await self.tci_listener.start()
        await self.tci_listener.ready()

        self.tci_listener.add_param_listener('IQ_SAMPLERATE', self.tci_check_response)
        self.tci_listener.add_param_listener('DDS', self.tci_check_response)

        self.demand_iq = asyncio.Event()
        self.demand_iq.clear()
        self.iq_packets = asyncio.Queue()
        self.tci_listener.add_data_listener(TciStreamType.IQ_STREAM, self.tci_receive_data)
        self.tci_ready.set()

        while not self.shutdown:
            done, _ = await asyncio.wait([asyncio.create_task(self.demand_iq.wait()),
                self.tci_listener._launch_task], return_when=asyncio.FIRST_COMPLETED)
            if self.tci_listener._launch_task in done:
                print(f'TCI client closed prematurely: {self.tci_listener._launch_task.exception()}', flush=True)
                return
            try:
                [task.result() for task in done]
            except asyncio.exceptions.CancelledError:
                return

            if self.args.verbose:
                print('IQ demand start', flush=True)
            if self.args.startstop:
                self.tci_listener.send_nowait(tci.COMMANDS['START'].prepare_string(
                    TciCommandSendAction.WRITE
                ))
            self.tci_listener.send_nowait(tci.COMMANDS['RX_ENABLE'].prepare_string(
                TciCommandSendAction.WRITE,
                rx=self.args.receiver,
                params=[True]
            ))
            self.update_rate()
            self.update_center()
            self.tci_listener.send_nowait(tci.COMMANDS['IQ_START'].prepare_string(
                TciCommandSendAction.WRITE,
                rx=self.args.receiver
            ))
            while self.demand_iq.is_set():
                try:
                    await asyncio.sleep(0.05)
                    if self.tci_listener._launch_task.done():
                        print(f'TCI client closed prematurely: {self.tci_listener._launch_task.exception()}', flush=True)
                        return
                except asyncio.exceptions.CancelledError:
                    break
            if self.args.verbose:
                print('IQ demand stop', flush=True)
            self.tci_listener.send_nowait(tci.COMMANDS['IQ_STOP'].prepare_string(
                TciCommandSendAction.WRITE,
                rx=self.args.receiver
            ))
            if self.args.startstop:
                self.tci_listener.send_nowait(tci.COMMANDS['STOP'].prepare_string(
                    TciCommandSendAction.WRITE
                ))
            while self.iq_packets.qsize():
                self.iq_packets.get_nowait()
                self.iq_packets.task_done()

    def cleanup(self, *_):
        async def _cleanup():
            if self.args.verbose:
                print('Received signal, shutting down', flush=True)
            self.shutdown = True
            for task in self.tasks:
                task.cancel()

        asyncio.get_running_loop().create_task(_cleanup())

    async def start(self):
        parser = argparse.ArgumentParser(
            prog='eesdr-owrx-connector',
            description='Connector to use the EESDR TCI Protocol to feed an OpenWebRX instance.'
        )
        parser.add_argument('-d', '--device',
            default='localhost:50001',
            help='TCI port for radio (default: localhost:50001)'
        )
        parser.add_argument('-r', '--receiver',
            choices=[0, 1],
            default=0,
            type=int,
            help='Which receiver to select (default: 0)'
        )
        parser.add_argument('-p', '--port',
            default=44880,
            type=int,
            help='IQ data port(default: 44880)'
        )
        parser.add_argument('-f', '--frequency',
            default=self.keystore['center_freq'],
            type=int,
            help='Initial center frequency (default: 14200000)'
        )
        parser.add_argument('-s', '--samplerate',
            choices=Connector.SAMPLERATES,
            default=self.keystore['samp_rate'],
            type=int,
            help='IQ sample rate (default: 78000)'
        )
        parser.add_argument('-c', '--control',
            default=44881,
            type=int,
            help='Control port (default: 44881)'
        )
        parser.add_argument('-t', '--startstop',
            default=False,
            action='store_true',
            help='Start/stop the device in addition to the IQ stream'
        )
        parser.add_argument('-v', '--verbose',
            default=False,
            action='store_true',
            help='Show debug info'
        )

        print('eesdr-owrx-connector version', importlib.metadata.version('eesdr-owrx-connector'))
        print('eesdr-tci            version', importlib.metadata.version('eesdr-tci'))

        self.args = parser.parse_args()
        self.keystore['center_freq'] = self.args.frequency
        self.keystore['samp_rate'] = self.args.samplerate

        signal.signal(signal.SIGTERM, self.cleanup)
        signal.signal(signal.SIGINT,  self.cleanup)

        self.tci_ready = asyncio.Event()
        ready_task = asyncio.create_task(self.tci_ready.wait())
        tci_task = asyncio.create_task(self.tci_interface())
        done, _ = await asyncio.wait([ready_task, tci_task], return_when = asyncio.FIRST_COMPLETED)
        if tci_task in done:
            print(f'Error during TCI client start: {tci_task.exception()}', flush=True)
            return
        self.tasks += [tci_task]
        self.tasks += [ControlServer(self.args.control, self.args.verbose, self.keystore, self.ks_handlers).start()]
        self.tasks += [IqServer(self.args.port, self.args.verbose, self.demand_iq, self.iq_packets).start()]

        await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
        self.cleanup()
        await asyncio.wait(self.tasks)

        if self.args.verbose:
            print('All tasks complete', flush=True)

def main():
    c = Connector()
    asyncio.run(c.start())

if __name__ == '__main__':
    main()
