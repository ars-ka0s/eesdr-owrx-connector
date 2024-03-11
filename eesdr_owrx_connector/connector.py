import array
import argparse
import asyncio

from eesdr_tci import tci
from eesdr_tci.Listener import Listener
from eesdr_tci.tci import TciSampleType, TciCommandSendAction, TciStreamType

class Connector:
    def __init__(self):
        self.keystore = {'center_freq': 14200000, 'samp_rate': 96000}
        self.ks_handlers = {'center_freq': self.update_center, 'samp_rate': self.update_rate}
        self.demand_iq = None
        self.iq_packets = None

    async def handle_control(self, reader, writer):
        peer = writer.get_extra_info('peername')
        print(f'New control connection from {peer}')
        try:
            while True:
                data = await reader.readuntil(b'\n')
                msg = data.decode('utf-8').strip()
                if ':' not in msg:
                    continue
                k, v = msg.split(':', 2)
                if self.args.verbose:
                    print(f'Control message received {k}={v}')
                if k not in self.keystore:
                    continue
                try:
                    iv = int(v)
                    self.keystore[k] = iv
                    if self.args.verbose:
                        print('New values', self.keystore)
                    await self.ks_handlers[k]()
                except ValueError:
                    continue
        except Exception as e:
            print('Error in control connection:', e)

    async def handle_iq(self, reader, writer):
        peer = writer.get_extra_info('peername')
        print(f'New IQ connection from {peer}')
        self.demand_iq.set()
        try:
            while True:
                data = await self.iq_packets.get()
                writer.write(data)
                await writer.drain()
                self.iq_packets.task_done()
        except Exception as e:
            print('Error in IQ connection:', e)
        finally:
            self.demand_iq.clear()

    async def start_server(self, kind, port, handler):
        print(f'Starting {kind} server on {port}')
        server = await asyncio.start_server(handler, None, port)
        if self.args.verbose:
            addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
            print(f'{kind} ready on {addrs}')
        await server.serve_forever()

    async def update_rate(self):
        await self.tci_listener.send(tci.COMMANDS['IQ_SAMPLERATE'].prepare_string(TciCommandSendAction.WRITE, params=[self.keystore['samp_rate']]))

    async def update_center(self):
        await self.tci_listener.send(tci.COMMANDS['DDS'].prepare_string(TciCommandSendAction.WRITE, rx=self.args.receiver, params=[self.keystore['center_freq']]))

    async def tci_check_response(self, command, rx, subrx, param):
        if command == 'IQ_SAMPLERATE':
            assert(param == self.keystore['samp_rate'])
        elif command == 'DDS' and int(rx) == self.args.receiver:
            assert(param == self.keystore['center_freq'])

    async def tci_receive_data(self, packet):
        self.iq_packets.put_nowait(packet.data)

    async def tci_interface(self):
        self.tci_listener = Listener(f'ws://{self.args.device}')
        await self.tci_listener.start()
        await self.tci_listener.ready()

        self.tci_listener.add_param_listener('IQ_SAMPLERATE', self.tci_check_response)
        self.tci_listener.add_param_listener('DDS', self.tci_check_response)
        await self.update_rate()

        self.demand_iq = asyncio.Event()
        self.demand_iq.clear()
        self.iq_packets = asyncio.Queue()
        self.tci_listener.add_data_listener(TciStreamType.IQ_STREAM, self.tci_receive_data)

        while True:
            await self.demand_iq.wait()
            await self.tci_listener.send(tci.COMMANDS['IQ_START'].prepare_string(TciCommandSendAction.WRITE, rx=self.args.receiver))
            while self.demand_iq.is_set():
                await asyncio.sleep(0.05)
            await self.tci_listener.send(tci.COMMANDS['IQ_STOP'].prepare_string(TciCommandSendAction.WRITE, rx=self.args.receiver))
            while self.iq_packets.qsize():
                self.iq_packets.get_nowait()
                self.iq_packets.task_done()

    async def start(self):
        parser = argparse.ArgumentParser(prog='eesdr-owrx-connector', description='Connector to use the EESDR TCI Protocol to feed an OpenWebRX instance.')
        parser.add_argument('-d', '--device', default='localhost:50001', help='TCI port for radio (default: localhost:50001)')
        parser.add_argument('-r', '--receiver', choices=[0, 1], default=0, type=int, help='Which receiver to select (default: 0)')
        parser.add_argument('-p', '--port', default=44880, type=int, help='IQ data port(default: 44880)')
        parser.add_argument('-f', '--frequency', default=self.keystore['center_freq'], type=int, help='Initial center frequency (default: 14200000)')
        parser.add_argument('-s', '--samplerate', choices=[48000, 96000, 192000, 384000], default=self.keystore['samp_rate'], type=int, help='IQ sample rate (default: 78000)')
        parser.add_argument('-c', '--control', default=44881, type=int, help='Control port (default: 44881)')
        parser.add_argument('-v', '--verbose', default=False, action='store_true', help='Show debug info')
        self.args = parser.parse_args()
        self.keystore['center_freq'] = self.args.frequency
        self.keystore['samp_rate'] = self.args.samplerate

        tci = asyncio.create_task(self.tci_interface())
        ctl = asyncio.create_task(self.start_server('Control', self.args.control, self.handle_control))
        iqs = asyncio.create_task(self.start_server('IQ', self.args.port, self.handle_iq))

        await tci
        await ctl
        await iqs

def main():
    c = Connector()
    asyncio.run(c.start())

if __name__ == '__main__':
    main()