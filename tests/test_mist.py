import contextlib
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import zlib

import dashboard
import logger
from sender import SenderService, BLOCK_SIZE, validate_id


def wait_for(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.01)
    raise AssertionError('Timed out waiting for state')


class MistTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.payload = bytes(range(256)) * 1025
        (self.root/'sample.bin').write_bytes(self.payload)
        logger.configure(self.root/'logs', json_logs=True, console=False)
        self.services = []

    def tearDown(self):
        for service in self.services:
            service.stop()
        # Detach file handlers before deleting their temporary directory.
        with logger._lock:
            for handler in logger._writer.handlers[:]:
                handler.close()
                logger._writer.removeHandler(handler)
        self.temp.cleanup()

    def service(self, **kwargs):
        service = SenderService('171', self.root, **kwargs)
        self.services.append(service)
        return service

    def assign(self, service, message=None, fragmented=False):
        message = message or f'ASSIGN:{service.sender_id}:sample.bin:7:{len(self.payload)-3}:127.0.0.1'
        with socket.create_connection(('127.0.0.1', 5000+int(service.sender_id)), timeout=2) as sock:
            if fragmented:
                for c in message.encode():
                    sock.sendall(bytes([c]))
                    time.sleep(.001)
            else:
                sock.sendall(message.encode())

    def mock_receive(self, service, **kwargs):
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1',5000))
        listener.listen()
        listener.settimeout(5)
        received = []
        def collect():
            try:
                conn,_ = listener.accept()
                with conn:
                    conn.settimeout(5)
                    parts=[]
                    while True:
                        block=conn.recv(8192)
                        if not block: break
                        parts.append(block)
                    received.append(b''.join(parts))
            finally:
                listener.close()
        thread=threading.Thread(target=collect)
        thread.start()
        try:
            self.assign(service, **kwargs)
            wait_for(lambda: service.snapshot()['history'])
        finally:
            thread.join(6)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(received),1)
        return received[0]

    def test_logger_legacy_thread_safety_rotation(self):
        logger.configure(self.root/'logs', True, max_bytes=512, backups=2, console=False)
        threads=[threading.Thread(target=lambda n=n: [logger.log(f'worker {n} event {i}', sender_id=n) for i in range(40)]) for n in range(6)]
        for t in threads:t.start()
        for t in threads:t.join()
        events=logger.snapshot()
        ids=[e['id'] for e in events]
        self.assertEqual(len(ids),len(set(ids)))
        self.assertEqual(ids,sorted(ids))
        self.assertTrue((self.root/'logs/mist.log.1').exists())
        self.assertTrue((self.root/'logs/mist.jsonl.1').exists())
        for path in (self.root/'logs').glob('mist.jsonl*'):
            for line in path.read_text().splitlines():json.loads(line)
        events[-1]['message']='changed'
        self.assertNotEqual(logger.snapshot()[-1]['message'],'changed')
        with contextlib.redirect_stdout(io.StringIO()) as out:logger.print_audit_log()
        self.assertIn('AUDIT LOG',out.getvalue())
        for i in range(2100):logger.log(str(i))
        self.assertEqual(len(logger.snapshot()),2000)
        self.assertEqual(len(logger.log_history),2000)

    def test_paths_and_ids(self):
        service=self.service()
        (self.root/'link').symlink_to('/etc/passwd')
        os.mkfifo(self.root/'pipe')
        for name in ('../sample.bin','/etc/passwd','a:b','x\\y','\n','.', 'link', 'pipe'):
            with self.subTest(name=name), self.assertRaises((ValueError,OSError)):service.open_file(name)
        self.assertEqual([f['name'] for f in service.files()],['sample.bin'])
        for sid in ('-1','60536','1.5','x','١'):
            with self.assertRaises(ValueError):validate_id(sid)
        self.assertEqual(validate_id('0'),'0')

    def test_normal_fragmented_and_corrupt_wire(self):
        for corrupt in (False,True):
            with self.subTest(corrupt=corrupt):
                service=self.service(corrupt=corrupt)
                service.start()
                message=self.mock_receive(service,fragmented=True)
                header=message.split(b':',6)
                original=self.payload[7:-3]
                self.assertEqual(header[:5],[b'CHUNK',b'171',b'sample.bin',b'7',str(len(self.payload)-3).encode()])
                self.assertEqual(header[5],f'{zlib.crc32(original):08x}'.encode())
                expected=bytes(b^255 for b in original[:10])+original[10:] if corrupt else original
                self.assertEqual(header[6],expected)
                record=service.snapshot()['history'][-1]
                self.assertEqual(record['bytes_sent'],len(original))
                self.assertEqual(record['wire_bytes_sent'],len(message))
                self.assertEqual(record['status'],'sender complete')
                self.assertFalse(record['receiver_confirmed'])
                service.stop()

    def test_invalid_ranges_and_malformed_assignments(self):
        service=self.service(timeout=.3)
        service.start()
        messages=['garbage', 'ASSIGN:172:sample.bin:0:1:127.0.0.1',
                  'ASSIGN:171:../sample.bin:0:1:127.0.0.1',
                  'ASSIGN:171:sample.bin:-1:2:127.0.0.1',
                  'ASSIGN:171:sample.bin:8:2:127.0.0.1',
                  'ASSIGN:171:sample.bin:0:999999999:127.0.0.1',
                  'ASSIGN:171:sample.bin:0:1:999.0.0.1', 'x'*4097]
        for count,message in enumerate(messages,1):
            self.assign(service,message)
            wait_for(lambda:len(service.snapshot()['history'])==count)
            self.assertEqual(service.snapshot()['history'][-1]['status'],'failed')

    def test_bounded_reads(self):
        service=self.service()
        class BoundedReader(io.BytesIO):
            def read(self,n=-1):
                if not 0<=n<=BLOCK_SIZE:raise AssertionError(f'Unbounded read: {n}')
                return super().read(n)
        self.assertEqual(b''.join(service._blocks(BoundedReader(self.payload),len(self.payload))),self.payload)
        with self.assertRaises(OSError):list(service._blocks(BoundedReader(b'x'),2))

    def test_retry_limits_and_no_partial_replay(self):
        service=self.service(retries=3,backoff=0)
        record={"id":"mock-retry"}
        class FailedSocket:
            attempts=0
            def settimeout(self,t):pass
            def connect(self,addr):
                self.__class__.attempts+=1
                raise ConnectionRefusedError('mock refusal')
            def close(self):pass
        with patch('sender.socket.socket',FailedSocket):
            with self.assertRaises(ConnectionRefusedError):service._connect('127.0.0.1',record)
        self.assertEqual(FailedSocket.attempts,3)
        self.assertFalse(service.sockets)
        service.start()
        class PartialSocket:
            calls=0
            def sendall(self,block):
                self.calls+=1
                if self.calls==2:raise OSError('mock partial payload failure')
            def close(self):pass
        partial=PartialSocket()
        with patch.object(service,'_connect',return_value=partial) as connect:
            self.assign(service)
            wait_for(lambda:service.snapshot()['history'])
            self.assertEqual(connect.call_count,1)
        r=service.snapshot()['history'][0]
        self.assertEqual(r['status'],'failed')
        self.assertEqual(r['bytes_sent'],0)

    def test_lifecycle_discovery_and_capacity(self):
        service=self.service(workers=1,timeout=.4)
        service.start()
        with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as udp:
            udp.settimeout(2)
            udp.sendto(b'REQUEST:sample.bin',('127.0.0.1',5000))
            self.assertEqual(udp.recv(4096),f'ACK:171:sample.bin:{len(self.payload)}'.encode())
        first=socket.create_connection(('127.0.0.1',5171))
        wait_for(lambda:len(service.snapshot()['active'])==1)
        with socket.create_connection(('127.0.0.1',5171)) as second:
            second.settimeout(1)
            self.assertEqual(second.recv(1),b'')
        start=time.monotonic()
        service.stop()
        first.close()
        self.assertLess(time.monotonic()-start,2)
        self.assertFalse(service.sockets)
        self.assertFalse(any(t.is_alive() for t in service.threads))
        self.assertEqual(service.state,'stopped')
        service.stop()
        replacement=self.service()
        replacement.start()

    def test_eof_deadline(self):
        service=self.service(timeout=.1)
        a,b=socket.socketpair()
        try:
            a.sendall(b'ASSIGN:171:sample.bin:0:1:127.0.0.1')
            with self.assertRaises(TimeoutError):service.read_assignment(b)
        finally:a.close();b.close()

    def test_dashboard_controls_security_exports_real_events(self):
        app=dashboard.Console(self.root)
        server=dashboard.Server(('127.0.0.1',0),app)
        thread=threading.Thread(target=server.serve_forever)
        thread.start()
        conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
        def request(method,path,body=None,headers=None):
            conn.request(method,path,json.dumps(body) if body else None,headers or {})
            response=conn.getresponse()
            return response.status,response.read()
        try:
            self.assertEqual(request('GET','/')[0],200)
            self.assertEqual(request('GET','/favicon.ico')[0],204)
            status,data=request('GET','/api/state')
            self.assertEqual(json.loads(data)['history'],[])
            self.assertEqual(json.loads(data)['state'],'stopped')
            body={'action':'start','sender_id':'171','input_dir':str(self.root),'corrupt':False}
            self.assertEqual(request('POST','/api/control',body)[0],403)
            headers={'Content-Type':'application/json','X-MIST-Token':app.token,'Origin':'https://evil.example'}
            self.assertEqual(request('POST','/api/control',body,headers)[0],403)
            headers.pop('Origin')
            self.assertEqual(request('GET','/api/state',headers={'Host':'evil.example'})[0],403)
            self.assertEqual(request('POST','/api/control',body,headers)[0],200)
            self.mock_receive(app.service)
            state=json.loads(request('GET','/api/state')[1])
            self.assertEqual(state['history'][0]['status'],'sender complete')
            self.assertTrue(any(e.get('transfer')==state['history'][0]['id'] for e in state['events']))
            for path in ('events.json','events.txt','transfers.json','transfers.csv'):
                status,data=request('GET','/api/export/'+path)
                self.assertEqual(status,200)
                self.assertTrue(data)
            self.assertEqual(request('POST','/api/control',{'action':'stop'},headers)[0],200)
        finally:
            conn.close();app.service.stop();server.shutdown();server.server_close();thread.join()

    def test_connection_recovery_before_transmission(self):
        service=self.service(retries=3,backoff=.001)
        class RecoveringSocket:
            attempts=0
            def settimeout(self,t):pass
            def connect(self,addr):
                self.__class__.attempts+=1
                if self.attempts<3:raise ConnectionRefusedError('listener not ready')
            def close(self):pass
        with patch('sender.socket.socket',RecoveringSocket):
            sock=service._connect('127.0.0.1',{'id':'recovery'})
            self.assertEqual(RecoveringSocket.attempts,3)
            service._close(sock)

    def test_bind_failure_cleans_up(self):
        with socket.socket() as occupied:
            occupied.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            occupied.bind(('',5171))
            occupied.listen()
            service=self.service()
            with self.assertRaises(OSError):service.start()
            self.assertEqual(service.state,'error')
            self.assertFalse(service.sockets)
            self.assertIsNone(service.root_fd)

    def test_actual_receiver_order_limitation(self):
        # Use untouched receiver functions; this intentionally demonstrates its bug.
        import importlib.util
        path=Path('receiver.py').resolve()
        spec=importlib.util.spec_from_file_location('mist_receiver_for_test',path)
        receiver=importlib.util.module_from_spec(spec)
        previous=Path.cwd()
        try:
            os.chdir(self.root)
            spec.loader.exec_module(receiver)
            assignments=receiver.assign_chunks([
                {'id':2,'ip':'127.0.0.1','filename':'order.bin','file_size':4},
                {'id':1,'ip':'127.0.0.1','filename':'order.bin','file_size':4}])
            self.assertEqual([a['start'] for a in assignments],['0','2'])
            receiver.chunks={2:b'AB',1:b'CD'}
            receiver.assemble_file('order.bin')
            self.assertEqual(Path('output/order.bin').read_bytes(),b'CDAB')
            self.assertNotEqual(Path('output/order.bin').read_bytes(),b'ABCD')
        finally:os.chdir(previous)

    def test_protected_sources(self):
        for line in Path('tests/protected.sha256').read_text().splitlines():
            expected,name=line.split()
            self.assertEqual(hashlib.sha256(Path(name).read_bytes()).hexdigest(),expected)


if __name__=='__main__':unittest.main()
