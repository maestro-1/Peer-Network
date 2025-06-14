"""
Peer Network version 1
"""

from itertools import chain
import random
import socket
from threading import Thread, Lock
import queue
import threading
from time import sleep

MULTICAST_ADDR = '224.0.0.255'
MULTICAST_PORT = 16000


def get_machine_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        s.connect(('10.254.254.254', 1))
        ip = s.getsockname()[0]
    except OSError:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


class PeerNotifierHandler:
    def __init__(self, host: str) -> None:
        self.host = host
        self.peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # pipeline to propagate information across server and client
        self.notifs = queue.Queue(10)

        self.configure_multicast()
        self.host_listen_thread = None
        self.start_listen()

        self.peer_listen_thread: Thread | None = None

    def start_listen(self):
        def listen_in():
            while True:
                data, sender = self.peer.recvfrom(1024)
                sender_addr, sender_port = sender
                
                if sender_addr == self.host and data == b'stop':
                    print('Received stop signal')
                    self.notifs.put(('stop',))
                    return
                else:
                    print(f'Ignoring broadcast message from {sender_addr}')
                
                print(sender_addr, 'multicast:', data)
                self.notifs.put((data.decode(), sender_addr, sender_port))

        self.host_listen_thread = Thread(target=listen_in)
        self.host_listen_thread.start() 

    def configure_multicast(self):
        """
        Configure the socket to work with the UDP
        """
        self.peer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.peer.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

        # add a new member to the multicast on peer join.
        self.peer.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(MULTICAST_ADDR) + socket.inet_aton("0.0.0.0"))
        self.peer.bind(('0.0.0.0', MULTICAST_PORT))

    def broadcast(self, message):
        """
        Advertise free ports available for clients to connect to
        """
        self.peer.sendto(message.encode(), (MULTICAST_ADDR, MULTICAST_PORT))
        print('Multicasting:', message)

    def stop_listen(self):
        """
        Peer host sends a stop message. The address of the host is important here because the peer
        identifies the host as itself and begins shutdown
        """
        # send a stop response to the network which drops this node from the network. 
        self.peer.sendto('stop'.encode(), (self.host, MULTICAST_PORT))
        print('Sending signal to stop local listening to multicast')
        self.host_listen_thread.join()

    def close(self):
        if self.host_listen_thread.is_alive():
            self.stop_listen()
        self.peer.close()

    
class Peers:
    def __init__(self, num_in = 5, num_out = 5) -> None:
        self.local_ip = get_machine_ip()

        self.peers = PeerNotifierHandler(self.local_ip)
        self.peer_aware = True

        # Max connections
        self.max_served = num_in
        self.max_connect = num_out


        # Thread locks
        self.serv_conn_lock = Lock()
        self.client_conn_lock = Lock()
        self.ports_lock = Lock()
    
        # state
        self.server_manager_signal = threading.Semaphore(1)
        self.stop_advertiser = False
        self.stop_serv = False

        # Thread containers
        self.peer_notifier_thread = None
        self.serv_manager_thread = None
        self.propogate_data_thread = None
        self.advertise_thread = None

        self.available_ports = dict()
        # ports used by server and client for connection
        self.used_ports = dict()

        # Store IPs and connected sockets. IP being the key
        self.served_connections = dict()
        self.client_connections = dict()

        # Queues for event handling on local node and sending to other nodes
        self.data_propogate_queue = queue.Queue(num_in + num_out)
        self.local_data_queue = queue.Queue((num_in + num_out)*2)
        self.start()
    
    def start(self):
        # Start handlers
        self._broadcasts_from_peer_servers()
        self._manage_servers()
        self._manage_advertisers()
        self._propogate_data()
    
    def _generate_port(self):
        # Generate range from just above MULTICAST_PORT to maximum port range
        random_port = random.randint(MULTICAST_PORT + 1, 65535)
        self.ports_lock.acquire()
        while random_port in self.available_ports or random_port in self.used_ports:
            random_port = random.randint(MULTICAST_PORT + 1, 65535)
        self.ports_lock.release()
        return random_port
    
    def _broadcasts_from_peer_servers(self):
        def _make_handler():
            client_threads = []
            while True:
                new_notification = self.peers.notifs.get()
                print('New notification:', new_notification)
                
                # server host is turning off
                if new_notification == ('stop',):
                    break

                elif self.peer_aware:
                    command, send_addr, _ = new_notification
                    command_op, command_args = command.split(' ')
                    if command_op == 'available':
                        if not type(command_args) == str: 
                            continue
                        
                        # connect to peer server and save or skip
                        self.serv_conn_lock.acquire()
                        if send_addr in self.served_connections.keys(): 
                            continue
                        self.serv_conn_lock.release()
                        
                        # save client connection
                        self.client_conn_lock.acquire()
                        if send_addr in self.client_connections.keys(): 
                            continue
                        self.client_conn_lock.release()

                        t = threading.Thread(target=self._generate_client, args=(send_addr, int(command_args)))
                        t.start()
                        client_threads.append(t)

            print('Joining client connection threads')
            # start client connections on multiple multiple threads
            for t in client_threads:
                if not t.is_alive(): 
                    continue
                t.join()

        self.peer_notifier_thread = threading.Thread(target=_make_handler)
        self.peer_notifier_thread.start()

    def _manage_advertisers(self):
        def _advertise_servers():
            while True:
                if self.stop_advertiser: return
                self.ports_lock.acquire()
                # print('Stop advertiser', self.stop_advertiser)
                for port in self.available_ports.keys():
                    try:
                        self.peers.broadcast(f'available {port}')
                    except OSError as e:
                        if e.args[0] == 9:
                            self.available_ports.pop(port)
                            break
                        print('Unable to advertise server:', e)
                self.ports_lock.release()
                sleep(15)
        
        self.advertise_thread = threading.Thread(target=_advertise_servers)
        self.advertise_thread.start()

    def _generate_server(self, available_port):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(('0.0.0.0', available_port))
        server_sock.listen()

        self.ports_lock.acquire()

        while True:
            self.available_ports[available_port] = server_sock
            self.ports_lock.release()
            try:
                client_conn, client_full_addr = server_sock.accept()
                client_addr, _ = client_full_addr
            except OSError as e:
                if e.args[0] == 22: 
                    break
                print('Error accepting client:', e)
                server_sock.close()
                break
            # self.server_manager_signal.release()

            self.ports_lock.acquire()
            self.available_ports.pop(available_port)
            self.used_ports[available_port] = server_sock
            self.ports_lock.release()

            self.serv_conn_lock.acquire()
            self.served_connections[client_addr] = client_conn
            self.serv_conn_lock.release()

            with client_conn:
                print(client_addr, 'connected to server port', available_port)
                while True:
                    data = client_conn.recvfrom(1024)
                    print(f'data is: {data}')
                    if not data: 
                        break
                    
                    if data[0] == b'':
                        print('Received empty bitstring from client peer, closing connection.')
                        break
                    print((client_addr, data))
                    self.data_propogate_queue.put((client_addr, data))
            
            # terminate tcp connection with client and pop out used ports
            self.serv_conn_lock.acquire()
            self.served_connections.pop(client_addr)
            self.serv_conn_lock.release()
            self.ports_lock.acquire()
            self.used_ports.pop(available_port)
        
        print('Server stopped,', available_port)

    def _manage_servers(self):
        def _manage_threads():
            server_threads = []
            while len(self.available_ports) + len(self.served_connections) < self.max_served:
                self.server_manager_signal.acquire()
                if self.stop_serv: 
                    break

                new_port = self._generate_port()
                t = threading.Thread(target=self._generate_server, args=(new_port,))
                t.start()
                server_threads.append(t)
                print(server_threads)
            print('Joining server connection threads')
            for t in server_threads:
                print(server_threads)
                if not t.is_alive(): 
                    continue
                t.join()

        self.serv_manager_thread = threading.Thread(target=_manage_threads)
        self.serv_manager_thread.start()

    def _generate_client(self, addr: str, port:int):
        """
        Where peer client connection is created and data from client is received
        """
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        with client_sock:
            try:
                # acquire lock and attach port to client socket
                self.ports_lock.acquire()
                self.used_ports[port] = client_sock
                self.ports_lock.release()

                # connect socket and keep track of connection
                client_sock.connect((addr, port))
                print('Client connected to', addr, 'port', port)
                
                self.client_conn_lock.acquire()
                self.client_connections[addr] = client_sock
                self.client_conn_lock.release()
                
                
                while True:
                    data = client_sock.recvfrom(1024)
                    if not data: break
                    if data[0] == b'':
                        print('closing connection.')
                        break
                    # self.data_propogate_queue.put((addr, data))

            except ConnectionRefusedError as e:
                print('Error:', e, ', aborting client connection')
            finally:
                # close client
                print('Start client cleanup')
                # remove client connection
                self.client_conn_lock.acquire()
                self.client_connections.pop(addr)
                self.client_conn_lock.release()
                
                # make used port available again
                self.ports_lock.acquire()
                self.used_ports.pop(port)
                self.ports_lock.release()
                print('Client Stopped')

    def data_spread(self, data, origin=None):
        for addr, conn in chain(self.served_connections.items(), self.client_connections.items()):
            if addr == origin: continue
            try:
                conn.send(data.encode())
            except BrokenPipeError:
                pass
    
    def _propogate_data(self):
        def _queue_listener():
            while True:
                origin_addr, new_data = self.data_propogate_queue.get()
                if origin_addr is None and new_data == 'stop': 
                    break
                if new_data[0] == b'': 
                    continue
                self.data_spread(new_data, origin_addr)
                self.local_data_queue.put(new_data)
        
        self.propogate_data_thread = threading.Thread(target=_queue_listener)
        self.propogate_data_thread.start()
    
    def _stop_data_propogator(self):
        print('Send signal to stop data propogation')
        self.data_propogate_queue.put((None, 'stop'))
        print('Start join data propogation thread')
        self.propogate_data_thread.join()


    def _stop_peer_notif(self):
        print('Close multicast socket')
        self.peers.close()
        print(self.peers.notifs.qsize())
        print('Start join peer notifier thread')
        self.peer_notifier_thread.join(3)
        if self.peer_notifier_thread.is_alive():
            print('Peer notifier thread still alive after 3 seconds.')

    def _stop_server_manager(self):
        print('Set server stop')
        self.stop_serv = True
        print('Set advertiser stop')
        self.stop_advertiser = True
        self.peer_aware = False
        self.server_manager_signal.release()
        print('Start join advertise thread')
        self.advertise_thread.join()
        print('Start join server manager thread')
        self.serv_manager_thread.join()

    def _shutdown_sockets(self):
        self.ports_lock.acquire()
        print('Connecting to available ports for shutdown')
        for port in self.available_ports:
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((self.local_ip, port))
        for conn in chain(self.available_ports.values(), self.used_ports.values()):
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError as e:
                if e.args[0] == 107: continue
            conn.close()
        self.ports_lock.release()

    def close(self):
        self._shutdown_sockets()
        self._stop_server_manager()
        self._stop_peer_notif()
        self._stop_data_propogator()


if __name__ == "__main__":
    local_node = Peers(5, 5)
    _msg_getter= print(local_node.local_data_queue.get())
    msg_t = threading.Thread(target=_msg_getter, daemon=True)
    msg_t.start()
    while True:
        local_node.data_spread(input('What data would you like to spread?'))