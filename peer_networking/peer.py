from itertools import chain
import queue
import socket
import threading
from time import sleep

from client import ClientPeer
from stage_manager import ConnectionState, get_machine_ip
from server import Advertiser, PeerServer


# the problem of the threads from other classes being closed here results in coupling
# TODO: decide where threads used in peer are created and terminated. Within or outside peer
class Peer:
    def __init__(self, max_client_conn: int, max_server_conn: int) -> None:
        self.local_ip = get_machine_ip()
        self.advertise_to_peers = Advertiser(self.local_ip)

        # Max connections
        self.max_client_conn = max_client_conn
        self.max_server_conn = max_server_conn

        # a maximum of 2 messages per connection
        self.data_to_propagate = queue.Queue((max_client_conn + max_server_conn) * 2)

        # connection state of servers, clients and ports
        self.connection_state = ConnectionState()
        self.aware_of_peers = True
        self.stop_serv = False
        self.stop_advertiser = False

        # Thread containers
        self.peer_notifier_thread = None
        self.serv_manager_thread = None
        self.propogate_data_thread = None
        self.advertise_thread = None
        

        self.initialize()
    
    
    def initialize(self):
        self.start_advertiser()
        self.start_server_manager()
        self.start_client_manager()

    def manage_advertising(self):
        while True:
            if self.stop_advertiser:
                return
            
            self.connection_state.ports_lock.acquire()
            for port in self.connection_state.available_ports.keys():
                try:
                    self.advertise_to_peers.cast_for_connection(f'available {port}')
                except OSError as e:
                    if e.args[0] == 9:
                        self.connection_state.available_ports.pop(port)
                        break
                    print('Unable to advertise server:', e)
            
            # self.connection_state.ports_lock.release()
            sleep(10)

    def start_advertiser(self):
        print("starting advert")
        self.advertise_thread = threading.Thread(target=self.manage_advertising)
        self.advertise_thread.start()

    def manager_servers(self):
        server_threads = []

        while len(self.connection_state.available_ports) + len(self.connection_state.served_connections) < self.max_server_conn:
            if self.stop_serv: 
                break
            new_port = self.connection_state.generate_port()
            new_server = PeerServer(new_port, self.data_to_propagate)
            
            sever_thread = new_server.server_thread()
            server_threads.append(sever_thread)
            
        # server thread clean up
        for t in server_threads:
            print(server_threads)
            if not t.is_alive(): 
                continue
            t.join()

    def start_server_manager(self):
        print("starting servers")
        self.serv_manager_thread = threading.Thread(target=self.manager_servers)
        self.serv_manager_thread.start()

    def manage_clients(self):
        """
        Responds to broadcasts from other peers on the network and connect
        """
        client_threads = []

        while True:
            command, addr, _ = self.advertise_to_peers.notifs.get()
            if command == "stop":
                break
            
            if self.aware_of_peers:
                command_op, command_args = command.split(" ")
                if command_op != 'available':
                    continue

                if command_op == 'available' and not isinstance(command_args, str):
                    continue
       
                client = ClientPeer(self.data_to_propagate, self.connection_state)
               
                # # confirm that address is not connected as a client or server before connecting
                if self.connection_state.client_exists(addr) or self.connection_state.server_exists(addr):
                    continue

                # create new client connection if there is space on the connection pool
                if self.max_client_conn > self.connection_state.client_conn_lock:
                    generated_client_thread = client.client_thread()
                    client_threads.append(generated_client_thread)
        
        # thread clean up.
        for t in client_threads:
            if not t.is_alive(): 
                continue
            t.join()

    def start_client_manager(self):
        print("starting client")
        self.peer_notifier_thread = threading.Thread(target=self.manage_clients)
        self.peer_notifier_thread.start()

    def data_spread(self, data, origin=None):
        for addr, conn in chain(self.connection_state.served_connections.items(), self.connection_state.client_connections.items()):
            if addr == origin: 
                continue
            try:
                conn.send(data.encode())
            except BrokenPipeError:
                pass

    def queue_listener(self):
        while True:
            origin_addr, new_data = self.data_to_propagate.get()
            if origin_addr is self.local_ip and new_data == 'stop': 
                break

            if new_data[0] == b'': 
                continue

            self.data_spread(new_data, origin_addr)

    def _propogate_data(self):
        self.propogate_data_thread = threading.Thread(target=self.queue_listener)
        self.propogate_data_thread.start()

    def _stop_servers(self):
        self.stop_serv = True
        self.stop_advertiser = True
        self.peer_aware = False
        
        self.advertise_thread.join()
        self.serv_manager_thread.join()

    def _stop_sockets(self):
        self.connection_state.ports_lock.acquire()
        print('Connecting to available ports for shutdown')
        for port in self.connection_state.available_ports:
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((self.local_ip, port))
        
        for conn in chain(self.connection_state.available_ports.values(), self.connection_state.used_ports.values()):
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError as e:
                if e.args[0] == 107: continue
            conn.close()
        self.connection_state.ports_lock.release()
        
        # clean up ports
        self.connection_state.clear_ports()

    def _stop_advertising(self):
        """
        Stop the broadcast of available data.
        Any data still present in the pipeline is lost
        """
        self.advertise_to_peers.close()
        
        # close advertise data queue in 3 seconds
        self.peer_notifier_thread.join(3)

    def _stop_data_propogation(self):
        self.data_to_propagate.put((self.local_ip, 'stop'))
        self.propogate_data_thread.join()
        

    def shutdown(self):
        self._stop_sockets()
        self._stop_servers()
        self._stop_advertising()
        self._stop_data_propogation()

