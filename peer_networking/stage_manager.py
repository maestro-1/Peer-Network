
import random
import socket
import logging
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
import atexit
from threading import Lock

from server import MULTICAST_PORT


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


class QueueListenerHandler(QueueHandler):
    def __init__(self, handlers, respect_handler_level=False, auto_run=True):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        
        self.queue = Queue(-1)

        super().__init__(self.queue)
        # handlers = _resolve_handlers(handlers)
        handlers = logging.StreamHandler()

        self._listener = QueueListener(
            self.queue,
            handlers,
            respect_handler_level=respect_handler_level)
        
        if auto_run:
            self.start()
            atexit.register(self.stop)


    def start(self):
        self._listener.start()

    def stop(self):
        self._listener.stop()

    def emit(self, record):
        return super().emit(record)


class ConnectionState:
    """
    Manages the states of the ports, existing server connections for thread safety
    """
    def __init__(self) -> None:
        # ports used by server and client for connection
        self.used_ports = dict()

        # Store IPs and connected sockets. IP being the key
        self.served_connections = dict()
        self.client_connections = dict()


        # Thread locks
        self.serv_conn_lock = Lock()
        self.client_conn_lock = Lock()
        self.ports_lock = Lock()

        # ports
        self.available_ports = dict()
        self.used_ports = dict()

    def set_server_conn(self, addr: str, server_sock: socket.socket):
        self.serv_conn_lock.acquire()
        self.served_connections[addr] = server_sock
        self.serv_conn_lock.release()

    def delete_server_conn(self, client_addr: str):
        self.serv_conn_lock.acquire()
        self.served_connections.pop(client_addr)
        self.serv_conn_lock.release()

    def set_client_conn(self, addr: str, client_sock: socket.socket):
        self.client_conn_lock.acquire()
        self.client_connections[addr] = client_sock
        self.client_conn_lock.release()

    def delete_client_conn(self, addr: str):
        self.client_conn_lock.acquire()
        self.client_connections.pop(addr)
        self.client_conn_lock.release()

    def set_available_port(self, port: int, client_sock: socket.socket):
        self.ports_lock.acquire()
        self.available_ports[port] = client_sock
        self.ports_lock.release()

    def delete_from_available_port(self, port: int, client_sock: socket.socket):
        self.ports_lock.acquire()
        self.available_ports.pop(port)
        self.ports_lock.release()

    def set_existing_port(self, port: int, client_sock: socket.socket):
        self.ports_lock.acquire()
        self.used_ports[port] = client_sock
        self.ports_lock.release()

    def delete_from_existing_port(self, port):
        self.ports_lock.acquire()
        self.used_ports.pop(port)
        self.ports_lock.release()

    def generate_port(self):
        """
        Generate a range of ports start with the Multicast port as the lowest.
        The generated port is a unique port that does not exist under the used port 
        or available port dict keys
        """
        random_port = random.randint(MULTICAST_PORT + 1, 65535)
        self.ports_lock.acquire()

        while random_port in self.available_ports or random_port in self.used_ports:
            random_port = random.randint(MULTICAST_PORT + 1, 65535)
        self.ports_lock.release()
        return random_port
    
    def server_exists(self, addr: str):
        self.serv_conn_lock.acquire()
        state = addr in self.served_connections.keys()
        self.serv_conn_lock.release()
        return state
        
    def client_exists(self, addr: str):
        self.client_conn_lock.acquire()
        state = addr in self.client_connections.keys()
        self.client_conn_lock.release()
        
        return state
        
    def clear_ports(self):
        self.ports_lock.acquire()
        self.available_ports.clear()
        self.used_ports.clear()
        self.ports_lock.release()

    
