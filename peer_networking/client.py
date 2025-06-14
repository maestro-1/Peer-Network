from queue import Queue
import socket
from threading import Thread

from stage_manager import ConnectionState


class ClientPeer:
    def __init__(self, data_queue: Queue, connection_state: ConnectionState, client_broadcast: bool = False) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # data_queue is not empty it is shared across several objects
        self.data_queue = data_queue
        self.connection_state = connection_state

        self.client_broadcast = client_broadcast

    def create_client(self, addr: str, port: int):
        with self.sock:
            try:
                self.sock.connect((addr, port))
                
                # store the connection and port
                self.connection_state.set_existing_port(port, self.sock)
                self.connection_state.set_client_conn(addr, self.sock)
                
                while True:
                    data = self.sock.recv(1024)
                    if not data:
                        break
    
                    if data[0] == b'':
                        print('closing connection.')
                        break

                    if self.client_broadcast:
                        self.data_queue.put((addr, data))

            except ConnectionRefusedError as e:
                print('Error:', e, ', aborting client connection')

    def client_thread(self, addr: str, port: int):
        client_thread = Thread(target=self.create_client, args=(addr, port))
        client_thread.start()

        return client_thread