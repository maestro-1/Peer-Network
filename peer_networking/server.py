import socket
import queue
from threading import Thread


MULTICAST_ADDR = '224.0.0.255'
MULTICAST_PORT = 16000



class Advertiser:
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
        """
        start up thread to allow listening without blocking other functions
        """
        self.host_listen_thread = Thread(target=self._listen_in)
        self.host_listen_thread.start() 

    def _listen_in(self):
        """
        listening for available servers to connect to and close connections. 
        Messages are inserted into a queue for peers to access and create a connection
        """
        
        print("listening for connection requests")
        while True:
            data, sender = self.peer.recvfrom(1024)
            sender_addr, sender_port = sender
            
            if sender_addr == self.host and data == b'stop':
                print('Received stop signal')
                self.notifs.put(('stop', sender_addr, sender_port))
                return
            else:
                print(f'Ignoring broadcast message from {sender_addr}')
            
            print(sender_addr, 'multicast:', data)
            self.notifs.put((data.decode(), sender_addr, sender_port))

    def configure_multicast(self):
        """
        Configure the socket to work with the UDP
        """
        self.peer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.peer.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

        # add a new member to the multicast on peer join.
        self.peer.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(MULTICAST_ADDR) + socket.inet_aton("0.0.0.0"))
        self.peer.bind(('0.0.0.0', MULTICAST_PORT))

    def cast_for_connection(self, message):
        """
        Advertise free ports available for clients to connect
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
        
        # close socket
        self.peer.close()


class PeerServer:
    def __init__(self, port: int, data_queue: queue.Queue, server_broadcast: bool = False) -> None:
        # setup socket and listen
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('0.0.0.0', port))
        self.server.listen()

        self.server_broadcast = server_broadcast

        # data_queue is not empty it is shared across several objects
        self.data_queue = data_queue

    def new_server(self):
        while True:
            try:
                client_conn, client_addr = self.server.accept()
            except OSError as e:
                if e.args[0] == 22: 
                    break
                print('Error accepting client:', e)
                self.server.close()
                break

            with client_conn:
                while True:
                    data = client_conn.recv(1024)
                    if not data: 
                        break
                    if data[0] == b'':
                        break

                    if self.server_broadcast:
                        self.data_queue.put((client_addr, data))

        print("stopping server")

    def server_thread(self):
        server_thread = Thread(target=self.new_server)
        server_thread.start()
        
        return server_thread


            

 
    

        
