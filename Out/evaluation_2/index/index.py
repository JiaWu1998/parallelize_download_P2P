import socket
import select
import os
import datetime
from _thread import *
import json
import hashlib


# get configurations
config = json.load(open(f"{os.path.dirname(os.path.abspath(__file__))}/config.json"))

HEADER_LENGTH = config['header_length']
META_LENGTH = config['meta_length']

IP = config['index']['ip_address']
PORT = config['index']['port']

WATCH_FOLDER_NAME = config['index']['watch_folder_name']
LOG = open(f"{os.path.dirname(os.path.abspath(__file__))}/{config['index']['log_file']}", "a")

json_peer_files = json.loads(json.dumps({}))

#################################################################HELPER FUNCTIONS###################################################################
def log_this(msg):
    """
    Logs messages
    """
    print(msg)
    LOG.write(f"{datetime.datetime.now()} {msg}\n")
    LOG.flush()

def send_message(target_socket, metadata, message):
    """
    Send a message to a target socket with meta data
    """
    log_this(f"MESSAGE SENT with metadata:{metadata} message:{message}")
    header = f"{len(message):<{HEADER_LENGTH}}".encode('utf-8')
    metadata = f"{metadata:<{META_LENGTH}}".encode('utf-8')
    message = message.encode('utf-8')
    target_socket.send(header + metadata + message)

#################################################################HELPER FUNCTIONS###################################################################

def receive_command(peer_socket):
    """
    Handles command receiving
    """

    try:

        # Receive our "header" containing command length, it's size is defined and constant
        command_header = peer_socket.recv(HEADER_LENGTH)

        # If we received no data, client gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
        if not len(command_header):
            return False

        # Convert header to int value
        command_length = int(command_header.decode('utf-8').strip())

        # Get meta data
        meta = peer_socket.recv(META_LENGTH)
        meta = meta.decode('utf-8').strip()

        # Get data
        data = peer_socket.recv(command_length)
        data = data.decode('utf-8')

        # Return an object of command header and command data
        return {'header': command_header, 'meta': meta, 'data': data}

    except:
        # client closed connection, violently or by user
        return False

def send_file_directory(peer_socket):
    """
    Sends json string of the peer files list back to the peer that request the list 
    """
    try:
        send_message(peer_socket, '', json.dumps(json_peer_files))
    except:
        # client closed connection, violently or by user
        return False

def update_file_directory(peer_id, dir_list):
    """
    Updates the json peer files list with the sub list of peer list from peer with peer_id
    """
    json_peer_files[peer_id] = dir_list.split('\n')

def unregister_client(peer_id):
    """
    Unregisters the client from the local list of client directories
    """
    del json_peer_files[peer_id]

def find_peer_hosts(peer_socket, peer_id, target_file):
    """
    Finds all peers that host the target_file and send the result back to the peer socket
    """
    
    peer_hosts = []
    target_file_size = 0


    for i in json_peer_files:
        if i != peer_id:
            for j in json_peer_files[i]:
                file_name = j.split('+')[0]

                if target_file == file_name:
                    target_file_size = j.split('+')[1] if str(j.split('+')[1]) != 0 else target_file_size
                    peer_hosts.append(str(i))
                    break
    
    if peer_hosts:
        peer_hosts.append(str(target_file_size))
        send_message(peer_socket,'', ' '.join(peer_hosts))
    else:
        send_message(peer_socket,'ERROR', 'File not Found')

if __name__ == "__main__":
    # Create index socket
    index_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    index_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    index_socket.bind((IP, PORT))
    index_socket.listen()

    # Create list of sockets for select.select()
    sockets_list = [index_socket]

    # List of connected clients - socket as a key, user header and name as data
    clients = {}

    log_this(f'Listening for connections on {IP}:{PORT}...')

    # Does Server Things
    while True:
        read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list)

        for notified_socket in read_sockets:

            # If notified socket is a server socket - new connection, accept it
            if notified_socket == index_socket:

                client_socket, client_address = index_socket.accept()

                # Client should send his name right away, receive it
                user = receive_command(client_socket)

                # If False - client disconnected before he sent his name
                if user is False:
                    continue

                # Add accepted socket to select.select() list
                sockets_list.append(client_socket)

                # Also save username and username header
                clients[client_socket] = user

                # logging 
                log_msg = 'Accepted new connection from {}:{}, username: {}'.format(*client_address, user['data'])
                log_this(log_msg)
            
            # Else existing socket is sending a command
            else:

                # Receive command
                command = receive_command(notified_socket)

                # If False, client disconnected, cleanup
                if command is False:
                    log_msg = '{} Closed connection from: {}'.format(datetime.datetime.now(), clients[notified_socket]['data'])                 
                    log_this(log_msg)

                    # remove connections
                    sockets_list.remove(notified_socket)
                    del clients[notified_socket]
                    continue

                # Get user by notified socket, so we will know who sent the command
                user = clients[notified_socket]

                # Get command
                command_msg = command["data"]
                command_msg = command_msg.split(' ')

                # logging
                log_msg = f'{datetime.datetime.now()} Received command from {user["data"]}: {command_msg[0]}'
                log_this(log_msg)

                # Handle commands
                if command_msg[0] == 'get_files_list':
                    start_new_thread(send_file_directory, (notified_socket,))

                elif command_msg[0] == 'update_list':
                    start_new_thread(update_file_directory, (int(command['meta']),command_msg[1],))

                elif command_msg[0] == 'unregister':
                    start_new_thread(unregister_client, (int(command['meta']),))
                
                elif command_msg[0] == 'find_peer_hosts':
                    start_new_thread(find_peer_hosts, (notified_socket,int(command_msg[1]),command_msg[2],))

        # handle some socket exceptions just in case
        for notified_socket in exception_sockets:
            
            # remove connections
            sockets_list.remove(notified_socket)
            del clients[notified_socket]