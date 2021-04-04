import socket
import select
import os
import datetime
from _thread import *
import json
import hashlib


# get configurations
config = json.load(open(f"{os.path.dirname(os.path.abspath(__file__))}/config.json"))

IP = config['server']['ip_address']
HEADER_LENGTH = config['header_length']
META_LENGTH = config['meta_length']
THREAD_PORTS = config['server']['ports']
WATCH_FOLDER_NAME = config['server']['watch_folder_name']
LOG = open(f"{os.path.dirname(os.path.abspath(__file__))}/{config['server']['log_file']}", "a")
CLIENT_FILES_DIR = f"{os.path.dirname(os.path.abspath(__file__))}/{WATCH_FOLDER_NAME}/client_files.json"
CLIENT_FILES = open(CLIENT_FILES_DIR, "w+")
json_client_files = json.load(CLIENT_FILES) if os.stat(CLIENT_FILES_DIR).st_size != 0 else json.loads(json.dumps({}))

#################################################################HELPER FUNCTIONS###################################################################
#################################################################HELPER FUNCTIONS###################################################################

###################################################################CLIENT RELATED###################################################################
###################################################################CLIENT RELATED###################################################################

###################################################################SERVER RELATED###################################################################
###################################################################SERVER RELATED###################################################################

# Logs messages
def log_this(msg):
    print(msg)
    LOG.write(f"{datetime.datetime.now()} {msg}\n")
    LOG.flush()

# Handles command receiving
def receive_command(client_socket):

    try:

        # Receive our "header" containing command length, it's size is defined and constant
        command_header = client_socket.recv(HEADER_LENGTH)

        # If we received no data, client gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
        if not len(command_header):
            return False

        # Get meta data
        meta = client_socket.recv(META_LENGTH)
        meta = meta.decode('utf-8').strip()

        # Convert header to int value
        command_length = int(command_header.decode('utf-8').strip())

        # Return an object of command header and command data
        return {'header': command_header, 'meta': meta, 'data': client_socket.recv(command_length)}

    except:
        # client closed connection, violently or by user
        return False

# Sends file directory to client
def send_file_directory(client_socket):
    try:
        list_of_dir = json.dumps(json_client_files).encode('utf-8')
        list_of_dir_header = f"{len(list_of_dir):<{HEADER_LENGTH}}".encode('utf-8')
        meta = f"{'':<{META_LENGTH}}".encode('utf-8')
        client_socket.send(list_of_dir_header + meta + list_of_dir)

    except:
        # client closed connection, violently or by user
        return False

# Updates file directory
def update_file_directory(client_id, dir_list):
    json_client_files[client_id] = dir_list.split('\n')

    # clear file and rewrite
    CLIENT_FILES.truncate(0)
    CLIENT_FILES.write(f"{json.dumps(json_client_files)}")
    CLIENT_FILES.flush()
 

def unregister_client(client_id):
    del json_client_files[client_id]

    # clear file and rewrite
    CLIENT_FILES.truncate(0)
    CLIENT_FILES.write(f"{json.dumps(json_client_files)}")
    CLIENT_FILES.flush()



if __name__ == "__main__":
    # Create list of server sockets
    server_sockets = []

    for i in range(len(THREAD_PORTS)):
        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        temp.bind((IP, THREAD_PORTS[i]))
        temp.listen()
        server_sockets.append(temp)

    # Create list of sockets for select.select()
    sockets_list = [i for i in server_sockets]

    # List of connected clients - socket as a key, user header and name as data
    clients = {}

    for port in THREAD_PORTS:
        log_this(f'Listening for connections on {IP}:{port}...')

    # Does Server Things
    while True:
        read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list)

        for notified_socket in read_sockets:

            # If notified socket is a server socket - new connection, accept it
            if notified_socket in server_sockets:

                client_socket, client_address = server_sockets[server_sockets.index(notified_socket)].accept()

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
                log_msg = 'Accepted new connection from {}:{}, username: {}'.format(*client_address, user['data'].decode('utf-8'))
                log_this(log_msg)
            
            # Else existing socket is sending a command
            else:

                # Receive command
                command = receive_command(notified_socket)

                # If False, client disconnected, cleanup
                if command is False:
                    log_msg = '{} Closed connection from: {}'.format(datetime.datetime.now(), clients[notified_socket]['data'].decode('utf-8'))                 
                    log_this(log_msg)

                    # remove connections
                    sockets_list.remove(notified_socket)
                    del clients[notified_socket]
                    continue

                # Get user by notified socket, so we will know who sent the command
                user = clients[notified_socket]

                # Get command
                command_msg = command["data"].decode("utf-8")
                command_msg = command_msg.split(' ')

                # logging
                log_msg = f'{datetime.datetime.now()} Received command from {user["data"].decode("utf-8")}: {command_msg[0]}'
                log_this(log_msg)

                # Handle commands
                if command_msg[0] == 'get_files_list':
                    start_new_thread(send_file_directory, (notified_socket,))
                    log_this(f'Sent file list to {user["data"].decode("utf-8")}')

                elif command_msg[0] == 'update_list':
                    start_new_thread(update_file_directory, (int(command['meta']),command_msg[1],))
                    log_this(f"Update directory for client_{int(command['meta'])}")

                elif command_msg[0] == 'unregister':
                    start_new_thread(unregister_client, (int(command['meta']),))
                    log_this(f"Unregister client_{int(command['meta'])}")

        # handle some socket exceptions just in case
        for notified_socket in exception_sockets:
            
            # remove connections
            sockets_list.remove(notified_socket)
            del clients[notified_socket]