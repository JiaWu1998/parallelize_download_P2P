import socket
import select
import errno
import os
import time
from threading import Thread
import json
import hashlib
import sys
import hashlib
import datetime
from pathlib import Path
from _thread import *

# get configurations 
config = json.load(open(f"{os.path.dirname(os.path.abspath(__file__))}/config.json"))

CLIENT_ID = int(os.path.basename(Path(os.path.realpath(__file__)).parent).split('_')[1])

HEADER_LENGTH = config['header_length']
META_LENGTH = config['meta_length']
REDOWNLOAD_TIME = config['redownload_times']

IP = config['peers'][CLIENT_ID]['ip_address']
PORT = config['peers'][CLIENT_ID]['port']

INDEX_IP = config['index']['ip_address']
INDEX_PORT = config['index']['port']

PEERS = config['peers']

DOWNLOAD_FOLDER_NAME = config['peers'][CLIENT_ID]['download_folder_name']
LOG = open(f"{os.path.dirname(os.path.abspath(__file__))}/{config['peers'][CLIENT_ID]['log_file']}", "a")

#################################################################HELPER FUNCTIONS###################################################################
def log_this(msg):
    """
    Logs a messages
    """
    # print(msg)
    LOG.write(f"{datetime.datetime.now()} {msg}\n")
    LOG.flush()

def help():
    """
    Prints out a verbose description of all available functions on the client 
    """
    print("\n*** INFO ON FUNCTIONS ***\n")
    print("[function_name] [options] [parameters] - [description]\n")
    print("get_files_list - gets the file names from all the clients\n")
    print("download <file_name> ... <file_name> - downloads one or more files serially or parallely from the target client. To download serially, use it without the -p option\n")
    print("help - prints verbose for functions\n")
    print("quit - exits client interface\n")

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

###################################################################CLIENT RELATED###################################################################
def wait_for_list(full_command):
    """
    Waiting for a list of directories from the server
    """
    # Initial the query from the client peer
    send_message(current_peer_socket, "", full_command)

    start = time.time()

    # Keep trying to recieve until client recieved returns from the server
    while True:
        try:
            # Receive our "header" containing username length, it's size is defined and constant
            header = current_peer_socket.recv(HEADER_LENGTH)

            # If we received no data, server gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
            if not len(header):
                log_this(f"Connection closed by the server")
                return

            # Convert header to int value
            header = int(header.decode('utf-8').strip())

            # Get meta data
            meta = current_peer_socket.recv(META_LENGTH)
            meta = meta.decode('utf-8').strip()

            # Receive and decode msg
            dir_list = current_peer_socket.recv(header).decode('utf-8')
            dir_list = json.loads(dir_list)

            # Print List
            for peer in dir_list:
                print(f"Peer with id {peer}:")
                for file in dir_list[peer]:
                    file = file.split('+')[0]
                    print(f"\t{file}")
            
            end = time.time()
            
            log_this(f"FileQueryComplete: {(end-start)*1000} ms.")
            # Break out of the loop when list is recieved                
            break

        except IOError as e:

            if e.errno != errno.EAGAIN and e.errno != errno.EWOULDBLOCK:
                log_this('Reading error: {}'.format(str(e)))
                return

            # We just did not receive anything
            continue

        except Exception as e:
            # Any other exception - something happened, exit
            log_this('Reading error: '.format(str(e)))
            return

def parallelize_wait_for_file_chunk(peer_socket, peer_id, offset, chunk_size, file):
    """
    waiting function for parallelized/serial file download
    """
    start = time.time()

    # Initial the download from the client peer
    send_message(peer_socket, '', f"download {offset} {chunk_size} {file}")

    # open files
    fd = open(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/{file}",'w')
    redownload_count = 0

    # md5 reconstruction
    m = hashlib.md5()

    # Keep trying to recieve until client recieved returns from the server
    while True:
        try:
            header = peer_socket.recv(HEADER_LENGTH)  

            # If we received no data, server gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
            if not len(header):
                log_this('Connection closed by the server')
                return
            
            # Convert header to int value
            header = int(header.decode('utf-8').strip())

            # Get meta data
            meta = peer_socket.recv(META_LENGTH)
            meta = meta.decode('utf-8').strip().split(' ')

            # Recieve line and convert to string
            chunk = peer_socket.recv(header).decode('utf-8') 
            
            # if there is any error, remove all files
            if meta[0] == 'ERROR':
                log_this(chunk)
                break

            elif meta[0] == 'END':

                # if there is contamination in the checksum, log and delete file
                if m.hexdigest() != chunk:
                    log_this(f"Failed Download Chunk: Incorrect checksum for file {file} at offset {offset}")
                    break

                # clean up
                end = time.time()
                log_this(f"DownloadChunkComplete: {(end-start)*1000} ms. Downloaded chunk for file {file} on offset {offset} from peer_{peer_id}")
                break

            else:
                # when chunk is successfully downloaded, we update the md5 hash and write to the file at offset
                m.update(chunk.encode('utf-8'))
                fd.seek(offset)
                fd.write(chunk)
                fd.flush()
                fd.close()


        except IOError as e:

            if e.errno != errno.EAGAIN and e.errno != errno.EWOULDBLOCK:

                if redownload_count < REDOWNLOAD_TIME:
                    redownload_count += 1
                    continue
                
                log_this('Reading error: {}'.format(str(e)))
                return 

            # We just did not receive anything
            continue

        except Exception as e:
            # Any other exception - something happened, exit
            log_this('Reading error: {}'.format(str(e)))
            return

def find_peer_hosts(target_file):
    """
    Query the index server to find all of the peers that host the target_file
    """

    send_message(current_peer_socket, '', f"find_peer_hosts {CLIENT_ID} {target_file}")

    while True:
        try:
            # Receive our "header" containing username length, it's size is defined and constant
            header = current_peer_socket.recv(HEADER_LENGTH)

            # If we received no data, server gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
            if not len(header):
                log_this(f"Connection closed by the server")
                return
            
            # Convert header to int value
            header = int(header.decode('utf-8').strip())

            # Get meta data
            meta = current_peer_socket.recv(META_LENGTH)
            meta = meta.decode('utf-8').strip()

            # Receive and decode msg
            peer_hosts = current_peer_socket.recv(header).decode('utf-8')
            
            if meta == 'ERROR':
                log_this(f"FileNotFound: target_file is {target_file}")
                return False, False
            else:
                peer_hosts = peer_hosts.split(' ')
                peer_hosts = [int(i) for i in peer_hosts]
                peer_hosts, file_size = peer_hosts[:-1], peer_hosts[-1]
                return peer_hosts, file_size
            
        except IOError as e:

            if e.errno != errno.EAGAIN and e.errno != errno.EWOULDBLOCK:
                log_this('Reading error: {}'.format(str(e)))
                return

            # We just did not receive anything
            continue

        except Exception as e:
            # Any other exception - something happened, exit
            log_this('Reading error: '.format(str(e)))
            return
        

def wait_for_file_download(full_command):
    """
    Waiting for the file contents from the server
    """
    parameters = full_command.split(' ')[1:]

    if len(parameters) <= 0:
        log_this("ParameterError: Too less parameters")
        return
    
    for file in parameters:
        # check if parallelism is needed
        peer_hosts, file_size = find_peer_hosts(file)

        if peer_hosts:

            peer_sockets = []

            for peer_id in peer_hosts:
                temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                temp.connect((PEERS[peer_id]["ip_address"],PEERS[peer_id]["port"]))
                temp.setblocking(False)
                send_message(temp, '', my_username)
                peer_sockets.append(temp)
            
            chunk_size = file_size // len(peer_hosts)

            threads = []

            for i in range(len(peer_hosts)):
                t = Thread(target=parallelize_wait_for_file_chunk, args=(peer_sockets[i], peer_hosts[i], i*chunk_size, chunk_size, file,))   
                t.start()
                threads.append(t)


            for t in threads:
                t.join()
        
        

# CONTINUE HERE



# parallelize_wait_for_file_chunk(peer_socket, peer_id, offset, chunk_size, file):

    # # Compute ports that 'this' peer will try to connect
    # client_thread_ports = [i+((len(config['client']['ports'])+1)*target_client) for i in THREAD_PORTS]

    # # initialize connections with the other peer
    # client_sockets = []

    # for i in range(len(client_thread_ports)):
    #     temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     temp.connect((IP, client_thread_ports[i]))
    #     temp.setblocking(False)

    #     # Initialize conneciton with the server
    #     send_message(temp, '', my_username)
        
    #     # add the temp socket to the pool
    #     client_sockets.append(temp)

    # # starts waiting for file download
    # if parallelize:       
    #     for i in range(0, len(files), len(client_thread_ports)):
    #         thread_idx = 0
    #         threads = []
    #         for j in range(i,i+ len(client_thread_ports)):
    #             if j < len(files):
    #                 t = Thread(target=parallelize_wait_for_file_download, args=(client_sockets[thread_idx], [files[j]],))
    #                 t.start()
    #                 threads.append(t)
    #                 thread_idx += 1
            
    #         for t in threads:
    #             t.join()
    # else:
    #     # start = time.time()
    #     t = Thread(target=parallelize_wait_for_file_download, args=(client_sockets[0], files,))
    #     t.start()

    #     # end = time.time()
    #     # log_this(f"DownloadComplete: {(end-start)*1000} ms. Downloaded Files are {' '.join(files)}")

    return

###################################################################CLIENT RELATED###################################################################

###################################################################SERVER RELATED###################################################################
def receive_command(client_socket):
    """
    Handles command receiving
    """

    try:

        # Receive our "header" containing command length, it's size is defined and constant
        command_header = client_socket.recv(HEADER_LENGTH)

        # If we received no data, client gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
        if not len(command_header):
            return False

        # Convert header to int value
        command_length = int(command_header.decode('utf-8').strip())

        # Get meta data
        meta = client_socket.recv(META_LENGTH)
        meta = meta.decode('utf-8').strip()

        # Get data
        data = client_socket.recv(command_length)
        data = data.decode('utf-8')

        # Return an object of command header and command data
        return {'header': command_header, 'meta': meta, 'data': data}

    except:
        # client closed connection, violently or by user
        return False

def send_file_chunk(peer_socket, peers, parameters):
    """
    Sends file chunk to the peer
    """
    offset = int(parameters[0])
    chunk_size = int(parameters[1])
    file = parameters[2]

    # using md5 checksum
    m = hashlib.md5()

    try:
        fd = open(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/{file}",'r')

        # go to offset
        fd.seek(offset)

        # read the chunk
        chunk = fd.read(chunk_size)
        fd.close()

        # update md5 checksum
        m.update(chunk.encode('utf-8'))

        # send chunk 
        send_message(peer_socket, '', chunk)

        # send the end status of the file and the md5 check sum
        send_message(peer_socket, f"END", m.hexdigest())

        log_this(f"{file} was sent to {peers[peer_socket]['data']}")

    except Exception as e:
        # client closed connection, violently or by user
        send_message(peer_socket, "ERROR", str(e))
        return False

def server_daemon():
    """
    A daemon that listens for download requests from any other clients/peers
    """
    # Create server socket for current peer
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((IP, PORT))
    server_socket.listen()

    # Create list of sockets for select.select()
    sockets_list = [server_socket]

    # List of connected clients - socket as a key, user header and name as data
    peers = {}

    log_this(f'Listening for connections on {IP}:{PORT}...')
    
    while True:
        read_sockets, _, exception_sockets = select.select(sockets_list, [], sockets_list)

        for notified_socket in read_sockets:
            
            # If notified socket is a server socket - new connection, accept it
            if notified_socket == server_socket:
                
                client_socket, client_address = server_socket.accept()

                # Client should send his name right away, receive it
                user = receive_command(client_socket)

                # If False - client disconnected before he sent his name 
                if user is False:
                    continue

                # Add accepted socket to select.select() list
                sockets_list.append(client_socket)
                
                # Also save username and username header
                peers[client_socket] = user

                #logging
                log_msg = 'Accepted new connection from {}:{}, username: {}'.format(*client_address, user['data'])
                log_this(log_msg)

            # Else existing socket is sending a command
            else:

                # Recieve command
                command = receive_command(notified_socket)

                # If False, client disconnected, cleanup
                if command is False:
                    log_msg = '{} Closed connection from: {}'.format(datetime.datetime.now(), peers[notified_socket]['data'])
                    log_this(log_msg)

                    # remove connections
                    sockets_list.remove(notified_socket)
                    del peers[notified_socket]
                    continue
                
                # Get user by notified socket, so we will know who sent the command
                user = peers[notified_socket]

                # Get command
                command_msg = command["data"]
                command_msg = command_msg.split(' ')

                # logging
                log_msg = f'{datetime.datetime.now()} Recieved command from {user["data"]}: {command_msg[0]}\n'
                log_this(log_msg)

                # Handle commands
                if command_msg[0] == 'download':
                    start_new_thread(send_file_chunk, (notified_socket,peers,command_msg[1:],))
            
            # handle some socket exceptions just in case
            for notified_socket in exception_sockets:
                
                # remove connections
                sockets_list.remove(notified_socket)
                del peers[notified_socket]

def update_server():
    """
    send updated directory to server
    """
    try:
        base_path = os.path.dirname(os.path.abspath(__file__))
        list_of_dir = os.listdir(f"{base_path}/{DOWNLOAD_FOLDER_NAME}/")
        list_of_dir = [f"{i}+{os.path.getsize(f'{base_path}/{DOWNLOAD_FOLDER_NAME}/{i}')}" for i in list_of_dir]
        list_of_dir = '\n'.join(list_of_dir)
        send_message(current_peer_socket, str(CLIENT_ID), f"update_list {list_of_dir}")

    except:
        # client closed connection, violently or by user
        return False

def folder_watch_daemon():
    """
    Daemon that updates the directory to the server whenever a new file is added or an file is deleted
    """
    # Initialize file directory to server
    update_server()

    # get current file directories
    current_file_directory = os.listdir(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/")

    while True:
        temp = os.listdir(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/")
        if current_file_directory != temp:
            update_server()
            current_file_directory = temp
###################################################################SERVER RELATED###################################################################
                
if __name__ == "__main__":

    # Initialize connection with the server with central socket
    current_peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    current_peer_socket.connect((INDEX_IP, INDEX_PORT))
    current_peer_socket.setblocking(False)

    # create username to connect to the server
    my_username = f"peer_{CLIENT_ID}"
    send_message(current_peer_socket,'',my_username)

    # Start the peer's server daemon
    start_new_thread(server_daemon,())
    time.sleep(3)

    # Start folder watch daemon to automatically update to server
    start_new_thread(folder_watch_daemon,())
    time.sleep(3)
    
    # Manual Mode of Client Interface
    if len(sys.argv) == 1:
        
        # Print verbose client shell begins
        help()

        # Does Client Things
        while True:

            # Wait for user to input a command
            full_command = input(f'{my_username} > ').strip()
            command = full_command.split(' ')[0]
            parameters = full_command.split(' ')[1:]

            if command == "download":
                wait_for_file_download(full_command)

            elif command == "get_files_list":
                wait_for_list(full_command)

            elif command == "help":
                help()
            
            elif command == "quit":
                # Encode command to bytes, prepare header and convert to bytes, like for username above, then send
                send_message(current_peer_socket, str(CLIENT_ID), "unregister")
                LOG.close()
                sys.exit()
    
    # Automatic Mode of Client Interface
    
    #Args
    #python client.py command1 command2 ... commandn

    else:
        
        # Does Client Things
        for i in sys.argv[2:]:

            # give time to process command
            time.sleep(1)

            # Wait for user to input a command
            full_command = i
            command = full_command.split(' ')[0]
            parameters = full_command.split(' ')[1:]

            if command == "download":
                if len(parameters) != 0:
                    wait_for_file_download(full_command)
                else:
                    log_this("ParameterError: Too less parameters")

            elif command == "get_files_list":
                if len(parameters) == 0:
                    wait_for_list(full_command)
                else:
                    log_this("ParameterError: Too many parameters")

            elif command == "help":
                help()
            
            elif command == "quit":
                # Encode command to bytes, prepare header and convert to bytes, like for username above, then send
                send_message(current_peer_socket, str(CLIENT_ID), "unregister")
                LOG.close()
                sys.exit()

            