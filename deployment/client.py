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
IP = config['client']['ip_address']
PORT = config['server']['ports'][CLIENT_ID]
HEADER_LENGTH = config['header_length']
META_LENGTH = config['meta_length']
THREAD_PORTS = config['client']['ports']
DOWNLOAD_FOLDER_NAME = config['client']['download_folder_name']
REDOWNLOAD_TIME = config['redownload_times']
LOG = open(f"{os.path.dirname(os.path.abspath(__file__))}/{config['client']['log_file']}", "a")

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
    print("download -p <client_number> <file_name> ... <file_name> - downloads one or more files serially or parallely from the target client. To download serially, use it without the -p option\n")
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
    send_message(central_socket, "", full_command)

    start = time.time()

    # Keep trying to recieve until client recieved returns from the server
    while True:
        try:
            # Receive our "header" containing username length, it's size is defined and constant
            header = central_socket.recv(HEADER_LENGTH)

            # If we received no data, server gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
            if not len(header):
                log_this(f"Connection closed by the server")
                return

            # Convert header to int value
            header = int(header.decode('utf-8').strip())

            # Get meta data
            meta = central_socket.recv(META_LENGTH)
            meta = meta.decode('utf-8').strip()

            # Receive and decode msg
            dir_list = central_socket.recv(header).decode('utf-8')
            dir_list = json.loads(dir_list)

            # Print List
            for client in dir_list:
                print(f"Client with id {client}:")
                for file in dir_list[client]:
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

def parallelize_wait_for_file_download(client_socket, files):
    """
    waiting function for parallelized/serial file download
    """
    # Initial the download from the client peer
    send_message(client_socket, '', f"download {' '.join(files)}")

    # open files
    fds = [open(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/{files[i]}",'w') for i in range(len(files))]
    files_closed = 0
    redownload_count = 0

    # md5 reconstruction
    m = [hashlib.md5() for _ in range(len(files))]

    # Keep trying to recieve until client recieved returns from the server
    while True:
        try:
            start = time.time()
            header = client_socket.recv(HEADER_LENGTH)  

            # If we received no data, server gracefully closed a connection, for example using socket.close() or socket.shutdown(socket.SHUT_RDWR)
            if not len(header):
                log_this('Connection closed by the server')
                return
            
            # Convert header to int value
            header = int(header.decode('utf-8').strip())

            # Get meta data
            meta = client_socket.recv(META_LENGTH)
            meta = meta.decode('utf-8').strip()
            meta = meta.split(' ')

            # Recieve line and convert to string
            line = client_socket.recv(header).decode('utf-8') 

            # if there is any error, remove all files
            if meta[0] == 'ERROR':
                log_this(line)
                
                for i in range(len(files)):
                    fds[i].flush()
                    fds[i].close()
                    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/{files[i]}")
                break
            
            # Flush and close and files is finished recieving
            elif meta[0] == 'END':
                fds[int(meta[1])].flush()
                fds[int(meta[1])].close()
                files_closed += 1

                # if there is contamination in the checksum, log and delete file
                if m[int(meta[1])].hexdigest() != line:
                    log_this(f"Incorrect checksum for file : {files[int(meta[1])]}")
                    log_this(f"Deleting file : {files[int(meta[1])]}")
                    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/{files[int(meta[1])]}")
 
            # continue to write and flush to files
            else:
                m[int(meta[0])].update(line.encode('utf-8'))
                fds[int(meta[0])].write(line)
                fds[int(meta[0])].flush()
            
            # when all files are closed/downloaded sucessfully then we can break from the loop
            if files_closed == len(fds):
                end = time.time()
                log_this(f"DownloadComplete: {(end-start)*1000} ms. Downloaded Files are {' '.join(files)}")
                break

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

def wait_for_file_download(full_command):
    """
    Waiting for the file contents from the server
    """
    parameters = full_command.split(' ')[1:]

    # check for parallelism option
    if parameters[0] == '-p':
        if len(parameters) <= 2:
            log_this("ParameterError: Too less parameters")
            return
        else:
            parallelize = True
            target_client = int(parameters[1])
            files = parameters[2:]
    else:
        if len(parameters) <= 1:
            log_this("ParameterError: Too less parameters")
            return
        else:
            parallelize = False
            target_client = int(parameters[0])
            files = parameters[1:]


    # if the target client is itself, don't do anything
    if target_client == CLIENT_ID:
        log_this("WrongClient: Target Client is Current Client.")
        return

    # Compute ports that 'this' peer will try to connect
    client_thread_ports = [i+((len(config['client']['ports'])+1)*target_client) for i in THREAD_PORTS]

    # initialize connections with the other peer
    client_sockets = []

    for i in range(len(client_thread_ports)):
        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp.connect((IP, client_thread_ports[i]))
        temp.setblocking(False)

        # Initialize conneciton with the server
        send_message(temp, '', my_username)
        
        # add the temp socket to the pool
        client_sockets.append(temp)

    # starts waiting for file download
    if parallelize:       
        for i in range(0, len(files), len(client_thread_ports)):
            thread_idx = 0
            threads = []
            for j in range(i,i+ len(client_thread_ports)):
                if j < len(files):
                    t = Thread(target=parallelize_wait_for_file_download, args=(client_sockets[thread_idx], [files[j]],))
                    t.start()
                    threads.append(t)
                    thread_idx += 1
            
            for t in threads:
                t.join()
    else:
        # start = time.time()
        t = Thread(target=parallelize_wait_for_file_download, args=(client_sockets[0], files,))
        t.start()

        # end = time.time()
        # log_this(f"DownloadComplete: {(end-start)*1000} ms. Downloaded Files are {' '.join(files)}")

    return

def update_server():
    """
    send updated directory to server
    """
    try:
        list_of_dir = os.listdir(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/")
        list_of_dir = '\n'.join(list_of_dir)
        send_message(central_socket, str(CLIENT_ID), f"update_list {list_of_dir}")

    except:
        # client closed connection, violently or by user
        return False

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

def send_files(peer_socket, peers, files):
    """
    Sends file to the peer
    """
    try:
        fds = [open(f"{os.path.dirname(os.path.abspath(__file__))}/{DOWNLOAD_FOLDER_NAME}/{files[i]}",'r') for i in range(len(files))]
        
        for i in range(len(files)):
            # using md5 checksum
            m = hashlib.md5()
            
            while True:

                # read line
                line = fds[i].readline()

                if not line:
                    # send the end status of the file and the md5 check sum
                    send_message(peer_socket, f"END {i}", m.hexdigest())

                    log_this(f"{files[i]} was sent to {peers[peer_socket]['data']}")
                    break
                
                line = line.encode('utf-8')

                # update md5 checksum
                m.update(line)

                # send line 
                send_message(peer_socket, str(i), line)

            fds[i].close()

    except Exception as e:
        # client closed connection, violently or by user
        send_message(peer_socket, "ERROR", str(e))
        return False

def server_daemon():
    """
    A daemon that listens for download requests from any other clients/peers
    """
    # Create list of listening server sockets 
    server_sockets = []

    # Compute ports that 'this' peer will listen to as a server
    server_thread_ports = [i+((len(config['client']['ports'])+1)*CLIENT_ID) for i in THREAD_PORTS]

    for i in range(len(server_thread_ports)):
        temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        temp.bind((IP, server_thread_ports[i]))
        temp.listen()
        server_sockets.append(temp)

    # Create list of sockets for select.select()
    sockets_list = [i for i in server_sockets]

    # List of connected clients - socket as a key, user header and name as data
    peers = {}

    for port in server_thread_ports:
        log_this(f'Listening for connections on {IP}:{port}...')
    
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
                    start_new_thread(send_files, (notified_socket,peers,command_msg[1:],))
            
            # handle some socket exceptions just in case
            for notified_socket in exception_sockets:
                
                # remove connections
                sockets_list.remove(notified_socket)
                del peers[notified_socket]

def folder_watch_daemon(current_file_directory):
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

    # Start the peer's server daemon
    start_new_thread(server_daemon,())
    time.sleep(3)

    # Initialize connection with the server with central socket
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    central_socket.connect((IP, PORT))
    central_socket.setblocking(False)

    # create username to connect to the server
    my_username = f"peer_{CLIENT_ID}"
    send_message(central_socket,'',my_username)

    # Start folder watch daemon to automatically update to server
    start_new_thread(folder_watch_daemon,())
    
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
                send_message(central_socket, str(CLIENT_ID), "unregister")
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
                send_message(central_socket, str(CLIENT_ID), "unregister")
                LOG.close()
                sys.exit()

            