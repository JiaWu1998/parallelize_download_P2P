import os
import shutil
from subprocess import Popen, PIPE
import time
import matplotlib.pyplot as plt
import sys

# Test File Load Sizes
TEST_LOAD_SIZES = [128,512,2000,8000,32000]

# Get parent directory
PARENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Create indexing server directory
def create_index():
    if not os.path.exists(f"{PARENT_DIR}/../index"):
        os.mkdir(f"{PARENT_DIR}/../index")
    if not os.path.exists(f"{PARENT_DIR}/../index/watch_folder"):
        os.mkdir(f"{PARENT_DIR}/../index/watch_folder")
    
    shutil.copyfile(f"{PARENT_DIR}/config.json", f"{PARENT_DIR}/../index/config.json")
    shutil.copyfile(f"{PARENT_DIR}/index.py", f"{PARENT_DIR}/../index/index.py")

# Create N peer directories
def create_peers(N):
    for i in range(N):
        if not os.path.exists(f"{PARENT_DIR}/../peer_{i}"):
            os.mkdir(f"{PARENT_DIR}/../peer_{i}")
        if not os.path.exists(f"{PARENT_DIR}/../peer_{i}/download_folder"):
            os.mkdir(f"{PARENT_DIR}/../peer_{i}/download_folder")

        shutil.copyfile(f"{PARENT_DIR}/config.json", f"{PARENT_DIR}/../peer_{i}/config.json")
        shutil.copyfile(f"{PARENT_DIR}/peer.py", f"{PARENT_DIR}/../peer_{i}/peer.py")

# Delete server directory
def delete_index():
    shutil.rmtree(f"{PARENT_DIR}/../index")

# Delete N client directories
def delete_peers(N):
    for i in range(N):
        shutil.rmtree(f"{PARENT_DIR}/../peer_{i}")

# Create test loads for client_idx
def create_test_loads(idx):
    for size in TEST_LOAD_SIZES:
        f = open(f"{PARENT_DIR}/../peer_{idx}/download_folder/load_{size}","wb")
        f.write(os.urandom(size))
        f.close()

# Evaluation 1:
def evaluation_1():
    N = 3

    create_index()
    create_peers(N)

    for n in range(N):
        create_test_loads(n)

    # start server and client and check
    server_process = Popen(['python','server.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../server")    
    client_process_1 = Popen(['python','client.py','Mr.0',f"download 1 load_{TEST_LOAD_SIZES[-1]}", "quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_0")
    client_process_2 = Popen(['python','client.py','Mr.1',f"download 2 load_{TEST_LOAD_SIZES[-1]}", "quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_1")
    client_process_3 = Popen(['python','client.py','Mr.2',f"download 0 load_{TEST_LOAD_SIZES[-1]}", "quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_2")

    # wait for downloads to finish
    time.sleep(5)

    client_process_1.wait()
    client_process_2.wait()
    client_process_3.wait()
    server_process.kill()

    download_completed = 0
    for i in range(N):
        f = open(f"{PARENT_DIR}/../client_{i}/client_log.txt","r")
        lines = f.readlines()
        f.close()

        for j in lines:
            try:
                if j.split(' ')[2] == "DownloadComplete:":
                    download_completed += 1
            except IndexError as e:
                pass
    
    if download_completed == N:
        print('Evaluation 1 Passed')
    else:
        print('Evaluation 1 Failed')

    delete_peers(N)
    delete_index()
    pass


if __name__ == "__main__":
    if sys.argv[1] == "-1":
        evaluation_1()

    elif sys.argv[1] == "-c" and len(sys.argv) == 3:
        try: 
            N = int(sys.argv[2])
            create_index()
            create_peers(N)
        except Exception as e:
            print(e)
    elif sys.argv[1] == "-d" and len(sys.argv) == 3:
        try:
            N = int(sys.argv[2])
            delete_index() 
            delete_peers(N)     
        except Exception as e:
            print(e)

    pass
