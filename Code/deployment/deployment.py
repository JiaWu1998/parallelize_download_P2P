import os
import shutil
from subprocess import Popen, PIPE
import time
import matplotlib.pyplot as plt
import sys

# Test File Load Sizes
TEST_LOAD_SIZES = [128,129,130,131,132,133,134,135,136,137]

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
        f = open(f"{PARENT_DIR}/../peer_{idx}/download_folder/load_{size}","w")
        f.write("b"*size)
        f.close()

# Evaluation 1:
def evaluation_1():
    N = 3

    create_index()
    create_peers(N)

    create_test_loads(1)
    create_test_loads(2)

    # start server and client and check
    server_process = Popen(['python','index.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../index")   
    time.sleep(2)

    peer_process_1 = Popen(['python','peer.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../peer_0")
    peer_process_2 = Popen(['python','peer.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../peer_1")
    peer_process_3 = Popen(['python','peer.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../peer_2")

    time.sleep(3)
    peer_process_1.communicate(input='download load_128\n download load_512'.encode("utf-8"))[0]

    time.sleep(3)

    download_completed = 0

    f = open(f"{PARENT_DIR}/../peer_0/client_log.txt","r")
    lines = f.readlines()
    f.close()

    for j in lines:
        try:
            if j.split(' ')[2] == "DownloadComplete:":
                download_completed += 1
        except IndexError as e:
            pass
    
    if download_completed == 2:
        print('Evaluation 1 Passed')
    else:
        print('Evaluation 1 Failed')
    
    peer_process_1.kill()
    peer_process_2.kill()
    peer_process_3.kill()
    server_process.kill()

    delete_peers(N)
    delete_index()
    pass

# Evaluation 1:
def evaluation_2():

    average_speed = []
    N = [2,4,8,16]

    for n in N:
        create_index()
        create_peers(n)

        for i in range(n):
            if i != 0:
                create_test_loads(i)

        # start server and client and check
        server_process = Popen(['python','index.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../index")   
        time.sleep(2)

        peer_processes = []

        for i in range(n):
            temp = Popen(['python','peer.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../peer_{i}")
            peer_processes.append(temp)

        time.sleep(3*n)

        command = ""

        for i in TEST_LOAD_SIZES:
            command += "download load_{i}\n"
        peer_processes[0].communicate(input=command.encode("utf-8"))[0]

        time.sleep(10)

        download_completed = 0
        speed = 0
        count = 0

        f = open(f"{PARENT_DIR}/../peer_0/client_log.txt","r")
        lines = f.readlines()
        f.close()

        for j in lines:
            try:
                if j.split(' ')[2] == "DownloadComplete:":
                    speed += float(j.split(' ')[3])
                    count += 1
                    download_completed += 1
            except IndexError as e:
                pass
        
        if download_completed == 10:
            print('Evaluation 2 Passed')

        else:
            print('Evaluation 2 Failed')
        
        speed /= count
        average_speed.append(speed)
        
        for i in range(n):
            peer_processes[i].kill()
        server_process.kill()

        delete_peers(n)
        delete_index()
    
    plt.plot(N,average_speed)
    plt.title('Average Transfer Time Vs Number of Nodes with 10 Same Files')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Average Transfer Speed (ms)')
    plt.savefig(f"{PARENT_DIR}/../results/eval2.png")

    
    


if __name__ == "__main__":
    if sys.argv[1] == "-1":
        evaluation_1()
    
    elif sys.argv[1] == "-2":
        evaluation_2()

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
