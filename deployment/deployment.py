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

# Create server directory
def create_server():
    if not os.path.exists(f"{PARENT_DIR}/../server"):
        os.mkdir(f"{PARENT_DIR}/../server")
    if not os.path.exists(f"{PARENT_DIR}/../server/watch_folder"):
        os.mkdir(f"{PARENT_DIR}/../server/watch_folder")
    
    shutil.copyfile(f"{PARENT_DIR}/config.json", f"{PARENT_DIR}/../server/config.json")
    shutil.copyfile(f"{PARENT_DIR}/server.py", f"{PARENT_DIR}/../server/server.py")

# Create N client directories
def create_clients(N):
    for i in range(N):
        if not os.path.exists(f"{PARENT_DIR}/../client_{i}"):
            os.mkdir(f"{PARENT_DIR}/../client_{i}")
        if not os.path.exists(f"{PARENT_DIR}/../client_{i}/download_folder"):
            os.mkdir(f"{PARENT_DIR}/../client_{i}/download_folder")

        shutil.copyfile(f"{PARENT_DIR}/config.json", f"{PARENT_DIR}/../client_{i}/config.json")
        shutil.copyfile(f"{PARENT_DIR}/client.py", f"{PARENT_DIR}/../client_{i}/client.py")

# Delete server directory
def delete_server():
    shutil.rmtree(f"{PARENT_DIR}/../server")

# Delete N client directories
def delete_clients(N):
    for i in range(N):
        shutil.rmtree(f"{PARENT_DIR}/../client_{i}")

# Create test loads for client_idx
def create_test_loads(idx):
    for size in TEST_LOAD_SIZES:
        f = open(f"{PARENT_DIR}/../client_{idx}/download_folder/load_{size}","wb")
        f.write(os.urandom(size))
        f.close()

# Evaluation 1:
def evaluation_1():
    N = 3

    create_server()
    create_clients(N)

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

    delete_clients(N)
    delete_server()
    pass

# Evaluation 2:
def evaluation_2():
    
    number_of_requests = 5
    commands = ['get_files_list'] * number_of_requests

    N = [2,4,8]
    avg_query_times = [0 for _ in range(len(N))]

    for n in N:
        create_server()
        create_clients(n)

        for i in range(n):
            create_test_loads(i)

        # start server and client and check
        server_process = Popen(['python','server.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../server")    

        client_processes = []

        for i in range(n):
            client_process = Popen(['python','client.py','Mr.{i}']+commands+["quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_{i}")
            client_processes.append(client_process)

        # wait for commands to finish
        time.sleep(5)

        for i in range(n):
            client_processes[i].wait()
        server_process.kill()

        count = 0

        for i in range(n):
            f = open(f"{PARENT_DIR}/../client_{i}/client_log.txt","r")
            lines = f.readlines()
            f.close()

            for j in lines:
                try:
                    temp = j.split(' ')
                    if temp[2] == "FileQueryComplete:":
                        avg_query_times[N.index(n)] += float(temp[3])
                        count += 1
                except IndexError as e:
                    pass

        avg_query_times[N.index(n)] /= count

        delete_clients(n)
        delete_server()
    
    print("Evaluation 2 Passed.")
    plt.plot(N,avg_query_times)
    plt.xticks(N)
    plt.title('Average Response Time For Increasing Number Of Concurrent Peers')
    plt.xlabel('Number of Peers Connected To Server')
    plt.ylabel('Average Query Response Time (ms)')
    plt.savefig(f"{PARENT_DIR}/../results/eval2.png")

    pass

# Evaluation 3:
def evaluation_3():
    N = 4
    avg_download_speed = [0 for _ in range(len(TEST_LOAD_SIZES))]
    number_of_file_downloads = 3
    
    for size in TEST_LOAD_SIZES:
        create_server()
        create_clients(N)

        for n in range(N):
            create_test_loads(n)

        # start server and client and check
        server_process = Popen(['python','server.py'], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../server")    
        client_process_1 = Popen(['python','client.py','Mr.0']+[f"download 1 load_{size}"]*number_of_file_downloads+["quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_0")
        client_process_2 = Popen(['python','client.py','Mr.1']+[f"download 2 load_{size}"]*number_of_file_downloads+["quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_1")
        client_process_3 = Popen(['python','client.py','Mr.2']+[f"download 3 load_{size}"]*number_of_file_downloads+["quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_2")
        client_process_4 = Popen(['python','client.py','Mr.3']+[f"download 0 load_{size}"]*number_of_file_downloads+["quit"], stdout=PIPE, stdin=PIPE, stderr=PIPE, cwd=f"{PARENT_DIR}/../client_3")

        # wait for downloads to finish

        client_process_1.wait()
        client_process_2.wait()
        client_process_3.wait()
        client_process_4.wait()
        server_process.kill()

        time.sleep(5)

        count = 0 

        for i in range(N):
            f = open(f"{PARENT_DIR}/../client_{i}/client_log.txt","r+")
            lines = f.readlines()
            f.close()

            for j in lines:
                try:
                    temp = j.split(' ')
                    if temp[2] == "DownloadComplete:" and int(temp[8].split('_')[1]) == size:
                        avg_download_speed[TEST_LOAD_SIZES.index(size)] += float(temp[3]) 
                        count += 1 if float(temp[3]) != 0.0 else 0
                except IndexError as e:
                    pass
        
        
        avg_download_speed[TEST_LOAD_SIZES.index(size)] /= count

        delete_clients(N)
        delete_server()
    
    print(avg_download_speed)

    print("Evaluation 3 Passed.")
    plt.plot(TEST_LOAD_SIZES,avg_download_speed)
    plt.title('Average Transfer Time For Increasing Size of Test Load')
    plt.xlabel('Size of Test Load (Bytes)')
    plt.ylabel('Average Transfer Speed (ms)')
    plt.savefig(f"{PARENT_DIR}/../results/eval3.png")
        
    
    pass

if __name__ == "__main__":
    if sys.argv[1] == "-1":
        evaluation_1()
    elif sys.argv[1] == "-2":
        evaluation_2()
    elif sys.argv[1] == "-3":
        evaluation_3()
    elif sys.argv[1] == "-c" and len(sys.argv) == 3:
        try: 
            N = int(sys.argv[2])
            create_server()
            create_clients(N)
        except Exception as e:
            print(e)
    elif sys.argv[1] == "-d" and len(sys.argv) == 3:
        try:
            N = int(sys.argv[2])
            delete_server()
            delete_clients(N)
        except Exception as e:
            print(e)

    pass
