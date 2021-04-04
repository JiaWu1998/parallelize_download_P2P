# Simple_Server_Client_System

This project is a peer-to-peer system where there is a central node that holds the directories of all files within the system and there are peer nodes that host those files. Peer nodes can download files from other peers with the help of the central node and they can also upload files to another peer, whether sequentially or simultaneously. 


To run a sample p2p system do the following:
1. type 'make setup' 
2. open 3 seperate terminals(use one for server and two for two peers)
3. choose one terminal and type 'python3 server/server.py'
4. choose a different terminal apart from the first one and type 'python3 client_0/client.py'
6. choose a different terminal apart from the first and second one and type 'python3 client_1/client.py'
7. choose a username for the peers in the peers' terminals and continue with the help of the manual from the peers' terminal
8. when you are done, type 'make clean'
* NOTE: All log files are in server and client folders. If you do a "make clean", it would be wiped out along with the server and client folders

To run evaluation 1 do the following:
1. type 'make eval_1'

To run evaluation 2 do the following:
1. type 'make eval_2'

To run evaluation 3 do the following:
1. type 'make eval_3'
