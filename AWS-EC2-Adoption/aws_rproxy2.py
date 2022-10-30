# The Reverse Proxy
# Supports Python v3.*


# Import required modules
import socket
import _thread
import threading
import json
import sys
import time
import pandas as pd
import itertools

# Request Handler #

# Enable locking for a thread
print_lock = threading.Lock()

# Option Set Up #

def option_check():
    # all available argument options
    avail_options = ["-port"]

    # receive user given options
    options = [opt for opt in sys.argv[1:] if opt.startswith("-")]

    # receive user given arguments
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

    # raise error if user given option is wrong
    for i in options:
        if i not in avail_options:
            raise SystemExit(f"Usage: {sys.argv[0]} -port <argument>...")

    # raise error if not all options or arguments are available
    if len(options) != 1 or len(args) != 1:
        raise SystemExit(f"Usage: {sys.argv[0]} -port <argument>...")

    return args


# Basic RP Server Functions #

def round_robin(iterable):
    return next(iterable)

# define the available table
column_names = ["type", "id", "privPolyId", "listenport", "ip_addr"]
updated_available_server_table = pd.DataFrame(columns = column_names)

# define the packet switch table
# column_names = ["policy", "server_id", "connections"]
# server_info_table = pd.DataFrame(columns = column_names)

# create table of available servers
def available_server(msg):
    global updated_available_server_table
    global policy_table

    updated_available_server_table = updated_available_server_table.append(msg, ignore_index = True)
    policy_list = set(updated_available_server_table["privPolyId"].tolist())
    # print(policy_list)
    
    policy_table = {}
    for policy in policy_list:
        policy_table[policy] = itertools.cycle(set(updated_available_server_table\
                [updated_available_server_table["privPolyId"]==policy]["id"].tolist()))


    
# Establish connection with new client
def on_new_client(clientsocket,addr):
    while True:  
        request = clientsocket.recv(1024).decode()
        print("request: ", request)
        print(type(request))
        requestjson={}
        try:
            requestjson=json.loads(request)
            print("requestjson: ", requestjson)
            print(type(requestjson))
        except:
            pass    
                
        if not request:
            # lock released on exit
            print_lock.release()
            break
        if  "type" in requestjson and requestjson["type"] == "1":
            print("Request from new server")
            ip, port = clientsocket.getpeername()
            requestjson["ip_addr"] = ip
            print ("Received setup message from server id", requestjson["id"], "ip", requestjson["ip_addr"],\
                    "privacy policy", requestjson["privPolyId"], "port", requestjson["listenport"])
            available_server(requestjson)
        else:
            print("Request from new client")
            # Get the client request
            ip, port = clientsocket.getpeername()
            print ('Received a message from client ', ip, \
                                                "From port ", port)
            msg = json.dumps({"type": "0", "srcid": "proxyPILB420", "privPoliId": "111", "payload": "xyz1"})
            new_json_msg = json.loads(msg)
            policy = new_json_msg["privPoliId"]
            # print(policy)       
            
            target_host_id = round_robin(policy_table[policy])
            # print(target_host_id)
            server_name = updated_available_server_table.loc\
                            [updated_available_server_table["id"]==target_host_id, "ip_addr"].values[0]
            server_port = int(updated_available_server_table.loc\
                            [updated_available_server_table["id"]==target_host_id, "listenport"].values[0])

            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect((server_name,server_port))        
            print("Forwarding a data message to server id", target_host_id, "server ip", server_name, \
                                                    "port", server_port)
            server_socket.send(json.dumps(new_json_msg).encode())
            recv_msg = server_socket.recv(2048)
            recv_json_msg = json.loads(recv_msg.decode())
            response=recv_json_msg["response"]
            print("response forwarded", response)
            print ("Received a data message from server id", recv_json_msg["srcid"],\
                                                    "payload", recv_json_msg["payload"])
            print("")
            server_socket.close()
            # Send HTTP response
            response = 'HTTP/1.0 200 OK\n\n'+response
            #response = 'HTTP/1.0 200 OK\n\nHello World'
            print("Sending user's response", response)
            clientsocket.sendall(response.encode())
            print("Response has been succesfully sent")
    clientsocket.close()

                
# Main Function #

if __name__ == "__main__":
    args = option_check()
    s = socket.socket()         # Create a socket object
    # host = socket.gethostname() # Get local machine name
    host = '172.31.21.133'
    port = int(args[0])              # Reserve a port for your service.
    print("Running reverse proxy on port", port)

    # Binds to the port
    s.bind((host, port))     
    # Allow 10 clients to connect
    s.listen(100) 

    while True:
        c, addr = s.accept()     # Establish connection with client.
        # lock acquired by client
        print_lock.acquire()
        _thread.start_new_thread(on_new_client,(c,addr))
        
    s.close()