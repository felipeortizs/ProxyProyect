# Proxy inverso con balanceador de cargas
# Python 3


# Modulos requeridos
import socket
import _thread
import threading
import sys

# Habilitar bloqueo para un thread
print_lock = threading.Lock()

# Configuración de opciones #

def option_check():
    #Argumentos recibidos
    avail_options = ["-port"]

    #Recibe opciones y argumentos dados por el cliente
    options = [opt for opt in sys.argv[1:] if opt.startswith("-")]    
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

    #Error en caso de que las opciones no sean válidos
    for i in options:
        if i not in avail_options:
            raise SystemExit(f"Usage: {sys.argv[0]} -port <argument>...")

    #Error en caso de que los argumentos no sean válidos
    if len(options) != 1 or len(args) != 1:
        raise SystemExit(f"Usage: {sys.argv[0]} -port <argument>...")

    return args


# Funciones básicas #

#Función que servirá para iterar entre los servidores disponibles
servers=[['172.31.21.234', '8080'], ['google.com', '']]
n = -1

def round_rob_server():
  global n
  n = n + 1
  if n == len(servers):
     n = 0
  return servers[n]
    
# Establecer nueva conexión (cliente o servidor)
def on_new_client(clientsocket,addr):
    while True:
        #Petición recibida idor y formato HTTP/1.1 si es una petición de un cliente
        request = clientsocket.recv(1024).decode()             
        #requestjson={}
        print("**************************************")
        print("Nueva petición de recibida")  
        #Evaluamos que la petición no esté vacía
        if not request:
            print_lock.release()
            break
        #En caso de que la conexión sea proveniente de un cliente, su petición es enviada a los servidores disponibles.
        #La respuesta de los servers se le entrega dal cliente.
        print("request: ", request)
        #Obtener datos del cliente (ip y puerto)
        ip, port = clientsocket.getpeername()
        print ('Mensaje recibido del cliente: ', ip, \
                                            "port ", port)
        contact_server = round_rob_server()
        server_name = contact_server[0]
        server_port = int(contact_server[1])
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.connect((server_name,server_port))        
        print("Reenviando datos a servidor con ip", server_name, \
                                                "port", server_port)
        server_socket.send(request.encode())
        response = server_socket.recv(2048).decode()
        print("Response recibida", response)
        print("")
        #Cerramos conexión con el server
        server_socket.close()
        #Enviamos HTTP response al cliente
        #response = 'HTTP/1.0 200 OK\n\n'+response
        print("Enviando response", response,)
        clientsocket.sendall(response.encode())
        print("Response se ha envíado al cliente correctamente")
        break
    #Cerramos conexión con el cliente
    clientsocket.close()
               
# Función Main #

if __name__ == "__main__":
    args = option_check()
    #Creamos objeto socket
    s = socket.socket()  
    host = '172.31.21.133' # ip de la instancia donde corre el proxy
    #El puerto se recibe como argumento
    port = int(args[0])              
    print("Ejecutando PILB en puerto", port)
    s.bind((host, port))     
    #Se permite tener hasta 10 clientes conectados
    s.listen(100)

    while True:
        c, addr = s.accept()     # Establecemos conexión con el cliente
        print_lock.acquire(blocking=False)
        _thread.start_new_thread(on_new_client,(c,addr))        
    s.close()