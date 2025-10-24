import socket
import threading

BUFFER_SIZE = 1024

def response(connection: socket.socket):
    while True:
        #wait for some data to come down the pipe
        data = connection.recv(BUFFER_SIZE)
        if(data):
            connection.sendall(b"+PONG\r\n")

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    #create the connection socket
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
        
    while True:
        #Wait for a connection to accept
        connection_socket, connection_addr = server_socket.accept()
        #create a thread, connect to function with argument
        thread = threading.Thread( target=response, args=(connection_socket,))
        #start the thread
        thread.start()

if __name__ == "__main__":
    main()
