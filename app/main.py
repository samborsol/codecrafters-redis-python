import socket
import threading

BUFFER_SIZE = 1024
data_storage = {}


def response(connection: socket.socket):
    while True:
        #wait for some data to come down the pipe
        data = connection.recv(BUFFER_SIZE)
        if(data):
            data_array = parser(data)
            
            msg_type = data_array[0]

            if msg_type == "PING":
                #print(msg_type)
                connection.sendall(b"+PONG\r\n")
            
            if msg_type == "ECHO":
                #print(msg_type)
                echo_msg = data_array[1]
                return_string = '+' + echo_msg + '\r\n'
                connection.sendall( return_string.encode() )

            if msg_type == "SET":
                #print(msg_type)
                set_key = data_array[1]
                set_data= data_array[2]
                data_storage[set_key] = set_data
                return_string = '+OK\r\n'
                connection.sendall( return_string.encode() )

            if msg_type == "GET":
                #print(msg_type)
                set_key = data_array[1]

                if set_key in data_storage.keys(): 
                    set_data = data_storage[set_key]
                    return_string = '+' + set_data + '\r\n'
                else:
                    return_string = '$-1\r\n'
                connection.sendall( return_string.encode() )


def parser(data: bytes):
    if not data.startswith(b'*'):
        raise ValueError("Not a RESP array")
    

    #bytes_object.find(sub, start, end)
    right_index = data.find(b'\r\n')
    if right_index == -1:
        raise ValueError("Invalid RESP array format")
    num_elements_str = data[1:right_index].decode()
    num_elements = int(num_elements_str)

    current_pos = right_index + 2  # Move past the \r\n
    data_str_array = []

    for i in range(num_elements):

        #start at the dollar sign, beginning of the segment, next byte is how many bytes
        if data[current_pos:current_pos+1] == b'$':

            #working with bytes instead of string
            #an index
            bulk_len_seg = data.find(b'\r\n', current_pos)
            #decode the byte that tells you how many bytes
            bulk_len = int(data[current_pos+1:bulk_len_seg].decode())

                                #advance past another \r\n pair of bytes
            bulk_string_start = bulk_len_seg + 2 

                             #start index + len = end index
            bulk_string_end = bulk_string_start + bulk_len
            
            #decode to get the string, append to data string array
            data_str_array.append(data[bulk_string_start:bulk_string_end].decode())

            #advance past the \r\n at the end of segment
            current_pos = bulk_string_end + 2

    return data_str_array

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    #create the connection socket
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
        
    while True:
        #Wait for a connection to accept
        connection_socket, connection_addr = server_socket.accept()
        #create a thread, connect to function with argument
        thread = threading.Thread( target=response, args=(connection_socket,) )
        #start the thread
        thread.start()

if __name__ == "__main__":
    main()
