import socket
import threading

BUFFER_SIZE = 1024
data_storage = {}


def response(connection: socket.socket):
    while True:
        print("HELLO")
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
                if len(data_array)>3:
                    if 'px' == str.lower(data_array[3]):
                        time_num = int(data_array[4])
                        #Timer, threading command
                        threading.Timer(time_num/1000, data_storage.pop, args=[set_key]).start()
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

            if msg_type == "RPUSH":
                set_key = data_array[1]
                for i in range(2, len(data_array)):
                    set_data = data_array[i]
                    if set_key in data_storage.keys():
                        data_storage[set_key].append(set_data)
                    else:
                        data_storage[set_key] = [set_data]
                list_len = len(data_storage[set_key])
                return_string = ':+' + str(list_len) + '\r\n'
                connection.sendall( return_string.encode() )

            if msg_type == "LRANGE":

                set_key = data_array[1]
                start_ind = int(data_array[2])
                end_ind = int(data_array[3])

                if set_key not in data_storage.keys():
                    return_string = "*0\r\n"
                    connection.sendall( return_string.encode() )
                    return
                
                retrieved_list = data_storage[set_key]

                if start_ind>=len(retrieved_list):
                    return_string = "*0\r\n"
                    connection.sendall( return_string.encode() )
                    return

                if end_ind >= len(retrieved_list):
                    end_ind = len(retrieved_list)-1

                if start_ind>end_ind:
                    return_string = "*0\r\n"
                    connection.sendall( return_string.encode() )
                    return


                length_array = (end_ind-start_ind)+1
                
                return_string = "*"+str(length_array)+"\r\n"
                for i in range(start_ind, end_ind+1):
                    length_entry = len( retrieved_list[i].encode() )
                    return_string = return_string + "$"+str(length_entry)+"\r\n"+str(retrieved_list[i])+"\r\n"
                
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
            bulk_bytes_start = bulk_len_seg + 2 

                             #start index + len = end index
            bulk_bytes_end = bulk_bytes_start + bulk_len
            
            #decode to get the string, append to data string array
            data_str_array.append(data[bulk_bytes_start:bulk_bytes_end].decode())

            #advance past the \r\n at the end of segment
            current_pos = bulk_bytes_end + 2

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
        #Thread, threading command
        thread = threading.Thread( target=response, args=(connection_socket,) )
        #start the thread
        thread.start()

if __name__ == "__main__":
    main()
