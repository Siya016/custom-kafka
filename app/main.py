# from socket import create_server

# class Message:
#     header: bytes
#     body: bytes

#     def __init__(self, header: bytes, body: bytes):
#         self.size = len(header + body)
#         self.header = header
#         self.request_api_key = int.from_bytes(header[:2], byteorder="big")
#         self.request_api_version = int.from_bytes(header[2:4], byteorder="big")
#         self.correlation_id = int.from_bytes(header[4:8], byteorder="big")
#         self.client_id = int.from_bytes(header[8:], byteorder="big")
#         self.tagged_fields = ""  # No tagged fields for us
#         self.body = body

#     def to_bytes(self):
#         return self.size.to_bytes(4, byteorder="big") + self.header + self.body

#     def __repr__(self):
#         return (
#             f"{self.size} | "
#             f"{self.request_api_key} - {self.request_api_version} - {self.correlation_id} - {self.client_id} | "
#             f"{self.body}"
#         )

# def main():
#     server = create_server(("localhost", 9092), reuse_port=True)
#     socket, address = server.accept()  # wait for client
#     print(f"Client connected: {address}")

#     # Receive data from the client
#     data = socket.recv(1024)
#     print(f"Received data: {data.hex()}")

#     # Parse the received request (skip first 4 bytes and initialize the Message object)
#     request = Message(data[4:], b"")
#     print(f"Received request: {request}")

#     # Check if the API version is supported (error code 0 for valid versions, 35 for unsupported)
#     error_code = 0 if request.request_api_version in [0, 1, 2, 3, 4] else 35

#     # Construct the response message
#     # Kafka ApiVersion V3 Response | Ref: https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
#     # error_code [api_keys] throttle_time_ms TAG_BUFFER
#     response_header = request.correlation_id.to_bytes(4, byteorder="big")

#     # ApiVersions response body (including error_code, api_keys, throttle_time_ms)
#     # response_body = (
#     #     error_code.to_bytes(2, byteorder="big") +  # error_code: 2 bytes
#     #     int(1 + 1).to_bytes(1, byteorder="big") +  # num_api_keys: 1 byte (1 key + 1 entry for API key 18)
#     #     int(18).to_bytes(2, byteorder="big") +  # api_key: 18 (2 bytes)
#     #     int(4).to_bytes(2, byteorder="big") +  # min_version: 4 (2 bytes)
#     #     int(4).to_bytes(2, byteorder="big") +  # max_version: 4 (2 bytes)
#     #     int(0).to_bytes(2, byteorder="big") +  # TAG_BUFFER: 0 (2 bytes)
#     #     int(0).to_bytes(4, byteorder="big")  # throttle_time_ms: 0 (4 bytes)
#     # )

#     response_body = (
#         error_code.to_bytes(2, byteorder="big") +  # error_code: 2 bytes
#         int(1).to_bytes(1, byteorder="big") +  # num_api_keys: 1 byte (1 key)
#         int(18).to_bytes(2, byteorder="big") +  # api_key: 18 (2 bytes)
#         int(4).to_bytes(2, byteorder="big") +  # min_version: 4 (2 bytes)
#         int(4).to_bytes(2, byteorder="big") +  # max_version: 4 (2 bytes)
#         int(0).to_bytes(4, byteorder="big")  # throttle_time_ms: 0 (4 bytes)
#     )

# # Ensure the size of the message includes both header and body
#     response_size = len(response_header + response_body)

#     # Construct the full response message
#     while True:
#         message = Message(response_header, response_body)
#         print(f"Sending message: {message.to_bytes().hex()}")
#         print(f"Sending message with size: {response_size} bytes")
#         print(f"Full response message: {message.to_bytes().hex()}")

#         # Send the response back to the client
#         socket.sendall(message.to_bytes())

#         # Close the connection
#         # socket.close()
#         # print("Connection closed.")

# if __name__ == "__main__":
#     main()



# 


# import socket

# def parse_message(msg):
#     """
#     Parses a Kafka request message and extracts apiKey, apiVersion, and coRelationId.
#     """
#     api_key = int.from_bytes(msg[4:6], byteorder="big")
#     api_version = int.from_bytes(msg[6:8], byteorder="big")
#     correlation_id = int.from_bytes(msg[8:12], byteorder="big")
#     return api_key, api_version, correlation_id

# def construct_response(correlation_id, api_key, api_version):
#     """
#     Constructs a Kafka response message based on the request details.
#     """
#     # Create the header (correlation ID)
#     header = correlation_id.to_bytes(4, byteorder="big")
    
#     # Determine the payload
#     valid_api_versions = [0, 1, 2, 3, 4]
#     error_code = 0 if api_version in valid_api_versions else 35
#     payload = error_code.to_bytes(2, byteorder="big")  # Error code
#     payload += int(1 + 1).to_bytes(1, byteorder="big")  # Number of API keys (fixed)
#     payload += api_key.to_bytes(2, byteorder="big")     # Echoed apiKey
#     payload += int(0).to_bytes(2, byteorder="big")      # Placeholder version
#     payload += int(4).to_bytes(2, byteorder="big")      # Placeholder flags
#     payload += int(0).to_bytes(2, byteorder="big")      # Additional placeholder
#     payload += int(0).to_bytes(4, byteorder="big")      # Final placeholder
    
#     # Combine header and payload
#     response_length = len(header + payload)
#     response = response_length.to_bytes(4, byteorder="big") + header + payload
#     return response

# def handle_client(client):
#     """
#     Handles a single client connection, processing one or more requests.
#     """
#     request = client.recv(1024)
#     if not request:
#         return
    
#     # Parse the request
#     api_key, api_version, correlation_id = parse_message(request)
    
#     # Construct the response
#     response = construct_response(correlation_id, api_key, api_version)
    
#     # Send the response to the client
#     client.sendall(response)

# def start_server(host="localhost", port=9092):
#     """
#     Starts the Kafka-like server on the specified host and port.
#     """
#     print("Starting server...")
#     server = socket.create_server((host, port), reuse_port=True)
    
#     while True:
#         # Accept a client connection
#         client, addr = server.accept()
#         print(f"Client connected from {addr}")
        
#         # Handle the client's requests
#         try:
#             while True:
#                 handle_client(client)
#         except ConnectionResetError:
#             print(f"Connection with {addr} closed.")
#         finally:
#             client.close()

# def main():
#     """
#     Entry point for the program.
#     """
#     start_server()

# if __name__ == "__main__":
#     main()






# import socket
# import threading

# def parse_message(msg):
#     """
#     Parses a Kafka request message and extracts apiKey, apiVersion, and coRelationId.
#     """
#     api_key = int.from_bytes(msg[4:6], byteorder="big")
#     api_version = int.from_bytes(msg[6:8], byteorder="big")
#     correlation_id = int.from_bytes(msg[8:12], byteorder="big")
#     return api_key, api_version, correlation_id

# def construct_response(correlation_id, api_key, api_version):
#     """
#     Constructs a Kafka response message based on the request details.
#     """
#     # Create the header (correlation ID)
#     header = correlation_id.to_bytes(4, byteorder="big")
    
#     # Determine the payload
#     valid_api_versions = [0, 1, 2, 3, 4]
#     error_code = 0 if api_version in valid_api_versions else 35
#     payload = error_code.to_bytes(2, byteorder="big")  # Error code
#     payload += int(1 + 1).to_bytes(1, byteorder="big")  # Number of API keys (fixed)
#     payload += api_key.to_bytes(2, byteorder="big")     # Echoed apiKey
#     payload += int(0).to_bytes(2, byteorder="big")      # Placeholder version
#     payload += int(4).to_bytes(2, byteorder="big")      # Placeholder flags
#     payload += int(0).to_bytes(2, byteorder="big")      # Additional placeholder
#     payload += int(0).to_bytes(4, byteorder="big")      # Final placeholder


    
    
#     # Combine header and payload
#     response_length = len(header + payload)
#     response = response_length.to_bytes(4, byteorder="big") + header + payload
#     return response

# def handle_client(client, addr):
#     """
#     Handles a single client connection, processing one or more requests.
#     """
#     print(f"Handling client from {addr}")
#     try:
#         while True:
#             request = client.recv(1024)
#             if not request:
#                 break  # Client disconnected
            
#             # Parse the request
#             api_key, api_version, correlation_id = parse_message(request)
            
#             # Construct the response
#             response = construct_response(correlation_id, api_key, api_version)
            
#             # Send the response to the client
#             client.sendall(response)
#     except ConnectionResetError:
#         print(f"Connection with {addr} reset by client.")
#     finally:
#         client.close()
#         print(f"Connection with {addr} closed.")

# def start_server(host="localhost", port=9092):
#     """
#     Starts the Kafka-like server on the specified host and port.
#     """
#     print("Starting server...")
#     server = socket.create_server((host, port), reuse_port=True)
#     server.listen()  # Enable listening for incoming connections
    
#     while True:
#         # Accept a client connection
#         client, addr = server.accept()
#         print(f"Client connected from {addr}")
        
#         # Handle the client's requests in a new thread
#         thread = threading.Thread(target=handle_client, args=(client, addr), daemon=True)
#         thread.start()

# def main():
#     """
#     Entry point for the program.
#     """
#     start_server()

# if __name__ == "__main__":
#     main()


import socket
import threading

def parse_message(msg):
    """
    Parses a Kafka request message and extracts apiKey, apiVersion, and correlationId.
    """
    api_key = int.from_bytes(msg[4:6], byteorder="big")
    api_version = int.from_bytes(msg[6:8], byteorder="big")
    correlation_id = int.from_bytes(msg[8:12], byteorder="big")
    return api_key, api_version, correlation_id


def construct_response(correlation_id, api_key, api_version):
    """
    Constructs a Kafka response message based on the request details.
    Dynamically handles the specified API key and adds logic for 
    APIVersions (API key 18) and DescribeTopicPartitions (API key 75).
    """
    # Create the header (correlation ID)
    header = correlation_id.to_bytes(4, byteorder="big")

    # Default response error code (valid for API versions 0-4)
    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = 0 if api_version in valid_api_versions else 35
    payload = error_code.to_bytes(2, byteorder="big")  # Error code

    # Supported API keys and their version ranges
    supported_api_keys = {
        18: (0, 4),  # APIVersions
        75: (0, 0),  # DescribeTopicPartitions
    }

    # Validate the provided API key
    if api_key not in supported_api_keys:
        raise ValueError(f"Unsupported API key: {api_key}")

    # Include only the requested API key in the response
    min_version, max_version = supported_api_keys[api_key]
    payload += (1).to_bytes(1, byteorder="big")  # Number of API keys (always 1 in this case)
    payload += api_key.to_bytes(2, byteorder="big")  # API Key
    payload += min_version.to_bytes(2, byteorder="big")  # MinVersion
    payload += max_version.to_bytes(2, byteorder="big")  # MaxVersion

    # Combine header and payload
    response_length = len(header + payload)
    response = response_length.to_bytes(4, byteorder="big") + header + payload
    return response



# def construct_response(correlation_id, api_key, api_version):
#     """
#     Constructs a Kafka response message based on the request details.
#     Adds logic for APIVersions (API key 18) and DescribeTopicPartitions (API key 75).
#     """
#     # Create the header (correlation ID)
#     header = correlation_id.to_bytes(4, byteorder="big")
    
#     # Default response error code (valid for API versions 0-4)
#     valid_api_versions = [0, 1, 2, 3, 4]
#     error_code = 0 if api_version in valid_api_versions else 35
#     payload = error_code.to_bytes(2, byteorder="big")  # Error code
#     api_keys = [18, 75]  # Both API keys will be included
#     payload += len(api_keys).to_bytes(1, byteorder="big")  # Number of API keys (2)
    
#     # API Key 18 (APIVersions)
#     payload += api_keys[0].to_bytes(2, byteorder="big")  # API Key 18
#     payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
#     payload += int(4).to_bytes(2, byteorder="big")  # MaxVersion
    
#     # API Key 75 (DescribeTopicPartitions)
#     payload += api_keys[1].to_bytes(2, byteorder="big")  # API Key 75
#     payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
#     payload += int(0).to_bytes(2, byteorder="big")  # MaxVersion
    
#     # Combine header and payload
#     response_length = len(header + payload)
#     response = response_length.to_bytes(4, byteorder="big") + header + payload
#     return response


     
    
   # Construct the payload based on api_key
    # if api_key == 18:  # APIVersions
    #     # Construct the APIVersionsResponse
    #     payload = error_code.to_bytes(2, byteorder="big")  # Error code
    #     payload += len([18, 75]).to_bytes(1, byteorder="big")  # Number of API keys
    #     api_key_18 = 18
    #     payload += api_key_18.to_bytes(2, byteorder="big")  # API Key 18
    #     payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
    #     payload += int(4).to_bytes(2, byteorder="big")  # MaxVersion
    #     payload += int(75).to_bytes(2, byteorder="big")  # API Key 75 (DescribeTopicPartitions)
    #     payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
    #     payload += int(0).to_bytes(2, byteorder="big")  # MaxVersion
    # elif api_key == 75:  # DescribeTopicPartitions
    #     # Construct a DescribeTopicPartitions response
    #     payload = error_code.to_bytes(2, byteorder="big")  # Error code
    #     payload += int(0).to_bytes(2, byteorder="big")  # Placeholder response for DescribeTopicPartitions
        
    # else:
    #     # Default error code if the API key is unknown
    #     payload = error_code.to_bytes(2, byteorder="big")  # Error code
    #     payload += int(0).to_bytes(2, byteorder="big")  # Placeholder version
    #     payload += int(4).to_bytes(2, byteorder="big")  # Placeholder flags
    
    # # Combine header and payload
    # response_length = len(header + payload)
    # response = response_length.to_bytes(4, byteorder="big") + header + payload
    # return response

    

def handle_client(client, addr):
    """
    Handles a single client connection, processing one or more requests.
    """
    print(f"Handling client from {addr}")
    try:
        while True:
            request = client.recv(1024)
            if not request:
                break  # Client disconnected
            
            # Parse the request
            api_key, api_version, correlation_id = parse_message(request)
            
            # Construct the response
            response = construct_response(correlation_id, api_key, api_version)
            
            # Send the response to the client
            client.sendall(response)
    except ConnectionResetError:
        print(f"Connection with {addr} reset by client.")
    finally:
        client.close()
        print(f"Connection with {addr} closed.")

def start_server(host="localhost", port=9092):
    """
    Starts the Kafka-like server on the specified host and port.
    """
    print("Starting server...")
    server = socket.create_server((host, port), reuse_port=True)
    server.listen()  # Enable listening for incoming connections
    
    while True:
        # Accept a client connection
        client, addr = server.accept()
        print(f"Client connected from {addr}")
        
        # Handle the client's requests in a new thread
        thread = threading.Thread(target=handle_client, args=(client, addr), daemon=True)
        thread.start()

def main():
    """
    Entry point for the program.
    """
    start_server()

if __name__ == "__main__":
    main()


