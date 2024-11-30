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


# import socket
# import threading

# def parse_message(msg):
#     """
#     Parses a Kafka request message and extracts apiKey, apiVersion, and correlationId.
#     """
#     api_key = int.from_bytes(msg[4:6], byteorder="big")
#     api_version = int.from_bytes(msg[6:8], byteorder="big")
#     correlation_id = int.from_bytes(msg[8:12], byteorder="big")
#     return api_key, api_version, correlation_id


# def construct_response(correlation_id, api_version):
#     """
#     Constructs a Kafka ApiVersionsResponse (version 4).
    
#     Parameters:
#     - correlation_id (int): The correlation ID from the request.
#     - api_version (int): The API version of the request.

#     Returns:
#     - bytes: The constructed ApiVersionsResponse.
#     """
#     # Create the header (correlation ID)
#     header = correlation_id.to_bytes(4, byteorder="big")

#     # Default response error code (valid for API versions 0-4)
#     valid_api_versions = [0, 1, 2, 3, 4]
#     error_code = 0 if api_version in valid_api_versions else 35

#     # Define supported API keys and their version ranges
#     supported_api_keys = {
#         18: (0, 4),  # ApiVersions: MinVersion = 0, MaxVersion = 4
#         75: (0, 0),  # DescribeTopicPartitions: MinVersion = 0, MaxVersion = 0
#     }

#     # Construct the payload fields
#     throttle_time_ms = (0).to_bytes(4, byteorder="big")  # No throttling
#     error_code_bytes = error_code.to_bytes(2, byteorder="big")
#     num_api_keys = len(supported_api_keys)
#     num_api_keys_bytes = num_api_keys.to_bytes(4, byteorder="big")

#     # Build API keys section
#     api_keys_payload = b""
#     for api_key, (min_version, max_version) in supported_api_keys.items():
#         api_keys_payload += api_key.to_bytes(2, byteorder="big")  # API Key
#         api_keys_payload += min_version.to_bytes(2, byteorder="big")  # Min Version
#         api_keys_payload += max_version.to_bytes(2, byteorder="big")  # Max Version

#     # Tagged fields (1 byte, empty)
#     tagged_fields = (0).to_bytes(1, byteorder="big")

#     # Combine all parts of the response body
#     response_body = (
#         throttle_time_ms
#         + error_code_bytes
#         + num_api_keys_bytes
#         + api_keys_payload
#         + tagged_fields
#     )

#     # Combine header and body
#     response = header + response_body

#     # Prepend the response length (4 bytes)
#     response_length = len(response)
#     full_response = response_length.to_bytes(4, byteorder="big") + response

#     return full_response




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




# import socket
# import threading


# def parse_message(msg):
#     """
#     Parses a Kafka request message and extracts apiKey, apiVersion, and correlationId.
#     """
#     api_key = int.from_bytes(msg[4:6], byteorder="big")
#     api_version = int.from_bytes(msg[6:8], byteorder="big")
#     correlation_id = int.from_bytes(msg[8:12], byteorder="big")
#     return api_key, api_version, correlation_id


# def construct_response(correlation_id, api_version):
#     """
#     Constructs a Kafka ApiVersionsResponse (version 4).

#     Parameters:
#     - correlation_id (int): The correlation ID from the request.
#     - api_version (int): The API version of the request.

#     Returns:
#     - bytes: The constructed ApiVersionsResponse.
#     """
#     # Create the header (correlation ID)
#     header = correlation_id.to_bytes(4, byteorder="big")

#     # Define supported API keys and their version ranges
#     supported_api_keys = {
#         18: (0, 4),  # ApiVersions: MinVersion = 0, MaxVersion = 4
#         75: (0, 0),  # DescribeTopicPartitions: MinVersion = 0, MaxVersion = 0
#     }

#     # Determine error code (0 = no error, 35 = UNSUPPORTED_VERSION)
#     error_code = 0 if api_version <= 4 else 35

#     # Construct response fields
#     # throttle_time_ms = (0).to_bytes(4, byteorder="big")  # No throttling
#     error_code_bytes = error_code.to_bytes(2, byteorder="big")
#     num_api_keys = len(supported_api_keys)
#     num_api_keys_bytes = num_api_keys.to_bytes(4, byteorder="big")

#     # Build API keys section
#     api_keys_payload = b""
#     for api_key, (min_version, max_version) in supported_api_keys.items():
#         api_keys_payload += api_key.to_bytes(2, byteorder="big")  # API Key
#         api_keys_payload += min_version.to_bytes(2, byteorder="big")  # Min Version
#         api_keys_payload += max_version.to_bytes(2, byteorder="big")  # Max Version

#     # Tagged fields (1 byte, empty)
#     tagged_fields = b"\x00\x00"

#     # Combine all parts of the response body
#     response_body = (
#         # throttle_time_ms
#          error_code_bytes
#         + num_api_keys_bytes
#         + api_keys_payload
#          + tagged_fields
#     )

#     # Combine header and body
#     response = header + response_body

#     # Prepend the response length (4 bytes)
#     response_length = len(response)
#     full_response = response_length.to_bytes(4, byteorder="big") + response

#     return full_response

# def handle_client(client, addr):
#     """
#     Handles a single client connection, processing one or more requests.
#     """
#     print(f"Handling client from {addr}")
#     try:
#         while True:
#             # Read the message length (first 4 bytes)
#             length_bytes = client.recv(4)
#             if not length_bytes:
#                 break  # Client disconnected

#             # Extract message length and read the entire message
#             msg_length = int.from_bytes(length_bytes, byteorder="big")
#             request = client.recv(msg_length)

#             # Parse the request
#             api_key, api_version, correlation_id = parse_message(request)

#             # Debugging: Show parsed values
#             print(
#                 f"Received request - apiKey: {api_key}, apiVersion: {api_version}, correlationId: {correlation_id}"
#             )

#             # Construct the response
#             response = construct_response(correlation_id, api_version)

#             # Send the response to the client
#             client.sendall(response)
#     except ConnectionResetError:
#         print(f"Connection with {addr} reset by client.")
#     except Exception as e:
#         print(f"Error while handling client {addr}: {e}")
#     finally:
#         client.close()
#         print(f"Connection with {addr} closed.")


# def start_server(host="localhost", port=9092):
#     """
#     Starts the Kafka-like server on the specified host and port.
#     """
#     print(f"Starting server on {host}:{port}...")
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
# # ne


# import socket
# import threading

# def parse_message(msg):
#     """
#     Parses a Kafka request message and extracts apiKey, apiVersion, and correlationId.
#     """
#     api_key = int.from_bytes(msg[4:6], byteorder="big")
#     api_version = int.from_bytes(msg[6:8], byteorder="big")
#     correlation_id = int.from_bytes(msg[8:12], byteorder="big")
#     return api_key, api_version, correlation_id

# def construct_response(correlation_id, api_key, api_version):
#     """
#     Constructs a Kafka response message based on the request details.
#     """
#     # Create the header (correlation ID) - 4 bytes for the correlation_id
#     header = correlation_id.to_bytes(4, byteorder="big")

#     # Error code: 0 indicates no error
#     error_code = 0

#     # Throttle time in milliseconds
#     throttle_time_ms = 0

#     # Define API keys and their versions
#     api_entries = [
#         {
#             "api_key": 18,  # ApiVersions
#             "min_version": 0,
#             "max_version": 4,
#         },
#         {
#             "api_key": 75,  # DescribeTopicPartitions
#             "min_version": 0,
#             "max_version": 0,
#         },
#     ]

#     # Construct the response payload
#     payload = bytearray()

#     # Add the error code (2 bytes)
#     payload += error_code.to_bytes(2, byteorder="big")

#     # Add number of API keys (4 bytes)
    
#     num_api_keys = len(api_entries)
#     payload += num_api_keys.to_bytes(4, byteorder="big")

#     # Add each API key entry
#     for entry in api_entries:
#         # API key (2 bytes), min_version (2 bytes), max_version (2 bytes)
#         payload += entry["api_key"].to_bytes(2, byteorder="big")  # API key
#         payload += entry["min_version"].to_bytes(2, byteorder="big")  # MinVersion
#         payload += entry["max_version"].to_bytes(2, byteorder="big")  # MaxVersion

#     # Add throttle time (4 bytes)
#     payload += throttle_time_ms.to_bytes(4, byteorder="big")

#     # The length of the entire response (header + payload)
#     response_length = 4 + len(payload)  # 4 bytes for header
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
#             print(f"Received request: apiKey={api_key}, apiVersion={api_version}, correlationId={correlation_id}")
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
#     print(f"Starting server on {host}:{port}...")
#     server = socket.create_server((host, port), reuse_port=True)
#     server.listen()  # Enable listening for incoming connections
#     while True:
#         # Accept a client connection
#         client, addr = server.accept()
#         print(f"Client connected from {addr}")
#         # Handle the client's requests in a new thread
#         thread = threading.Thread(
#             target=handle_client, args=(client, addr), daemon=True
#         )
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
    api_key = int.from_bytes(msg[4:6], byteorder="big")  # Extract the apiKey
    api_version = int.from_bytes(msg[6:8], byteorder="big")
    correlation_id = int.from_bytes(msg[8:12], byteorder="big")
    return api_key, api_version, correlation_id

def construct_response(correlation_id, api_key, api_version):
    """
    Constructs a Kafka response message based on the request details.
    """
    # Create the header (correlation ID) - 4 bytes for the correlation_id
    header = correlation_id.to_bytes(4, byteorder="big")

    # Error code: 0 indicates no error
    error_code = 0

    # Throttle time in milliseconds
    throttle_time_ms = 0

    # Define API keys and their versions
    api_entries = [
        {
            "api_key": 18,  # ApiVersions
            "min_version": 0,
            "max_version": 4,
        },
        {
            "api_key": 75,  # DescribeTopicPartitions
            "min_version": 0,
            "max_version": 0,
        },
    ]

    # Filter the API entry that matches the api_key
    matched_entry = next((entry for entry in api_entries if entry["api_key"] == api_key), None)

    if matched_entry is None:
        # If no matching API key is found, return an error response or handle accordingly
        print(f"No matching API entry found for apiKey {api_key}")
        error_code = 1  # Set an error code for unknown apiKey
        matched_entry = {"api_key": api_key, "min_version": 0, "max_version": 0}

    # Construct the response payload
    payload = bytearray()

    # Add the error code (2 bytes)
    payload += error_code.to_bytes(2, byteorder="big")

    # Add number of API keys (4 bytes)
    num_api_keys = 1  # Only one API key entry is returned (either matched or default)
    payload += num_api_keys.to_bytes(4, byteorder="big")

    # Add the matching API key entry
    payload += matched_entry["api_key"].to_bytes(2, byteorder="big")  # API key
    payload += matched_entry["min_version"].to_bytes(2, byteorder="big")  # MinVersion
    payload += matched_entry["max_version"].to_bytes(2, byteorder="big")  # MaxVersion

    # Add throttle time (4 bytes)
    payload += throttle_time_ms.to_bytes(4, byteorder="big")

    # The length of the entire response (header + payload)
    response_length = 4 + len(payload)  # 4 bytes for header
    response = response_length.to_bytes(4, byteorder="big") + header + payload

    return response

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
            # Parse the request and extract api_key, api_version, and correlation_id
            api_key, api_version, correlation_id = parse_message(request)
            print(f"Received request: apiKey={api_key}, apiVersion={api_version}, correlationId={correlation_id}")
            # Construct the response using the extracted api_key
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
    print(f"Starting server on {host}:{port}...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()  # Enable listening for incoming connections
    while True:
        # Accept a client connection
        client, addr = server.accept()
        print(f"Client connected from {addr}")
        # Handle the client's requests in a new thread
        thread = threading.Thread(
            target=handle_client, args=(client, addr), daemon=True
        )
        thread.start()

def main():
    """
    Entry point for the program.
    """
    start_server()

if __name__ == "__main__":
    main()
