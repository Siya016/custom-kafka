


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

# def construct_response(correlation_id, api_key, api_version):
#     header = correlation_id.to_bytes(4, byteorder="big")
#     error_code = 0  # Error code: 0 indicates no error
#     if api_key == 18:  # APIVersions
#         # Construct the APIVersionsResponse
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += len([18, 75]).to_bytes(1, byteorder="big")  # Number of API keys
#         api_key_18 = 18
#         payload += api_key_18.to_bytes(2, byteorder="big")  # API Key 18
#         payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
#         payload += int(4).to_bytes(2, byteorder="big")  # MaxVersion
#         payload += int(75).to_bytes(2, byteorder="big")  # API Key 75 (DescribeTopicPartitions)
#         payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
#         payload += int(0).to_bytes(2, byteorder="big")  # MaxVersion
#     elif api_key == 75:  # DescribeTopicPartitions
#         # Construct a DescribeTopicPartitions response
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += int(0).to_bytes(2, byteorder="big")  # Placeholder response for DescribeTopicPartitions
        
#     else:
#         # Default error code if the API key is unknown
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += int(0).to_bytes(2, byteorder="big")  # Placeholder version
#         payload += int(4).to_bytes(2, byteorder="big")  # Placeholder flags
    
#     # Combine header and payload
#     response_length = len(header + payload)
#     response = response_length.to_bytes(4, byteorder="big") + header + payload
#     return response


def construct_response(correlation_id, api_key, api_version):
    header = correlation_id.to_bytes(4, byteorder="big")
    error_code = 0  # Error code: 0 indicates no error

    if api_key == 18:  # APIVersions
        # Construct the APIVersionsResponse with multiple API keys
        payload = error_code.to_bytes(2, byteorder="big")  # Error code
        
        # Include two API keys instead of one
        payload += len([18, 75]).to_bytes(1, byteorder="big")  # Number of API keys

        # First API Key (APIVersions)
        payload += int(18).to_bytes(2, byteorder="big")  # API Key 18
        payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
        payload += int(4).to_bytes(2, byteorder="big")  # MaxVersion

        # Second API Key (DescribeTopicPartitions)
        payload += int(75).to_bytes(2, byteorder="big")  # API Key 75
        payload += int(0).to_bytes(2, byteorder="big")  # MinVersion
        payload += int(0).to_bytes(2, byteorder="big")  # MaxVersion

    elif api_key == 75:  # DescribeTopicPartitions
        # Construct a DescribeTopicPartitions response
        payload = error_code.to_bytes(2, byteorder="big")  # Error code
        payload += int(0).to_bytes(2, byteorder="big")  # Placeholder response
    else:
        # Default error code if the API key is unknown
        payload = error_code.to_bytes(2, byteorder="big")  # Error code
        payload += int(0).to_bytes(2, byteorder="big")  # Placeholder version
        payload += int(4).to_bytes(2, byteorder="big")  # Placeholder flags

    # Combine header and payload
    response_length = len(header + payload)
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
