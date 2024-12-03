



# import socket
# import logging
# import threading

# def parse_message(msg):
#     """
#     Parses a Kafka request message and extracts apiKey, apiVersion, and correlationId.
#     """
#     api_key = int.from_bytes(msg[4:6], byteorder="big")  # Extract the apiKey
#     api_version = int.from_bytes(msg[6:8], byteorder="big")
#     correlation_id = int.from_bytes(msg[8:12], byteorder="big")
#     return api_key, api_version, correlation_id



# def construct_response(correlation_id, api_key, api_version):
#     header = correlation_id.to_bytes(4, byteorder="big")
#     error_code = (0).to_bytes(2, byteorder="big")

#     throttle_time_ms = (0).to_bytes(4, byteorder="big")

#     print(f"Received request: api_key={api_key}, api_version={api_version}")

#     # if api_key == 18:  # ApiVersions
#     #     # Construct the ApiVersionsResponse with multiple API keys
#     #     payload = error_code  # Error code

#     api_keys = [
#         {"key": 18, "min_version": 0, "max_version": 4},
#         {"key": 75, "min_version": 0, "max_version": 0}
#     ]

#         print(f"Sending ApiVersions response with {len(api_keys)} keys")
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += throttle_time_ms.to_bytes(4, byteorder="big") 
#         payload += len(api_keys).to_bytes(4, byteorder="big")  # Number of API keys

        

#         for api_info in api_keys:
#             payload += api_info["key"].to_bytes(2, byteorder="big")
#             payload += api_info["min_version"].to_bytes(2, byteorder="big")
#             payload += api_info["max_version"].to_bytes(2, byteorder="big")

#             print(f"API Key: {api_info['key']}, Min Version: {api_info['min_version']}, Max Version: {api_info['max_version']}")

#         payload += (0).to_bytes(1, byteorder="big")


#     elif api_key == 75:  # DescribeTopicPartitions
#         # Construct a DescribeTopicPartitions response
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += int(0).to_bytes(2, byteorder="big")  # Placeholder response
#         logging.debug("Sending DescribeTopicPartitions response")

#     else:
#         # Default error code if the API key is unknown
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += int(0).to_bytes(2, byteorder="big")  # Placeholder version
#         payload += int(4).to_bytes(2, byteorder="big")  # Placeholder flags
#         logging.debug("Sending default error response")

#     response_length = len(header + payload)
#     response = response_length.to_bytes(4, byteorder="big") + header + payload
#     print(f"Constructed response: {response.hex()}")

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
#             # Parse the request and extract api_key, api_version, and correlation_id
#             api_key, api_version, correlation_id = parse_message(request)
#             print(f"Received request: apiKey={api_key}, apiVersion={api_version}, correlationId={correlation_id}")
#             # Construct the response using the extracted api_key
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
#     server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server.bind((host, port))
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


# 





# import socket

# def construct_response(correlation_id):
#     """
#     Constructs a properly formatted ApiVersionsResponse.
#     """
#     # Correlation ID
#     header = correlation_id.to_bytes(4, byteorder="big")

#     # Error code: 0 (no error)
#     error_code = (0).to_bytes(2, byteorder="big")

#     # Throttle time: 0 (no throttling)
#     throttle_time_ms = (0).to_bytes(4, byteorder="big")

#     # API keys
#     api_keys = [
#         {"key": 18, "min_version": 0, "max_version": 4},  # ApiVersions key
#         {"key": 75, "min_version": 0, "max_version": 0},  # Custom key
#     ]
#     num_api_keys = len(api_keys)
#     api_keys_data = b""
#     for api_info in api_keys:
#         api_keys_data += api_info["key"].to_bytes(2, byteorder="big")
#         api_keys_data += api_info["min_version"].to_bytes(2, byteorder="big")
#         api_keys_data += api_info["max_version"].to_bytes(2, byteorder="big")

#     # Tagged fields: Empty for this response
#     tagged_fields = (0).to_bytes(1, byteorder="big")

#     # Construct payload
#     payload = error_code + throttle_time_ms
#     payload += num_api_keys.to_bytes(4, byteorder="big")
#     payload += api_keys_data
#     payload += tagged_fields

#     # Prepend length of the message
#     response_length = len(header + payload)
#     response = response_length.to_bytes(4, byteorder="big") + header + payload
#     return response


# def handle_client(client_socket):
#     """
#     Handles a Kafka client connection, decodes the request, and sends the response.
#     """
#     try:
#         # Receive the request
#         request = client_socket.recv(1024)
#         print(f"[Server] Received request: {request.hex()}")

#         # Decode request (correlation_id is at offset 4–8)
#         correlation_id = int.from_bytes(request[4:8], byteorder="big")
#         print(f"[Server] Decoded correlation_id: {correlation_id}")

#         # Construct and send the response
#         response = construct_response(correlation_id)
#         client_socket.sendall(response)
#         print(f"[Server] Sent response: {response.hex()}")

#     except Exception as e:
#         print(f"[Server] Error: {e}")
#     finally:
#         client_socket.close()


# def start_server(host="localhost", port=9092):
#     """
#     Starts a server to handle Kafka client connections.
#     """
#     server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server_socket.bind((host, port))
#     server_socket.listen(5)
#     print(f"[Server] Listening on {host}:{port}...")

#     try:
#         while True:
#             client_socket, addr = server_socket.accept()
#             print(f"[Server] Client connected from {addr}")
#             handle_client(client_socket)
#     except KeyboardInterrupt:
#         print("\n[Server] Shutting down...")
#     finally:
#         server_socket.close()


# if __name__ == "__main__":
#     start_server()


# import socket  # noqa: F401
# import os
# API_VERSIONS = 18
# FETCH = 1
# def make_body_ApiVersions(data):
#     # error_code
#     body = int(0).to_bytes(2, "big")
#     # Array size INT32
#     # ????????????????
#     # COMPACT_ARRAY: size + 1 como UNSIGNED_VARINT
    
#     # Number of api versions + 1
#     # 4 for
#     #  Include DescribeTopicPartitions in APIVersions #yk1
#     body += int(4).to_bytes(1, "big")
#     # ARRAY: size as INT32, does not work
#     # body += int(1).to_bytes(4, "big")
#     # api version 18, minversion(4), maxversion(>=4) 3 INT16
#     body += (
#         int(18).to_bytes(2, "big")
#         + int(4).to_bytes(2, "big")
#         + int(4).to_bytes(2, "big")
#     )
#     # tagged?? INT32
#     # codecrafters challenge:
#     # The value for this will always be a null byte in this challenge (i.e. no tagged fields are present)
#     body += b"\x00"
#     # api version 1 (Fetch), minversion(4), maxversion(>=16) 3 INT16
#     body += (
#         int(1).to_bytes(2, "big")
#         + int(4).to_bytes(2, "big")
#         + int(16).to_bytes(2, "big")
#     )
#     # tagged?? INT32
#     # codecrafters challenge:
#     # The value for this will always be a null byte in this challenge (i.e. no tagged fields are present)
#     body += b"\x00"
#     #  Include DescribeTopicPartitions in APIVersions #yk1
#     # api version 75, minversion(0>=0), max(version>=0)
#     body += int(75).to_bytes(2) + int(0).to_bytes(2) + int(0).to_bytes(2)
#     body += b"\x00"
#     # ---
#     # throttle_time_ms INT32
#     body += int(0).to_bytes(4, "big")
#     # tagged?? INT32
#     body += b"\x00"
#     return body
# def make_body_Fetch(data):
#     max_wait_ms = data[0:4]
#     min_bytes = data[4:8]
#     max_bytes = data[8:12]
#     isolation_level = data[12]
#     session_id = data[13:17]
#     session_epoch = data[17:21]
#     # [topics] = topic_id [partitions] TAG_BUFFER
#     # size of topics = UNSIGNED_VARINT
#     # NO HAY ???
#     topics_size = data[21]
#     # UUID
#     topic_id = data[22:38]
#     #  ...
#     print("Topics size: ", topics_size)
#     print("Topic id: ", topic_id)
#     # throttle_time_ms
#     body = int(0).to_bytes(4, "big")
#     # error_code
#     body += int(0).to_bytes(2, "big")
#     # session_id
#     # body += session_id
#     body += int(0).to_bytes(4)
#     # RESPONSES-----------------------------
#     # Beware! It seems that uses 1 for 0 topics instead of 0 per Kafka's doc
#     # 0 Responses
#     if topics_size == 0 or topics_size == 1:
#         print("No topic")
#         body += int(0).to_bytes(1, "big")
#     else:
#         # 1 Response
#         body += b"\x02"
#         # Topic
#         body += topic_id
#         # Partitions 1 element
#         body += b"\x02"
#         # Partion index INT32
#         body += b"\x00\x00\x00\x00"
#         # Error code 100 UNKNOWN_TOPIC INT16
#         body += int(100).to_bytes(2)
#         body += int(0).to_bytes(8, byteorder="big")  # high watermark
#         body += int(0).to_bytes(8, byteorder="big")  # last stable offset
#         body += int(0).to_bytes(8, byteorder="big")  # log start offset
#         body += int(0).to_bytes(1, byteorder="big")  # num aborted transactions
#         body += int(0).to_bytes(4, byteorder="big")  # preferred read replica
#         body += int(0).to_bytes(1, byteorder="big")  # num records
#         # TAG_BUFFER Partition
#         body += b"\x00"
#         # TAG_BUFFER Topic
#         body += b"\x00"
#     # TAG_BUFFER Final
#     body += b"\x00"
#     return body
# def handle(data):
#     print("Request:\n", data, "\n")
#     tam = data[0:4]  # length field
#     request_api_key = data[4:6]
#     request_api_version = data[6:8]
#     correlation_id = data[8:12]
#     # Rest of fields ignored !!!
#     # Fetch with an unknown topic
#     client_id_size = data[12:14]
#     isize = int.from_bytes(client_id_size)
#     if isize > 0:
#         client_id = data[14 : 14 + isize]
#         tagged = data[14 + isize]
#     else:
#         client_id = ""
#         tagged = data[14]
#     print(f"client_id = {client_id}")
#     req_index = 14 + isize + 1
#     # client_id
#     # tagged_fields
#     version = int.from_bytes(request_api_version, signed=True)
#     # print(request_api_version, f'version(int) = {version}')
#     api_key = int.from_bytes(request_api_key)
#     if version in [0, 1, 2, 3, 4, 16]:
#         # error code INT16
#         # body = int(0).to_bytes(2, "big")
#         if api_key == API_VERSIONS:
#             print("ApiVersions")
#             body = make_body_ApiVersions(data[12:])
#         elif api_key == FETCH:
#             print("Fetch")
#             body = make_body_Fetch(data[req_index:])
#     else:
#         print(f"Error! version: {version} request_api_version: ", request_api_version)
#         body = int(35).to_bytes(2, "big")
#     # Fetch with an unknown topic #hn6
#     # Espera Response header v1:
#     # correlation_id TAG_BUFFER
#     size = 4 + len(body)
#     if api_key == FETCH:
#         # Response header v2
#         size += 1
#     header = size.to_bytes(4, "big") + correlation_id
#     if api_key == FETCH:
#         # Response header v2
#         header += b"\x00"
#     print(f"Size: {size}")
#     print("Header: ", end="")
#     print(header)
#     print("Body: ")
#     print(body)
#     return header + body
# def main():
#     # You can use print statements as follows for debugging,
#     # they'll be visible when running tests.
#     print("Logs from your program will appear here!")
#     # Uncomment this to pass the first stage
#     #
#     server = socket.create_server(("localhost", 9092), reuse_port=True)
#     while True:
#         client, addr = server.accept()  # wait for client
#         pid = os.fork()
#         if pid == 0:
#             # Child
#             server.close()
#             while True:
#                 r = client.recv(1024)
#                 client.sendall(handle(r))
#         print(f"New process: {pid}")
#         client.close()
# if __name__ == "__main__":
#     main()

import socket
import threading

DESCRIBE_TOPIC_PARTITIONS = 75  # API key for DescribeTopicPartitions
UNKNOWN_TOPIC_ERROR = 3  # Error code for UNKNOWN_TOPIC_OR_PARTITION


def make_body_DescribeTopicPartitions(topic_name, topic_id=b"\x00" * 16):
    body = int(0).to_bytes(4, "big")  # throttle_time_ms
    body += int(UNKNOWN_TOPIC_ERROR).to_bytes(2, "big")  # error_code
    body += int(1).to_bytes(4, "big")  # array size (1 topic)
    body += len(topic_name).to_bytes(2, "big") + topic_name.encode()  # topic_name
    body += topic_id  # topic_id
    body += int(0).to_bytes(4, "big")  # partitions array size (0 partitions)
    body += b"\x00"  # TAG_BUFFER Topic
    return body


def handle(data):
    print("Request:\n", data, "\n")
    request_api_key = int.from_bytes(data[4:6], "big")
    correlation_id = data[8:12]

    body = b""
    if request_api_key == DESCRIBE_TOPIC_PARTITIONS:
        # Extract topic_name from request
        topic_name_size = int.from_bytes(data[21:23], "big")
        topic_name = data[23:23 + topic_name_size].decode()
        body = make_body_DescribeTopicPartitions(topic_name)
    else:
        print("Unsupported request type.")
        return b""

    header = (4 + len(body)).to_bytes(4, "big") + correlation_id
    return header + body


def client_handler(client):
    try:
        while (data := client.recv(1024)):
            client.sendall(handle(data))
    finally:
        client.close()


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    try:
        while True:
            client, _ = server.accept()
            threading.Thread(target=client_handler, args=(client,), daemon=True).start()
    finally:
        server.close()


if __name__ == "__main__":
    main()
