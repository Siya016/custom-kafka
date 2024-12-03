



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


import socket
import struct
import threading

# Define the necessary classes and methods locally

class RequestHeaderV0:
    def __init__(self, correlation_id, request_api_version):
        self.correlation_id = correlation_id
        self.request_api_version = request_api_version

    @classmethod
    def from_bytes(cls, data: bytes):
        # Unpack the first 8 bytes into correlation_id and request_api_version
        correlation_id, request_api_version = struct.unpack(">I4xI", data)
        return cls(correlation_id, request_api_version)


class ResponseHeaderV0:
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id

    def to_bytes(self):
        return struct.pack(">I", self.correlation_id)


class APIVersionsResponseBodyV4:
    def __init__(self, error_code, api_keys=None, throttle_time_ms=0):
        self.error_code = error_code
        self.api_keys = api_keys or []
        self.throttle_time_ms = throttle_time_ms

    def to_bytes(self):
        api_keys_count = len(self.api_keys)
        api_keys_bytes = b"".join(
            struct.pack(">hBB", key, min_version, max_version)
            for key, min_version, max_version in self.api_keys
        )
        return (
            struct.pack(">i", self.error_code)
            + struct.pack(">i", self.throttle_time_ms)
            + struct.pack(">i", api_keys_count)
            + api_keys_bytes
        )


class Response:
    def __init__(self, header, body):
        self.header = header
        self.body = body

    def serialize(self):
        header_bytes = self.header.to_bytes()
        body_bytes = self.body.to_bytes()
        total_length = len(header_bytes) + len(body_bytes)
        return struct.pack(">I", total_length) + header_bytes + body_bytes


# Handle client connections
def handle_connection(connection: socket.socket):
    while True:
        msg = recv_request(connection)
        if not msg:
            break
        req_header = RequestHeaderV0.from_bytes(msg[:8])
        req_body = msg[8:]
        resp_header = ResponseHeaderV0(req_header.correlation_id)
        if not 0 <= req_header.request_api_version <= 4:
            resp_body = APIVersionsResponseBodyV4(error_code=35)
        else:
            resp_body = APIVersionsResponseBodyV4(
                error_code=0,
                api_keys=[(18, 0, 4), (75, 0, 0)],
                throttle_time_ms=0,
            )
        response = Response(resp_header, resp_body)
        connection.send(response.serialize())
    connection.shutdown(socket.SHUT_WR)
    connection.close()


# Receive request from the client
def recv_request(connection: socket.socket) -> bytes:
    msg_len = connection.recv(4)
    if not msg_len:
        return b""
    (msg_len,) = struct.unpack(">I", msg_len)
    if msg_len == 0:
        return b""
    msg = connection.recv(msg_len)
    if len(msg) != msg_len:
        print("Invalid length received")
        return b""
    return msg


# Main server setup
if __name__ == "__main__":
    with socket.create_server(("localhost", 9092), reuse_port=True) as server:
        while True:
            conn, address = server.accept()
            t = threading.Thread(target=handle_connection, args=(conn,), daemon=True)
            t.start()
