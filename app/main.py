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
#     response_body = (
#         error_code.to_bytes(2, byteorder="big") +  # error_code: 2 bytes
#         int(1 + 1).to_bytes(1, byteorder="big") +  # num_api_keys: 1 byte (1 key + 1 entry for API key 18)
#         int(18).to_bytes(2, byteorder="big") +  # api_key: 18 (2 bytes)
#         int(4).to_bytes(2, byteorder="big") +  # min_version: 4 (2 bytes)
#         int(4).to_bytes(2, byteorder="big") +  # max_version: 4 (2 bytes)
#         int(0).to_bytes(2, byteorder="big") +  # TAG_BUFFER: 0 (2 bytes)
#         int(0).to_bytes(4, byteorder="big")  # throttle_time_ms: 0 (4 bytes)
#     )

#     # Construct the full response message
#     message = Message(response_header, response_body)
#     print(f"Sending message: {message.to_bytes().hex()}")

#     # Send the response back to the client
#     socket.sendall(message.to_bytes())

#     # Close the connection
#     socket.close()
#     print("Connection closed.")

# if __name__ == "__main__":
#     main()

import socket
from enum import Enum, unique

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35

def parse_request(data):
    """
    Parses the received request data into a Kafka-like structure.
    Args:
        data (bytes): The raw data received from the client.
    Returns:
        dict: Parsed request data with keys 'api_key', 'api_version', and 'correlation_id'.
    """
    if len(data) < 12:
        raise ValueError("Incomplete Kafka request received.")
    return {
        "api_key": int.from_bytes(data[4:6], byteorder="big"),
        "api_version": int.from_bytes(data[6:8], byteorder="big"),
        "correlation_id": int.from_bytes(data[8:12], byteorder="big"),
    }

def construct_response(request):
    """
    Constructs a Kafka-style response based on the given request.
    Args:
        request (dict): Parsed request data.
    Returns:
        bytes: The constructed response message.
    """
    # Header
    correlation_id = request["correlation_id"]
    response_header = correlation_id.to_bytes(4, byteorder="big")

    # Validate API version
    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE.value
        if request["api_version"] in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION.value
    )

    # Body
    api_key = request["api_key"]
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    response_body = (
        error_code.to_bytes(2, byteorder="big")  # error_code: 2 bytes
        + int(1).to_bytes(1, byteorder="big")  # num_api_keys: 1 byte
        + api_key.to_bytes(2, byteorder="big")  # api_key: 2 bytes
        + min_version.to_bytes(2, byteorder="big")  # min_version: 2 bytes
        + max_version.to_bytes(2, byteorder="big")  # max_version: 2 bytes
        + throttle_time_ms.to_bytes(4, byteorder="big")  # throttle_time_ms: 4 bytes
        + tag_buffer  # TAG_BUFFER: 1 byte
    )

    # Combine header and body
    response = response_header + response_body
    response_size = len(response).to_bytes(4, byteorder="big")
    return response_size + response

def handle_client(client_socket):
    """
    Handles communication with a connected client.
    Args:
        client_socket (socket.socket): The client's socket connection.
    """
    try:
        while True:
            # Receive data from client
            data = client_socket.recv(2048)
            if not data:
                print("Client disconnected.")
                break

            # Parse the request
            request = parse_request(data)
            print(f"Received request: {request}")

            # Construct and send response
            response = construct_response(request)
            print(f"Sending response: {response.hex()}")
            client_socket.sendall(response)

    except (ConnectionResetError, ValueError) as e:
        print(f"Connection error: {e}")
    finally:
        client_socket.close()
        print("Client connection closed.")

def start_server():
    """
    Starts the Kafka-like server to listen for client connections.
    """
    with socket.create_server(("localhost", 9092), reuse_port=True) as server:
        print("Server is listening on port 9092...")
        while True:
            client_socket, address = server.accept()
            print(f"Client connected from {address}")
            handle_client(client_socket)

if __name__ == "__main__":
    start_server()
