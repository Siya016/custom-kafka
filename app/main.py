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
from typing import Tuple

class Message:
    def __init__(self, header: bytes, body: bytes):
        # Calculate total size including the 4-byte length prefix
        total_data = header + body
        self.size = len(total_data)
        
        # Parse header fields
        self.header = header
        self.request_api_key = int.from_bytes(header[:2], byteorder="big")
        self.request_api_version = int.from_bytes(header[2:4], byteorder="big")
        self.correlation_id = int.from_bytes(header[4:8], byteorder="big")
        self.client_id = int.from_bytes(header[8:], byteorder="big")
        self.tagged_fields = ""  # No tagged fields for us
        self.body = body

    def to_bytes(self):
        # Include 4-byte message length prefix
        return self.size.to_bytes(4, byteorder="big") + self.header + self.body

    def __repr__(self):
        return (
            f"{self.size} | "
            f"{self.request_api_key} - {self.request_api_version} - {self.correlation_id} - {self.client_id} | "
            f"{self.body}"
        )

def receive_full_message(sock: socket.socket) -> bytes:
    """
    Receive a complete Kafka-style message, which includes a 4-byte length prefix
    """
    # Read the 4-byte message length
    length_bytes = sock.recv(4)
    if not length_bytes:
        return b''
    
    # Convert length bytes to integer
    message_length = int.from_bytes(length_bytes, byteorder="big")
    
    # Read the rest of the message
    message_data = sock.recv(message_length)
    
    # Combine length bytes and message data
    return length_bytes + message_data

def handle_api_versions_request(request: Message) -> Message:
    """
    Handle APIVersions request and generate appropriate response
    """
    # Construct the response header (correlation ID from request)
    response_header = request.correlation_id.to_bytes(4, byteorder="big")

    # ApiVersions response body 
    response_body = (
        int(0).to_bytes(2, byteorder="big") +  # error_code: 2 bytes (0 = No Error)
        int(1).to_bytes(1, byteorder="big") +  # num_api_keys: 1 byte 
        int(18).to_bytes(2, byteorder="big") +  # api_key: 18 (2 bytes)
        int(4).to_bytes(2, byteorder="big") +  # min_version: 4 (2 bytes)
        int(4).to_bytes(2, byteorder="big") +  # max_version: 4 (2 bytes)
        int(0).to_bytes(4, byteorder="big") +  # throttle_time_ms: 4 bytes
        int(0).to_bytes(2, byteorder="big")    # TAG_BUFFER: 2 bytes
    )

    # Construct and return the full response message
    return Message(response_header, response_body)

def main():
    # Create server socket
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on localhost:9092")
    
    # Accept client connection
    client_socket, address = server.accept()
    print(f"Client connected: {address}")

    try:
        while True:
            # Receive full message
            received_data = receive_full_message(client_socket)
            
            # Check if connection is closed
            if not received_data:
                break
            
            # Skip first 4 bytes (message length), parse request
            request = Message(received_data[4:12], received_data[12:])
            print(f"Received request: {request}")

            # Handle APIVersions request
            response = handle_api_versions_request(request)
            
            # Send response
            client_socket.sendall(response.to_bytes())
            print(f"Sent response: {response.to_bytes().hex()}")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        # Close the connection
        client_socket.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()