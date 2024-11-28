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


# def handle_request(data: bytes) -> bytes:
#     """
#     Handles a single request from the client and constructs the appropriate response.
#     """
#     request = Message(data[4:], b"")  # Skip the first 4 bytes for size
#     print(f"Received request: {request}")

#     # Check if the API version is supported (error code 0 for valid versions, 35 for unsupported)
#     error_code = 0 if request.request_api_version in [0, 1, 2, 3, 4] else 35

#     # Construct the response message
#     response_header = request.correlation_id.to_bytes(4, byteorder="big")

#     # ApiVersions response body
#     response_body = (
#         error_code.to_bytes(2, byteorder="big") +  # error_code: 2 bytes
#         int(1).to_bytes(1, byteorder="big") +  # num_api_keys: 1 byte
#         int(18).to_bytes(2, byteorder="big") +  # api_key: 18 (API_VERSIONS)
#         int(4).to_bytes(2, byteorder="big") +  # min_version: 4
#         int(4).to_bytes(2, byteorder="big") +  # max_version: 4
#         int(0).to_bytes(4, byteorder="big")  # throttle_time_ms: 0
#     )

#     response_size = len(response_header + response_body)

#     # Construct the full response message
#     response = response_size.to_bytes(4, byteorder="big") + response_header + response_body
#     print(f"Constructed response: {response.hex()}")
#     return response


# def main():
#     server = create_server(("localhost", 9092), reuse_port=True)
#     print("Server started and listening on port 9092...")
    
#     client_socket, client_address = server.accept()  # Wait for client
#     print(f"Client connected: {client_address}")

#     try:
#         while True:
#             data = client_socket.recv(1024)
#             if not data:
#                 print("No more data received. Closing connection.")
#                 break

#             print(f"Received data: {data.hex()}")

#             # Handle the request and generate a response
#             response = handle_request(data)

#             # Send the response back to the client
#             client_socket.sendall(response)
#     except Exception as e:
#         print(f"An error occurred: {e}")
#     finally:
#         client_socket.close()
#         print("Connection closed.")


# if __name__ == "__main__":
#     main()



import socket

class KafkaMessage:
    """Represents a Kafka message."""
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id

    def to_bytes(self):
        """Constructs the ApiVersions response message."""
        error_code = 0
        num_api_keys = 1
        api_key = 18
        min_version = 0
        max_version = 4
        throttle_time_ms = 0

        # Construct response body
        body = (
            error_code.to_bytes(2, "big") +
            num_api_keys.to_bytes(4, "big") +
            api_key.to_bytes(2, "big") +
            min_version.to_bytes(2, "big") +
            max_version.to_bytes(2, "big") +
            throttle_time_ms.to_bytes(4, "big")
        )
        # Construct header
        header = self.correlation_id.to_bytes(4, "big")
        # Combine header and body with size
        return len(header + body).to_bytes(4, "big") + header + body


def handle_client(client_socket):
    """Handles client requests and sends responses."""
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break  # No more data

            correlation_id = int.from_bytes(data[4:8], "big")
            print(f"Received request with Correlation ID: {correlation_id}")

            response = KafkaMessage(correlation_id).to_bytes()
            client_socket.sendall(response)
            print(f"Sent response: {response.hex()}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()
        print("Connection closed.")


def main():
    """Starts the Kafka server."""
    with socket.create_server(("localhost", 9092), reuse_port=True) as server:
        print("Server listening on localhost:9092...")
        while True:
            client_socket, _ = server.accept()
            print("Client connected.")
            handle_client(client_socket)


if __name__ == "__main__":
    main()
