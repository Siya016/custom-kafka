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

from socket import create_server

class Message:
    def __init__(self, data: bytes):
        self.size = int.from_bytes(data[:4], byteorder="big")
        self.header = data[4:12]
        self.request_api_key = int.from_bytes(data[4:6], byteorder="big")
        self.request_api_version = int.from_bytes(data[6:8], byteorder="big")
        self.correlation_id = int.from_bytes(data[8:12], byteorder="big")
        self.body = data[12:]

    def __repr__(self):
        return (
            f"Size: {self.size} | ApiKey: {self.request_api_key} | "
            f"Version: {self.request_api_version} | CorrelationId: {self.correlation_id} | Body: {self.body}"
        )


def process_request(data: bytes) -> bytes:
    # Parse the request
    request = Message(data)
    print(f"Received request: {request}")

    # Ensure the API version is supported
    error_code = 0 if request.request_api_version == 4 else 35

    # Construct the response
    response_header = request.correlation_id.to_bytes(4, byteorder="big")
    num_api_keys = 1

    # Construct the body
    response_body = (
        error_code.to_bytes(2, byteorder="big") +  # Error code: 2 bytes
        num_api_keys.to_bytes(4, byteorder="big") +  # Num API keys: 4 bytes
        int(18).to_bytes(2, byteorder="big") +  # API key: 18
        int(4).to_bytes(2, byteorder="big") +  # Min version: 4
        int(4).to_bytes(2, byteorder="big") +  # Max version: 4
        int(0).to_bytes(4, byteorder="big") +  # Throttle time (ms): 4 bytes
        b"\x00"  # Tagged fields (empty): 1 byte
    )

    # Create the full response
    message_length = (len(response_header) + len(response_body)).to_bytes(4, byteorder="big")
    full_response = message_length + response_header + response_body

    print(f"Sending response: {full_response.hex()}")
    return full_response


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on port 9092...")

    while True:
        conn, address = server.accept()
        print(f"Client connected: {address}")

        try:
            while True:
                # Read the request
                data = conn.recv(1024)
                if not data:
                    print("Client disconnected.")
                    break

                # Process the request and send the response
                response = process_request(data)
                conn.sendall(response)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
