# from socket import create_server

class Message:
    header: bytes
    body: bytes

    def __init__(self, header: bytes, body: bytes):
        self.size = len(header + body)
        self.header = header
        self.request_api_key = int.from_bytes(header[:2], byteorder="big")
        self.request_api_version = int.from_bytes(header[2:4], byteorder="big")
        self.correlation_id = int.from_bytes(header[4:8], byteorder="big")
        self.client_id = int.from_bytes(header[8:], byteorder="big")
        self.tagged_fields = ""  # No tagged fields for us
        self.body = body

    def to_bytes(self):
        return self.size.to_bytes(4, byteorder="big") + self.header + self.body

    def __repr__(self):
        return (
            f"{self.size} | "
            f"{self.request_api_key} - {self.request_api_version} - {self.correlation_id} - {self.client_id} | "
            f"{self.body}"
        )


def handle_request(data: bytes) -> bytes:
    """
    Handles a single request from the client and constructs the appropriate response.
    """
    request = Message(data[4:], b"")  # Skip the first 4 bytes for size
    print(f"Received request: {request}")

    # Check if the API version is supported (error code 0 for valid versions, 35 for unsupported)
    error_code = 0 if request.request_api_version in [0, 1, 2, 3, 4] else 35

    # Construct the response message
    response_header = request.correlation_id.to_bytes(4, byteorder="big")

    # ApiVersions response body
    response_body = (
        error_code.to_bytes(2, byteorder="big") +  # error_code: 2 bytes
        int(1).to_bytes(1, byteorder="big") +  # num_api_keys: 1 byte
        int(18).to_bytes(2, byteorder="big") +  # api_key: 18 (API_VERSIONS)
        int(4).to_bytes(2, byteorder="big") +  # min_version: 4
        int(4).to_bytes(2, byteorder="big") +  # max_version: 4
        int(0).to_bytes(4, byteorder="big")  # throttle_time_ms: 0
    )

    response_size = len(response_header + response_body)

    # Construct the full response message
    response = response_size.to_bytes(4, byteorder="big") + response_header + response_body
    print(f"Constructed response: {response.hex()}")
    return response


def main():
    server = create_server(("localhost", 9092), reuse_port=True)
    print("Server started and listening on port 9092...")
    
    client_socket, client_address = server.accept()  # Wait for client
    print(f"Client connected: {client_address}")

    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                print("No more data received. Closing connection.")
                break

            print(f"Received data: {data.hex()}")

            # Handle the request and generate a response
            response = handle_request(data)

            # Send the response back to the client
            client_socket.sendall(response)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client_socket.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
