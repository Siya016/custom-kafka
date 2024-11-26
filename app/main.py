import socket
from enum import Enum, unique

@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35

def parse_request_data(data: bytes):
    """
    Parses the incoming data to extract the API key, API version, and correlation ID.
    """
    api_key = int.from_bytes(data[0:2], 'big')  # API key is 2 bytes
    api_version = int.from_bytes(data[2:4], 'big')  # API version is 2 bytes
    correlation_id = int.from_bytes(data[4:8], 'big')  # Correlation ID is 4 bytes
    return api_key, api_version, correlation_id

def make_response(api_key, api_version, correlation_id):
    """
    Constructs the Kafka response message based on the request.
    """
    valid_api_versions = [0, 1, 2, 3, 4]
    
    # Check if API version is supported
    if api_version not in valid_api_versions:
        error_code = ErrorCode.UNSUPPORTED_VERSION
    else:
        error_code = ErrorCode.NONE

    response_header = correlation_id.to_bytes(4, 'big')

    min_version = 0  # Minimum supported version
    max_version = 4  # Maximum supported version
    throttle_time_ms = 0  # Throttle time
    tag_buffer = b"\x00"  # Placeholder buffer

    # Build the response body
    response_body = (
        error_code.value.to_bytes(2, 'big')  # Error code (2 bytes)
        + int(2).to_bytes(1, 'big')  # Number of supported versions (1 byte)
        + api_key.to_bytes(2, 'big')  # API key (2 bytes)
        + min_version.to_bytes(2, 'big')  # Minimum version (2 bytes)
        + max_version.to_bytes(2, 'big')  # Maximum version (2 bytes)
        + tag_buffer  # Placeholder buffer
        + throttle_time_ms.to_bytes(4, 'big')  # Throttle time (4 bytes)
        + tag_buffer  # Another placeholder buffer (1 byte)
    )

    # Calculate total response length (including length header)
    total_response_length = len(response_header) + len(response_body) + 4  # Include 4 bytes for length header
    response = response_header + response_body

    # Add the response length at the start of the response
    final_response = total_response_length.to_bytes(4, 'big') + response
    return final_response

def handle_request(client: socket.socket):
    """
    Handles the incoming Kafka request, processes it, and sends a response.
    """
    data = client.recv(2048)
    print(f"Received request (hex): {data.hex()}")

    # Parse the incoming request
    api_key, api_version, correlation_id = parse_request_data(data)
    print(f"Parsed request - API key: {api_key}, API version: {api_version}, Correlation ID: {correlation_id}")

    # Create and send the response
    response = make_response(api_key, api_version, correlation_id)
    client.sendall(response)
    print(f"Sent response (hex): {response.hex()}")

def main():
    # Server listens on port 9092
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on port 9092...")

    client, addr = server.accept()
    print(f"Connection established with {addr}")

    # Handle request and send response
    handle_request(client)

    # Close the connection
    client.close()
    print("Connection closed")

if __name__ == "__main__":
    main()
