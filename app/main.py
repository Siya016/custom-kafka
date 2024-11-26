import socket
from enum import Enum, unique

@unique
class ErrorCode(Enum):
    NONE = 0  # No error
    UNSUPPORTED_VERSION = 35  # Unsupported API version

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
    # Valid API versions for ApiVersions
    valid_api_versions = [0, 1, 2, 3, 4]

    # Set error code to NONE (0) for valid API versions
    if api_version not in valid_api_versions:
        error_code = ErrorCode.UNSUPPORTED_VERSION
    else:
        error_code = ErrorCode.NONE

    # Construct the response body
    response_header = correlation_id.to_bytes(4, 'big')  # 4 bytes for correlation ID

    # Number of entries for supported API keys
    num_api_key_entries = 1  # We have at least 1 entry for API key 18

    # API Key 18 (ApiVersions) and supported version information
    api_key_18 = 18  # The API key we're responding to
    min_version = 0  # Minimum supported version
    max_version = 4  # Maximum supported version for API key 18

    # Error code for this API key (0 indicates no error)
    api_key_error_code = error_code.value.to_bytes(2, 'big')

    # Response body for API key 18
    api_key_entry = (
        api_key_error_code  # 2 bytes for error code
        + num_api_key_entries.to_bytes(1, 'big')  # Number of entries (1 byte)
        + api_key_18.to_bytes(2, 'big')  # API key (2 bytes)
        + min_version.to_bytes(2, 'big')  # Min version (2 bytes)
        + max_version.to_bytes(2, 'big')  # Max version (2 bytes)
    )

    # Throttle time and placeholder buffer (standard Kafka response padding)
    throttle_time_ms = 0  # No throttle
    response_body = api_key_entry + throttle_time_ms.to_bytes(4, 'big')

    # Calculate total response length (including the 4-byte length field)
    total_response_length = len(response_header) + len(response_body) + 4  # Length includes the 4-byte header
    response = response_header + response_body

    # Add the message length at the start of the response
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
