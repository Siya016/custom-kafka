import socket  # noqa: F401
from enum import Enum, unique

# Define the error codes for the Kafka protocol
@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35


# Function to parse the incoming Kafka request data
def parse_request_data(data: bytes):
    """
    Parses the incoming data to extract the API key, API version, and correlation ID.
    
    Args:
    - data: The raw request data received from the client.
    
    Returns:
    - api_key: The API key as an integer.
    - api_version: The API version as an integer.
    - correlation_id: The correlation ID as an integer.
    """
    api_key = int.from_bytes(data[0:2], 'big')  # API key is 2 bytes
    api_version = int.from_bytes(data[2:4], 'big')  # API version is 2 bytes
    correlation_id = int.from_bytes(data[4:8], 'big')  # Correlation ID is 4 bytes
    return api_key, api_version, correlation_id


# Function to create a response based on the request
def make_response(api_key, api_version, correlation_id):
    """
    Constructs the Kafka response message based on the request.
    
    Args:
    - api_key: The API key received from the client.
    - api_version: The API version received from the client.
    - correlation_id: The correlation ID received from the client.
    
    Returns:
    - response: The constructed response message as bytes.
    """
    # Define valid API versions supported by the server
    valid_api_versions = [0, 1, 2, 3, 4]
    
    # Determine the error code based on the request's API version
    if api_version not in valid_api_versions:
        error_code = ErrorCode.UNSUPPORTED_VERSION
    else:
        error_code = ErrorCode.NONE

    # Response header: correlation_id (4 bytes)
    response_header = correlation_id.to_bytes(4, 'big')

    # Define the response body structure for API versions
    min_version = 0  # Minimum supported version
    max_version = 4  # Maximum supported version
    throttle_time_ms = 0  # Throttle time (not used here, so set to 0)
    tag_buffer = b"\x00"  # Placeholder buffer

    # Construct the response body
    response_body = (
        error_code.value.to_bytes(2, 'big')  # Error code (2 bytes)
        + int(2).to_bytes(1, 'big')  # Number of supported versions (1 byte)
        + api_key.to_bytes(2, 'big')  # API key (2 bytes)
        + min_version.to_bytes(2, 'big')  # Minimum version (2 bytes)
        + max_version.to_bytes(2, 'big')  # Maximum version (2 bytes)
        + tag_buffer  # Placeholder buffer (1 byte)
        + throttle_time_ms.to_bytes(4, 'big')  # Throttle time (4 bytes)
        + tag_buffer  # Another placeholder buffer (1 byte)
    )

    # Return the full response with header and body
    response = response_header + response_body
    response_length = len(response) + 4  # Include length of the response message

    # Adding the response size (4 bytes) at the beginning of the response
    final_response = response_length.to_bytes(4, 'big') + response
    return final_response


# Function to handle the client request and send a response
def handle_request(client: socket.socket):
    """
    Handles the incoming Kafka request, processes it, and sends a response.
    
    Args:
    - client: The client socket connection.
    """
    # Receive the request data (up to 2048 bytes)
    data = client.recv(2048)
    print(f"Received request (hex): {data.hex()}")  # Print request data in hex for debugging

    # Parse the incoming request data
    api_key, api_version, correlation_id = parse_request_data(data)

    # Print parsed values for debugging
    print(f"Parsed request - API key: {api_key}, API version: {api_version}, Correlation ID: {correlation_id}")

    # Create the response based on the parsed data
    response = make_response(api_key, api_version, correlation_id)

    # Send the response back to the client
    client.sendall(response)
    print(f"Sent response (hex): {response.hex()}")  # Print response in hex for debugging


# Main function to set up the server and handle incoming requests
def main():
    # Create and bind the server to localhost and port 9092
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on port 9092...")

    # Wait for client connection
    client, addr = server.accept()
    print(f"Connection established with {addr}")

    # Handle the incoming request and send the response
    handle_request(client)

    # Close the connection
    client.close()
    print("Connection closed")
if __name__ == "__main__":
    main()
