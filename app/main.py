import socket  # noqa: F401

import socket

def parse_request(request_data):
    """
    Parse the incoming request data to extract the correlation_id and determine the error_code
    based on the request's API version.
    """
    # Extract the request_api_version (2 bytes starting from index 4)
    request_api_version = int.from_bytes(request_data[4:6], 'big')
    
    # Extract the correlation_id (4 bytes starting from index 8)
    correlation_id = int.from_bytes(request_data[8:12], 'big')

    # Check if the version is within the supported range (0 to 4)
    if request_api_version > 4:
        # Unsupported version, return error code 35
        error_code = 35
    else:
        # Valid version, no error
        error_code = 0

    return correlation_id, error_code

def construct_response(correlation_id, error_code):
    """
    Construct the response data including message size, correlation_id, and error_code.
    """
    # Start constructing the response message
    response_data = bytearray()

    # Add the correlation_id (4 bytes)
    response_data.extend(correlation_id.to_bytes(4, 'big'))

    # Add the error_code (2 bytes)
    response_data.extend(error_code.to_bytes(2, 'big'))

    # Add the number of API entries (2 bytes, in this case, 1 entry for API_VERSIONS)
    response_data.extend((1).to_bytes(2, 'big'))

    # Add the API entry for API_VERSIONS (API key 18)
    api_key = 18  # API_VERSIONS
    min_version = 0  # Min version (should be at least 0)
    max_version = 4  # Max version (should be >= 4)
    
    response_data.extend(api_key.to_bytes(2, 'big'))  # API Key (2 bytes)
    response_data.extend(min_version.to_bytes(2, 'big'))  # Min Version (2 bytes)
    response_data.extend(max_version.to_bytes(2, 'big'))  # Max Version (2 bytes)

    # Calculate the message size (including the message size field itself)
    message_size = len(response_data) + 4  # +4 for the message_size field itself
    
    # Add the message size (4 bytes) at the beginning
    final_response = bytearray(message_size.to_bytes(4, 'big')) + response_data

    return final_response

def handle_request(request_data):
    """
    Handle the incoming request, parse it, construct the response, and return the response.
    """
    # Parse the incoming request to extract the correlation_id and error_code
    correlation_id, error_code = parse_request(request_data)

    # Construct the response based on the parsed data
    response_data = construct_response(correlation_id, error_code)

    # Return the response data to be sent back to the client
    return response_data

def main():
    # Set up the server to listen on localhost and port 9092
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on port 9092...")

    # Wait for the client to connect
    conn, addr = server.accept()
    print(f"Connection established with {addr}")

    # Read the incoming request (up to 1024 bytes)
    data = conn.recv(1024)
    print(f"Received request (hex): {data.hex()}")  # Print request data in hex for debugging

    # Handle the request and generate the response
    response_data = handle_request(data)

    # Send the response back to the client
    conn.sendall(response_data)
    print(f"Sent response (hex): {response_data.hex()}")  # Print the response in hex for debugging

    # Close the connection
    conn.close()
    print("Connection closed")

if __name__ == "__main__":
    main()
