import socket  # noqa: F401

def parse_request(request_data):
    # Extract the correlation_id from the request header (4 bytes)
    correlation_id = int.from_bytes(request_data[8:12], 'big')
    return correlation_id

def construct_response(correlation_id):
    # Response starts with the message size (which will be calculated)
    
    # Step 1: Set the error_code to 0 (No Error)
    error_code = 0

    # Step 2: Set the API key and version details for API_VERSIONS (API key 18)
    api_key = 18  # API_VERSIONS
    min_version = 0  # Minimum version
    max_version = 4  # Maximum version, must be >= 4

    # Step 3: Construct the response body
    response_data = bytearray()

    # Add the correlation_id (4 bytes)
    response_data.extend(correlation_id.to_bytes(4, 'big'))

    # Add the error_code (2 bytes)
    response_data.extend(error_code.to_bytes(2, 'big'))

    # Add the number of API entries (2 bytes, in this case, 1 entry for API_VERSIONS)
    response_data.extend((1).to_bytes(2, 'big'))

    # Add the API entry for API_VERSIONS (API key 18)
    response_data.extend(api_key.to_bytes(2, 'big'))  # API Key (2 bytes)
    response_data.extend(min_version.to_bytes(2, 'big'))  # Min Version (2 bytes)
    response_data.extend(max_version.to_bytes(2, 'big'))  # Max Version (2 bytes)

    # Step 4: Calculate the message_size (4 bytes) to include the length of the entire response
    message_size = len(response_data) + 4  # +4 for the message_size field itself

    # Add the message_size (4 bytes) at the beginning of the response
    final_response = bytearray(message_size.to_bytes(4, 'big')) + response_data

    return final_response

def main():
    # Set up the server to listen on localhost and port 9092
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server is listening on port 9092...")

    # Wait for the client to connect
    conn, addr = server.accept()
    print(f"Connection established with {addr}")

    # Read up to 1024 bytes from the client
    data = conn.recv(1024)
    print(f"Received request: {data.hex()}")  # Print the request data in hex for debugging

    # Parse the request to extract correlation_id
    correlation_id = parse_request(data)
    print(f"Extracted correlation_id: {correlation_id}")

    # Construct the response based on the parsed data
    response_data = construct_response(correlation_id)

    # Send the response to the client
    conn.sendall(response_data)
    print(f"Sent response: {response_data.hex()}")  # Print the response in hex for debugging

    # Close the connection
    conn.close()
    print("Connection closed")





if __name__ == "__main__":
    main()
