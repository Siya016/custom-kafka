import socket  # noqa: F401

def parse_request(request_data):
    # Extract the request_api_version from the request data (2 bytes)
    request_api_version = int.from_bytes(request_data[4:6], 'big')
    correlation_id = int.from_bytes(request_data[8:12], 'big')

    # Check if the version is within the supported range (0 to 4)
    if request_api_version > 4:
        # Unsupported version, return error code 35 (UNSUPPORTED_VERSION)
        error_code = 35
    else:
        # Valid version, no error
        error_code = 0

    return correlation_id, error_code

def construct_response(correlation_id, error_code):
    # Construct the response message
    # message_size (placeholder), correlation_id (4 bytes), error_code (2 bytes)
    response_data = bytearray()

    # Add the correlation_id (4 bytes)
    response_data.extend(correlation_id.to_bytes(4, 'big'))

    # Add the error_code (2 bytes)
    response_data.extend(error_code.to_bytes(2, 'big'))

    # Calculate the actual message size
    message_size = len(response_data) + 4  # message_size itself is 4 bytes

    # Add the message size at the beginning
    response_data = bytearray(message_size.to_bytes(4, 'big')) + response_data

    return response_data

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

    # Step 4: Parse the request and check for unsupported version
    correlation_id, error_code = parse_request(data)
    print(f"Extracted correlation_id: {correlation_id}")
    print(f"Error code: {error_code}")

    # Step 5: Construct the response based on the parsed data
    response_data = construct_response(correlation_id, error_code)

    # Step 6: Send the response to the client
    conn.sendall(response_data)
    print(f"Sent response: {response_data.hex()}")  # Print the response in hex for debugging

    # Step 7: Close the connection
    conn.close()
    print("Connection closed")





if __name__ == "__main__":
    main()
