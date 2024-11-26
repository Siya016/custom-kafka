import socket  # noqa: F401




def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr =server.accept() # wait for client
    data = conn.recv(1024)  # Read up to 1024 bytes from the client
    print(f"Received request: {data}")

    # Step 4: Build the response
    # Message size (4 bytes): Any value (0 for this stage)
    correlation_id = int.from_bytes(data[8:12], byteorder='big', signed=True)
    print(f"Extracted correlation_id: {correlation_id}")

    # Step 5: Build the response
    # Message size (4 bytes): Set to 0 for now (this will be handled in a later stage)
    message_size = (0).to_bytes(4, byteorder="big", signed=True)

    # Correlation ID (4 bytes): Use the extracted correlation_id from the request
    correlation_id_bytes = correlation_id.to_bytes(4, byteorder="big", signed=True)

    # Combine the message size and correlation ID into the final response
    response = message_size + correlation_id_bytes

    # Step 6: Send the response to the client
    conn.sendall(response)
    print(f"Sent response: {response.hex()}")  # Print the response in hex for debugging

    # Step 7: Close the connection
    conn.close()
    print("Connection closed")
   


if __name__ == "__main__":
    main()
