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
    message_size = (0).to_bytes(4, byteorder="big", signed=True)

    # Correlation ID (4 bytes): Hardcoded to 7
    correlation_id = (7).to_bytes(4, byteorder="big", signed=True)

    # Combine the message size and correlation ID into the final response
    response = message_size + correlation_id

    # Step 5: Send the response to the client
    conn.sendall(response)
    print(f"Sent response: {response.hex()}")

    # Step 6: Close the connection
    conn.close()
    print("Connection closed")

   


if __name__ == "__main__":
    main()
