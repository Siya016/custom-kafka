



# import socket
# import logging
# import threading

# def parse_message(msg):
#     """
#     Parses a Kafka request message and extracts apiKey, apiVersion, and correlationId.
#     """
#     api_key = int.from_bytes(msg[4:6], byteorder="big")  # Extract the apiKey
#     api_version = int.from_bytes(msg[6:8], byteorder="big")
#     correlation_id = int.from_bytes(msg[8:12], byteorder="big")
#     return api_key, api_version, correlation_id



# def construct_response(correlation_id, api_key, api_version):
#     header = correlation_id.to_bytes(4, byteorder="big")
#     error_code = 0  # Error code: 0 indicates no error

#     logging.debug(f"Received request: api_key={api_key}, api_version={api_version}")

#     if api_key == 18:  # ApiVersions
#         # Construct the ApiVersionsResponse with multiple API keys
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code

#         api_keys = [
#             {"key": 18, "min_version": 0, "max_version": 4},
#             {"key": 75, "min_version": 0, "max_version": 0}
#         ]

#         print(f"Sending ApiVersions response with {len(api_keys)} keys")

#         payload += int(2).to_bytes(1, byteorder="big")

#         for api_info in api_keys:
#             payload += api_info["key"].to_bytes(2, byteorder="big")
#             payload += api_info["min_version"].to_bytes(2, byteorder="big")
#             payload += api_info["max_version"].to_bytes(2, byteorder="big")

#             print(f"API Key: {api_info['key']}, Min Version: {api_info['min_version']}, Max Version: {api_info['max_version']}")


#     elif api_key == 75:  # DescribeTopicPartitions
#         # Construct a DescribeTopicPartitions response
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += int(0).to_bytes(2, byteorder="big")  # Placeholder response
#         logging.debug("Sending DescribeTopicPartitions response")

#     else:
#         # Default error code if the API key is unknown
#         payload = error_code.to_bytes(2, byteorder="big")  # Error code
#         payload += int(0).to_bytes(2, byteorder="big")  # Placeholder version
#         payload += int(4).to_bytes(2, byteorder="big")  # Placeholder flags
#         logging.debug("Sending default error response")

#     response_length = len(header + payload)
#     response = response_length.to_bytes(4, byteorder="big") + header + payload
#     print(f"Constructed response: {response.hex()}")

#     return response

# def handle_client(client, addr):
#     """
#     Handles a single client connection, processing one or more requests.
#     """
#     print(f"Handling client from {addr}")
#     try:
#         while True:
#             request = client.recv(1024)
#             if not request:
#                 break  # Client disconnected
#             # Parse the request and extract api_key, api_version, and correlation_id
#             api_key, api_version, correlation_id = parse_message(request)
#             print(f"Received request: apiKey={api_key}, apiVersion={api_version}, correlationId={correlation_id}")
#             # Construct the response using the extracted api_key
#             response = construct_response(correlation_id, api_key, api_version)
#             # Send the response to the client
#             client.sendall(response)
#     except ConnectionResetError:
#         print(f"Connection with {addr} reset by client.")
#     finally:
#         client.close()
#         print(f"Connection with {addr} closed.")

# def start_server(host="localhost", port=9092):
#     """
#     Starts the Kafka-like server on the specified host and port.
#     """
#     print(f"Starting server on {host}:{port}...")
#     server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server.bind((host, port))
#     server.listen()  # Enable listening for incoming connections
#     while True:
#         # Accept a client connection
#         client, addr = server.accept()
#         print(f"Client connected from {addr}")
#         # Handle the client's requests in a new thread
#         thread = threading.Thread(
#             target=handle_client, args=(client, addr), daemon=True
#         )
#         thread.start()

# def main():
#     """
#     Entry point for the program.
#     """
#     start_server()

# if __name__ == "__main__":
#     main()


# 



import socket
import threading

def handle_connection(connection: socket.socket):
    while True:
        msg = recv_request(connection)
        if not msg:
            break

        # Extract header and body manually
        header = msg[:8]
        body = msg[8:]

        # Parse the header fields manually
        try:
            correlation_id = int.from_bytes(header[0:4], "big")
            request_api_version = int.from_bytes(header[4:8], "big")
        except ValueError:
            # If header parsing fails, respond with an error
            response = build_response(correlation_id=0, error_code=35)
            connection.send(response)
            continue

        # Validate request API version
        if not 0 <= request_api_version <= 4:
            response = build_response(correlation_id, error_code=35)
        else:
            response = build_response(
                correlation_id,
                error_code=0,
                api_keys=[(18, 0, 4), (75, 0, 0)],
                throttle_time_ms=0,
            )

        connection.send(response)

    connection.shutdown(socket.SHUT_WR)
    connection.close()

def recv_request(connection: socket.socket) -> bytes:
    # Receive the message length (first 4 bytes)
    msg_len_bytes = connection.recv(4)
    if not msg_len_bytes:
        return b""

    # Convert length from bytes to an integer
    msg_len = int.from_bytes(msg_len_bytes, "big")
    if msg_len == 0:
        return b""

    # Receive the message body
    msg = connection.recv(msg_len)
    if len(msg) != msg_len:
        print("Invalid length received")
        return b""

    return msg

def build_response(correlation_id, error_code, api_keys=None, throttle_time_ms=0):
    """
    Constructs a response message.
    """
    if api_keys is None:
        api_keys = []

    # Serialize response manually
    api_keys_str = ",".join(f"({key},{min_ver},{max_ver})" for key, min_ver, max_ver in api_keys)
    body = f"error_code={error_code},api_keys=[{api_keys_str}],throttle_time_ms={throttle_time_ms}"
    header = correlation_id.to_bytes(4, "big") + error_code.to_bytes(4, "big")
    return header + body.encode()

if __name__ == "__main__":
    with socket.create_server(("localhost", 9092), reuse_port=True) as server:
        while True:
            conn, address = server.accept()
            t = threading.Thread(target=handle_connection, args=(conn,), daemon=True)
            t.start()

