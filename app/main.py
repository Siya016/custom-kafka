



import socket
import logging
import threading

def parse_message(msg):
    """
    Parses a Kafka request message and extracts apiKey, apiVersion, and correlationId.
    """
    api_key = int.from_bytes(msg[4:6], byteorder="big")  # Extract the apiKey
    api_version = int.from_bytes(msg[6:8], byteorder="big")
    correlation_id = int.from_bytes(msg[8:12], byteorder="big")
    return api_key, api_version, correlation_id



def construct_response(correlation_id, api_key, api_version):
    header = correlation_id.to_bytes(4, byteorder="big")
    error_code = 0  # Error code: 0 indicates no error
    throttle_time_ms = 0  # Throttle time in milliseconds

    print(f"Received request: api_key={api_key}, api_version={api_version}")

    if api_key == 18:  # ApiVersions
        # Construct the ApiVersionsResponse with multiple API keys
        payload = error_code.to_bytes(2, byteorder="big")  # Error code

        api_keys = [
            {"key": 18, "min_version": 0, "max_version": 4},
            {"key": 75, "min_version": 0, "max_version": 0}
        ]

        print(f"Sending ApiVersions response with {len(api_keys)} keys")
        payload = error_code.to_bytes(2, byteorder="big")  # Error code
        payload += throttle_time_ms.to_bytes(4, byteorder="big") 
        payload += len(api_keys).to_bytes(4, byteorder="big")  # Number of API keys

        

        for api_info in api_keys:
            payload += api_info["key"].to_bytes(2, byteorder="big")
            payload += api_info["min_version"].to_bytes(2, byteorder="big")
            payload += api_info["max_version"].to_bytes(2, byteorder="big")

            print(f"API Key: {api_info['key']}, Min Version: {api_info['min_version']}, Max Version: {api_info['max_version']}")

        payload += (0).to_bytes(1, byteorder="big")


    elif api_key == 75:  # DescribeTopicPartitions
        # Construct a DescribeTopicPartitions response
        payload = error_code.to_bytes(2, byteorder="big")  # Error code
        payload += int(0).to_bytes(2, byteorder="big")  # Placeholder response
        logging.debug("Sending DescribeTopicPartitions response")

    else:
        # Default error code if the API key is unknown
        payload = error_code.to_bytes(2, byteorder="big")  # Error code
        payload += int(0).to_bytes(2, byteorder="big")  # Placeholder version
        payload += int(4).to_bytes(2, byteorder="big")  # Placeholder flags
        logging.debug("Sending default error response")

    response_length = len(header + payload)
    response = response_length.to_bytes(4, byteorder="big") + header + payload
    print(f"Constructed response: {response.hex()}")

    return response

def handle_client(client, addr):
    """
    Handles a single client connection, processing one or more requests.
    """
    print(f"Handling client from {addr}")
    try:
        while True:
            request = client.recv(1024)
            if not request:
                break  # Client disconnected
            # Parse the request and extract api_key, api_version, and correlation_id
            api_key, api_version, correlation_id = parse_message(request)
            print(f"Received request: apiKey={api_key}, apiVersion={api_version}, correlationId={correlation_id}")
            # Construct the response using the extracted api_key
            response = construct_response(correlation_id, api_key, api_version)
            # Send the response to the client
            client.sendall(response)
    except ConnectionResetError:
        print(f"Connection with {addr} reset by client.")
    finally:
        client.close()
        print(f"Connection with {addr} closed.")

def start_server(host="localhost", port=9092):
    """
    Starts the Kafka-like server on the specified host and port.
    """
    print(f"Starting server on {host}:{port}...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()  # Enable listening for incoming connections
    while True:
        # Accept a client connection
        client, addr = server.accept()
        print(f"Client connected from {addr}")
        # Handle the client's requests in a new thread
        thread = threading.Thread(
            target=handle_client, args=(client, addr), daemon=True
        )
        thread.start()

def main():
    """
    Entry point for the program.
    """
    start_server()

if __name__ == "__main__":
    main()


# 



