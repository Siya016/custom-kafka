import socket
import struct
import asyncio
import threading
def parse_describetopic_request(request):
    length = struct.unpack(">h", request[8:10])[0]
    client_id = request[10 : 10 + length].decode("utf-8")
    offset = 10 + length
    # buffer = request[offset:offset+1]
    array_length = struct.unpack(">B", request[offset + 1 : offset + 2])[0] - 1
    topic_name_length = struct.unpack(">B", request[offset + 2 : offset + 3])[0] - 1
    offset = offset + 3
    topic_name = request[offset : offset + topic_name_length].decode("utf-8")
    offset = offset + topic_name_length
    # buffer = request[offset:offset+1]
    partition_limit = request[offset + 1 : offset + 5]
    cursor = struct.unpack(">B", request[offset + 5 : offset + 6])[0]
    return array_length, topic_name_length, topic_name, partition_limit, cursor
def create_response(request):
    print(f"{request.hex()}")
    api_key = struct.unpack(">h", request[:2])[0]  # int16
    api_version = struct.unpack(">h", request[2:4])[0]  # int16
    correlation_id = struct.unpack(">i", request[4:8])[0]  # int32
    # body
    
    if api_key == 75:
        array_length, topic_name_length, topic_name, partition_limit, cursor = (
            parse_describetopic_request(request)
        )
        throttle_time_ms = 0
        error_code = 3
        uuid = "00000000-0000-0000-0000-000000000000"
        is_internal = 0
        partitions_array = 0
        topic_authorized_operations = int("00000df8", 16)
        # cursor = 255 # null
        body = struct.pack(">B", 0)
        body += struct.pack(">i", throttle_time_ms)
        body += struct.pack(">B", array_length + 1)
        body += struct.pack(">h", error_code)
        body += struct.pack(">B", topic_name_length + 1)  # topicname
        body += topic_name.encode("utf-8")  # contents
        body += bytes.fromhex(uuid.replace("-", ""))  # topic id
        body += struct.pack(">B", is_internal)
        body += struct.pack(">B", partitions_array + 1)
        body += struct.pack(">i", topic_authorized_operations)
        body += struct.pack(">B", 0)
        body += struct.pack(">B", cursor)
        body += struct.pack(">B", 0)
    
    # body
    if api_key == 18:
        error_code = 0 if api_version in [0, 1, 2, 3, 4] else 35
    
        api_keys = {18: [0, 4], 75: [0, 0]}
    
        # api_key = 18
        # min_version, max_version = 0, 4
        throttle_time_ms = 0
        tag_buffer = b"\x00"
        body = struct.pack(">h", error_code)  # error_code: 2 bytes
        number_api_key = len(api_keys) + 1
        print(number_api_key)
        body += struct.pack(">B", number_api_key)  # api_version count
        for key, (min_version, max_version) in api_keys.items():
            body += struct.pack(">hhh", key, min_version, max_version)
            body += struct.pack(">B", 0)
        body += struct.pack(">i", throttle_time_ms)
        body += struct.pack(">B", 0)
        response_message_size = len(body) + 4
    header = struct.pack(">i", response_message_size)
    header += struct.pack(">i", correlation_id)
    print(header, response_message_size, correlation_id)
    response = header + body
    print(f"Response (Hex): {response.hex()}")
    return response
async def handle_client(reader, writer):
    try:
        while True:
            message_size_data = await reader.readexactly(4)
            if not message_size_data:
                break
            message_size = struct.unpack(">i", message_size_data)[0]
            request = await reader.readexactly(message_size)
            response = create_response(request)
            writer.write(response)
            await writer.drain()
    except asyncio.IncompleteReadError:
        print(f"Connection closed unexpectedly")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
async def main():
    server = await asyncio.start_server(handle_client, "localhost", 9092)
    
    addr = server.sockets[0].getsockname()
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
