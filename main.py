import struct
import asyncio
import os

def read_kafka_metadata_log(file_path, topic_name):
    try:
        with open(file_path, "rb") as f:
            while True:
                offset_data = f.read(8)
                if not offset_data:
                    break
                offset = struct.unpack(">q", offset_data)[0]

                length_data = f.read(4)
                if not length_data:
                    break
                message_length = struct.unpack(">i", length_data)[0]

                message_data = f.read(message_length)
                if not message_data:
                    break

                key_length = struct.unpack(">i", message_data[:4])[0]
                key = message_data[4:4 + key_length]
                value = message_data[4 + key_length:]

                if topic_name.encode("utf-8") in value:
                    return True
    except FileNotFoundError:
        print(f"Log file not found: {file_path}")
    except Exception as e:
        print(f"Error reading log file: {e}")
    return False


def parse_describetopic_request(request):
    try:
        # Client ID length and value
        client_id_length = struct.unpack(">h", request[8:10])[0]
        client_id = request[10:10 + client_id_length].decode("utf-8", errors="ignore")
        offset = 10 + client_id_length

        # Array length
        array_length = struct.unpack(">h", request[offset:offset + 2])[0]
        offset += 2

        # Topic name length and value
        topic_name_length = struct.unpack(">h", request[offset:offset + 2])[0]
        offset += 2
        topic_name = request[offset:offset + topic_name_length].decode("utf-8")
        offset += topic_name_length

        # Skip remaining binary fields
        partition_limit = struct.unpack(">i", request[offset:offset + 4])[0]
        offset += 4
        cursor = request[offset]  # Single byte

        return topic_name, partition_limit, cursor
    except UnicodeDecodeError as e:
        raise ValueError(f"UnicodeDecodeError at position {e.start}: {e.reason}")
    except Exception as e:
        raise ValueError(f"Error parsing DescribeTopic request: {e}")


def create_response(request):
    print(f"Request (Hex): {request.hex()}")
    api_key = struct.unpack(">h", request[:2])[0]  # API key
    correlation_id = struct.unpack(">i", request[4:8])[0]  # Correlation ID

    if api_key == 75:  # DescribeTopicPartitions
        try:
            topic_name, partition_limit, cursor = parse_describetopic_request(request)

            # Create response
            throttle_time_ms = 0
            error_code = 0  # No error
            partitions_array = 1  # Single partition
            partition_index = 0
            partition_error_code = 0  # No error
            leader_id = 1
            replicas = [1]
            isr = [1]

            body = struct.pack(">i", throttle_time_ms)
            body += struct.pack(">h", error_code)
            body += struct.pack(">h", len(topic_name))
            body += topic_name.encode("utf-8")
            body += struct.pack(">h", partitions_array)
            body += struct.pack(">h", partition_index)
            body += struct.pack(">h", partition_error_code)
            body += struct.pack(">i", leader_id)
            body += struct.pack(">h", len(replicas))
            body += struct.pack(">i", replicas[0])
            body += struct.pack(">h", len(isr))
            body += struct.pack(">i", isr[0])

            response_message_size = len(body) + 4
            header = struct.pack(">i", response_message_size)
            header += struct.pack(">i", correlation_id)
            response = header + body
            print(f"Response (Hex): {response.hex()}")
            return response
        except ValueError as e:
            print(f"Error parsing DescribeTopic request: {e}")
            return b""
    else:
        print("Unsupported API key")
        return b""


    raise ValueError("Unsupported API key")


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
        print("Connection closed unexpectedly")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, "localhost", 9092)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
