# Custom Kafka Implementation in Python

## Project Description:
This project implements a basic Kafka-like server in Python, simulating key features of Kafka, such as handling "DescribeTopic" requests and creating appropriate responses. The application processes Kafka metadata logs and implements core logic for managing topic partitions and managing client requests asynchronously using `asyncio`.

## Key Features:
- **Custom Kafka Server**: A server that listens for requests on port 9092 and handles client connections asynchronously.
- **Request Handling**: Parses "DescribeTopic" requests to extract topic details and create structured responses with metadata.
- **Kafka Metadata Parsing**: Reads binary Kafka logs to validate specific topics.
- **Error Handling**: Implements error handling for unsupported API keys and missing log files.
- **Async Communication**: Uses `asyncio` for asynchronous client handling, ensuring real-time communication and efficient response times.

## Technologies Used:
- Python
- asyncio
- struct
- Custom Kafka-like protocol implementation

