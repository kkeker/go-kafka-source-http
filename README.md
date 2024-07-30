# GoLang Kafka HTTP Receiver Service

## Overview

Kafka HTTP Receiver Service is a Go-based application designed to receive JSON payloads via HTTP POST requests and forward them to a specified Kafka topic. This service provides detailed logging, monitoring through Prometheus, and a health check endpoint to ensure the service's health and connectivity with Kafka.

## Features

- **Receive JSON payloads via HTTP POST**: Accepts HTTP POST requests with JSON payloads and sends them to a specified Kafka topic.
- **Dynamic Topic Handling**: Extracts topic names from the URL and ensures the topic exists in Kafka, creating it if necessary.
- **Prometheus Monitoring**: Provides detailed metrics about the HTTP requests and Kafka message processing, including:
  - Total number of messages sent to Kafka.
  - Total number of message errors in Kafka.
  - Size of messages sent to Kafka.
  - Time taken to deliver messages to Kafka.
  - Total number of HTTP requests.
  - Duration of HTTP requests.
  - Total number of HTTP request errors.
- **Health Check Endpoint**: Provides an endpoint to check the service's health, ensuring it can connect to Kafka.

## Installation

### Prerequisites

- Go 1.22.5 or higher
- Kafka broker running and accessible (for the test you can use [Redpanda](https://docs.redpanda.com/current/get-started/quick-start/))
- Git (for fetching dependencies)

### Steps

1. Clone the repository:

    ```sh
    git clone github.com/kkeker/go-kafka-source-http
    cd go-kafka-source-http
    ```

2. Install dependencies:

    ```sh
    go mod download
    ```

3. Build the application:

    ```sh
    go build .
    ```

## Configuration

The service can be configured using command-line flags:

- `--http`: The address for the HTTP server to listen on (default: `:8080`).
- `--kafka`: The Kafka broker address (default: `localhost:9092`).
- `--debug`: Enable debug mode for detailed logging (default: `false`).

## Running the Application

To run the application, use the following command:

```sh
./kafka-http-receiver --http=:8080 --kafka=localhost:9092 --debug=true
```

### Endpoints

- **POST /receiver/post/{topic}**: Receives a JSON payload and forwards it to the specified Kafka topic.
- **GET /metrics**: Prometheus metrics endpoint providing detailed monitoring information.
- **GET /health**: Health check endpoint to verify the service's connectivity with Kafka.

## Example

1. Start the Kafka HTTP Receiver Service:

    ```sh
    ./kafka-http-receiver --http=:8080 --kafka=localhost:9092 --debug=true
    ```

2. Send a JSON payload to the service:

    ```sh
    curl -X POST http://localhost:8080/receiver/post/my-topic -H "Content-Type: application/json" -d '{"key": "value"}'
    ```

3. Check the Prometheus metrics:

    Open your browser and navigate to `http://localhost:8080/metrics`.

4. Check the health of the service:

    Open your browser and navigate to `http://localhost:8080/health`.

## Monitoring and Metrics

Prometheus metrics provide insights into the service's performance and health. The following metrics are available:

- `kafka_messages_total{topic}`: Total number of messages sent to Kafka for each topic.
- `kafka_message_errors_total{topic}`: Total number of message errors in Kafka for each topic.
- `kafka_message_size_bytes{topic}`: Size of messages sent to Kafka for each topic.
- `kafka_message_delivery_seconds{topic}`: Time taken to deliver messages to Kafka for each topic.
- `http_requests_total{path}`: Total number of HTTP requests for each path.
- `http_request_duration_seconds{path}`: Duration of HTTP requests for each path.
- `http_request_errors_total{path}`: Total number of HTTP request errors for each path.

## Health Check

The `/health` endpoint provides a simple health check mechanism. It returns `HTTP 200 OK` if the service is running and can connect to the Kafka broker. If the service cannot connect to Kafka, it returns `HTTP 503 Service Unavailable`.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any bugs or enhancements.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.