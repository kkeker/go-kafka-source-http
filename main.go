package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	HTTPAddress string
	KafkaConfig KafkaConfig
	Debug       bool
}

type KafkaConfig struct {
	Broker string
}

var (
	kafkaProducer  *kafka.Producer
	messageCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_total",
			Help: "Total number of messages sent to Kafka",
		},
		[]string{"topic"},
	)
	messageErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_message_errors_total",
			Help: "Total number of message errors in Kafka",
		},
		[]string{"topic"},
	)
	messageSizes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_message_size_bytes",
			Help:    "Size of messages sent to Kafka",
			Buckets: prometheus.ExponentialBuckets(100, 10, 5), // 100 bytes to ~100 MB
		},
		[]string{"topic"},
	)
	messageDeliveryTimes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_message_delivery_seconds",
			Help:    "Time taken to deliver messages to Kafka",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic"},
	)
	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"path"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)
	httpRequestErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_request_errors_total",
			Help: "Total number of HTTP request errors",
		},
		[]string{"path"},
	)
	kafkaHealthy int32 // 0 indicates unhealthy, 1 indicates healthy
)

func main() {
	// Defining Command Line Flags
	httpAddress := flag.String("http", ":8080", "HTTP server address")
	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker address")
	debug := flag.Bool("debug", false, "Enable debug mode")

	// Parsing flags
	flag.Parse()

	// Configuration
	config := Config{
		HTTPAddress: *httpAddress,
		KafkaConfig: KafkaConfig{
			Broker: *kafkaBroker,
		},
		Debug: *debug,
	}

	// Prometheus metrics logging
	prometheus.MustRegister(messageCounter)
	prometheus.MustRegister(messageErrors)
	prometheus.MustRegister(messageSizes)
	prometheus.MustRegister(messageDeliveryTimes)
	prometheus.MustRegister(httpRequests)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(httpRequestErrors)

	// Initializing Kafka producer
	var err error
	kafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": config.KafkaConfig.Broker})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %s", err)
	}
	defer kafkaProducer.Close()

	// Updating the health status of Kafka producer
	atomic.StoreInt32(&kafkaHealthy, 1)

	if config.Debug {
		log.Printf("Kafka broker: %s", config.KafkaConfig.Broker)
		log.Printf("HTTP server address: %s", config.HTTPAddress)
	}

	// Goroutine for processing messages from Kafka
	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					messageErrors.WithLabelValues(*ev.TopicPartition.Topic).Inc()
					logError(config.Debug, "Delivery failed: %v\n", ev.TopicPartition)
				} else {
					messageDeliveryTimes.WithLabelValues(*ev.TopicPartition.Topic).Observe(time.Since(ev.Timestamp).Seconds())
					logDebug(config.Debug, "Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// HTTP handler for sending messages
	http.HandleFunc("/receiver/post/", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		path := r.URL.Path

		httpRequests.WithLabelValues(path).Inc()

		if r.Method != http.MethodPost {
			httpRequestErrors.WithLabelValues(path).Inc()
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		// Extracting topic name from URL
		pathSegments := strings.Split(r.URL.Path, "/")
		if len(pathSegments) < 4 || pathSegments[3] == "" {
			httpRequestErrors.WithLabelValues(path).Inc()
			http.Error(w, "Topic name is missing in URL", http.StatusBadRequest)
			return
		}
		topic := pathSegments[3]

		if config.Debug {
			log.Printf("Received request for topic: %s", topic)
		}

		var msg interface{}
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			httpRequestErrors.WithLabelValues(path).Inc()
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		msgBytes, err := json.Marshal(msg)
		if err != nil {
			httpRequestErrors.WithLabelValues(path).Inc()
			http.Error(w, "Failed to encode JSON", http.StatusInternalServerError)
			return
		}

		go func(topic string, msgBytes []byte) {
			// Checking and creating a topic if it doesn't exist
			err := ensureTopicExists(config.KafkaConfig.Broker, topic, config.Debug)
			if err != nil {
				messageErrors.WithLabelValues(topic).Inc()
				logError(config.Debug, "Failed to ensure topic exists: %v", err)
				return
			}

			deliveryChan := make(chan kafka.Event, 1)
			defer close(deliveryChan)

			err = kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          msgBytes,
			}, deliveryChan)

			if err != nil {
				messageErrors.WithLabelValues(topic).Inc()
				logError(config.Debug, "Failed to produce message: %v", err)
				return
			}

			messageSizes.WithLabelValues(topic).Observe(float64(len(msgBytes)))

			// Waiting for delivery event
			event := <-deliveryChan
			msgEvent := event.(*kafka.Message)

			if msgEvent.TopicPartition.Error != nil {
				messageErrors.WithLabelValues(topic).Inc()
				logError(config.Debug, "Failed to deliver message: %v", msgEvent.TopicPartition.Error)
			} else {
				messageCounter.WithLabelValues(topic).Inc()
				logDebug(config.Debug, "Message delivered to topic %s [%d] at offset %v",
					*msgEvent.TopicPartition.Topic, msgEvent.TopicPartition.Partition, msgEvent.TopicPartition.Offset)
			}
		}(topic, msgBytes)

		duration := time.Since(startTime).Seconds()
		httpRequestDuration.WithLabelValues(path).Observe(duration)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Message sent to Kafka"))
	})

	// HTTP handler for Prometheus metrics
	http.Handle("/metrics", promhttp.Handler())

	// HTTP handler for health check
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&kafkaHealthy) == 1 {
			_, err := kafkaProducer.GetMetadata(nil, false, 5000)
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(fmt.Sprintf("Kafka is not healthy: %v", err)))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Kafka is healthy"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Kafka is not healthy"))
		}
	})

	// Starting HTTP Server
	log.Printf("Starting server on %s", config.HTTPAddress)
	if err := http.ListenAndServe(config.HTTPAddress, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %s", err)
	}
}

// ensureTopicExists checks for the existence of a topic and creates it if it does not exist
func ensureTopicExists(broker, topic string, debug bool) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer adminClient.Close()
	// Checking the topic availability
	metadata, err := adminClient.GetMetadata(&topic, false, 5000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %v", err)
	}

	for _, t := range metadata.Topics {
		if t.Topic == topic {
			if debug {
				log.Printf("Topic %s already exists", topic)
			}
			return nil
		}
	}

	// Create a topic if it doesn't exist
	topicConfig := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	_, err = adminClient.CreateTopics(context.TODO(), []kafka.TopicSpecification{topicConfig})
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	if debug {
		log.Printf("Topic %s created successfully", topic)
	}
	return nil

}

// logDebug prints a message to the log only if debug mode is enabled.
func logDebug(debug bool, format string, v ...interface{}) {
	if debug {
		log.Printf(format, v...)
	}
}

// logError prints an error message to the log
func logError(_ bool, format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}
