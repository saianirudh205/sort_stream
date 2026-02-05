# Kafka Sort

A Dockerized Kafka application for sorting and processing data streams.

## Prerequisites

- Git
- Docker
- Docker Compose (if applicable)

## Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <repository-name>
```

Or pull the latest changes if you already have the repo:

```bash
git pull origin latest_dev
```

### 2. Build the Docker Image

```bash
sudo docker build -t kafka-sort .
```

### 3. Run the Container

```bash
sudo docker run --name kafka-sort --rm -it \
  --memory=2g \
  --memory-swap=2g \
  --cpus=4 \
  kafka-sort
```

**Container Resource Limits:**
- Memory: 2GB
- Swap: 2GB
- CPUs: 4 cores

### 4. Automatic Execution

Once the container starts:
- The application runs automatically
- Test cases execute for the first 20 packets
- Results are published to Kafka topics

## Testing & Monitoring

### Consume Messages from Kafka Topics

#### Option 1: From Host Machine

Connect to Kafka on `localhost:9092` using your preferred Kafka consumer.

#### Option 2: Using Kafka Console Consumer

Access the container shell and run:

```bash
# Consume messages from the 'id' topic
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic id \
  --from-beginning \
  --max-messages 20
```

### Check Topic Offsets

To view current offsets for a topic:

```bash
/opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic <topic-name>
```

## Available Topics

- `id` - Contains sorted packet IDs (sample output available for first 20 messages)
- so as `name' , `continent`

## Troubleshooting

- **Container exits immediately**: Check logs with `sudo docker logs kafka-sort`
- **Port conflicts**: Ensure port 9092 is available on your host machine






## Contributing

[Add contribution guidelines here]
