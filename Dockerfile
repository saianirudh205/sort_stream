FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV KAFKA_HEAP_OPTS="-Xms512m -Xmx512m"

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jre \
    curl \
    bash \
    git \
    ca-certificates

# Install Go
RUN curl -fsSL https://go.dev/dl/go1.22.0.linux-amd64.tar.gz | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}"

# Install Kafka
WORKDIR /opt
RUN curl -fsSL https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz \
    | tar -xz
RUN mv kafka_2.13-3.6.1 kafka

# Copy app
WORKDIR /app
COPY . .

# Build Go binaries
RUN go build -o source source.go
RUN go build -o sort sort_topics.go

CMD ["bash", "start.sh"]
