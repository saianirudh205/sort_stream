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
#WORKDIR /opt
RUN curl -fsSL https://dlcdn.apache.org/kafka/3.7.2/kafka_2.13-3.7.2.tgz \
    | tar -xz
RUN mv kafka_2.13-3.7.2 kafka

# Copy app
WORKDIR /app
COPY . .

# ⚠️ This generates go.sum INSIDE the image
RUN go mod tidy
RUN go mod download
# Build Go binaries
RUN go build -o source batch_and_send.go
RUN go build -o sort test.go

CMD ["bash", "start.sh"]
