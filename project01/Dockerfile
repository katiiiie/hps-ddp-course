FROM golang:1.25

ENV GOTMPDIR=/go/tmp
RUN mkdir -p /go/tmp

RUN apt-get update && \
    apt-get install -y rabbitmq-server

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
