.PHONY: up down build producer consumer tidy

up:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 20

down:
	docker-compose down -v

tidy:
	go mod tidy

build:
	go build -o bin/producer ./cmd/producer
	go build -o bin/consumer ./cmd/consumer

producer:
	go run ./cmd/producer

consumer:
	go run ./cmd/consumer

run: up
	@echo "Starting consumer in background..."
	go run ./cmd/consumer &
	@sleep 2
	@echo "Running producer..."
	go run ./cmd/producer
