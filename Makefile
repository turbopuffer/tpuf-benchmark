sqlc:
	go tool sqlc generate

nightly: sqlc
	mkdir -p bin
	go build -o bin/nightly ./cmd/nightly