
install:
	go install ./cmds/engine
	go install ./cmds/ectl

run:
	go run ./cmds/engine -L debug -d local/db -F ./ui -P /watch -c

test:
	go test ./...

