
install:
	go install ./cmds/engine
	go install ./cmds/ectl

run:
	go run ./cmds/engine -L debug -d local/db

test:
	go test ./...

