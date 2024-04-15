
install:
	go install ./cmds/engine
	go install ./cmds/ectl

run-steps:
	go run ./cmds/engine -L debug -d local/db -F ./ui -P /watch -c -D 1s 2>&1 | tee local/log

run:
	go run ./cmds/engine -L debug -d local/db -F ./ui -P /watch -c 2>&1 | tee local/logrun:

run-ui:
	@sleep 10
	firefox http://localhost:8080/ui/

test:
	go test ./...

