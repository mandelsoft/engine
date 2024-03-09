
install:
	go install ./cmds/engine
	go install ./cmds/ectl

run:
	engine -d local/db
