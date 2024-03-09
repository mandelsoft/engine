package main

import (
	"fmt"
	"os"

	"github.com/mandelsoft/engine/cmds/ectl/app"
)

func Error(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+msg, args...)
	os.Exit(1)
}

func main() {
	cmd := app.New()
	cmd.SetArgs(os.Args[1:])
	err := cmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(1)
	}
}
