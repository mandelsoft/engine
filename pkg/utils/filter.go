package utils

import (
	"strconv"
)

func IsNoNumber(s string) bool {
	_, err := strconv.Atoi(s)
	return err != nil
}

func IsNumber(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}
