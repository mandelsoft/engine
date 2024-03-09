package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/mandelsoft/engine/pkg/database/service"
)

func ResponseData(r *http.Response) ([]byte, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if r.StatusCode == http.StatusCreated || r.StatusCode == http.StatusOK {
		return data, nil
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("request failed with status %s", r.Status)
	}

	var msg service.Error
	err = json.Unmarshal(data, &msg)
	if err != nil {
		return nil, fmt.Errorf("request failed with status %s", r.Status)
	}
	return nil, fmt.Errorf("%s", msg.Error)
}
