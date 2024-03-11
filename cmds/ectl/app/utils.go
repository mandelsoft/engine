package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/mandelsoft/engine/pkg/database"
	"github.com/mandelsoft/engine/pkg/database/service"
	"github.com/spf13/cobra"
)

func ResponseData(r *http.Response) ([]byte, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if r.StatusCode == http.StatusCreated || r.StatusCode == http.StatusOK || r.StatusCode == http.StatusAccepted {
		return data, nil
	}

	if r.StatusCode == http.StatusNotFound {
		return nil, database.ErrNotExist
	}
	if r.StatusCode == http.StatusConflict {
		return nil, database.ErrModified
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

func TweakCommand(cmd *cobra.Command) {
	cmd.SilenceUsage = true
	cmd.TraverseChildren = true
}
