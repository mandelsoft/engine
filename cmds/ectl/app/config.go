package app

import (
	"os"
	"path/filepath"

	"github.com/mandelsoft/goutils/generics"
	"sigs.k8s.io/yaml"
)

type Config struct {
	Namespace *string `json:"namespace,omitempty"`
	Server    *string `jso:"server,omitempty"`
}

func GetConfig() *Config {
	var cfg Config

	dir, err := os.UserHomeDir()
	if err == nil {
		Mergeonfig(&cfg, ReadConfig(filepath.Join(dir, ".ectl")))
	}
	dir, err = os.UserConfigDir()
	if err == nil {
		Mergeonfig(&cfg, ReadConfig(filepath.Join(dir, ".ectl")))
	}
	if err == nil {
		Mergeonfig(&cfg, ReadConfig(".ectl"))
	}

	if v := os.Getenv("ENGINE_SERVER"); v != "" {
		cfg.Server = generics.Pointer(v)
	}
	if v := os.Getenv("ENGINE_NAMESPACE"); v != "" {
		cfg.Namespace = generics.Pointer(v)
	}
	if cfg.Server == nil || *cfg.Server == "" {
		cfg.Server = generics.Pointer("http://localhost:8080")
	}
	return &cfg
}

func ReadConfig(path string) *Config {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil
	}
	return &cfg
}

func Mergeonfig(cfg *Config, add *Config) {
	if add == nil {
		return
	}
	if add.Namespace != nil {
		cfg.Namespace = add.Namespace
	}
	if add.Server != nil {
		cfg.Server = add.Server
	}
}
