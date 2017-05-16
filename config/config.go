package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

var cfg *Config

func init() {
	cfg = new(Config)
}

func GetConfig() *Config {
	return cfg
}

type Config struct {
	LevelDbPath string `json:"leveldbpath"`
}

func (c *Config) LoadConfig(filepath string) error {
	if filepath == "" {
		return errors.New("file path is empty")
	}
	fi, err := os.Stat(filepath)
	if err != nil {
		return err
	}

	if fi == nil {
		return errors.New("file not usable")
	}

	if fi.IsDir() == true {
		return errors.New("file is dir")
	}

	if fi.Size() <= 0 {
		return errors.New("file is empty")
	}
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}

	json.Unmarshal(data, c)
	return nil
}
