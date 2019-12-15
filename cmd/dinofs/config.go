package main

import (
	"fmt"
	"os"

	"github.com/rogpeppe/rjson"
)

type config struct {
	Mountpoint string `json:"mountpoint"`
	Name       string `json:"name"`
	Debug      bool   `json:"debug"`
	DebugFUSE  bool   `json:"debug_fuse"`
	LogPath    string `json:"log_path"`
	DataPath   string `json:"data_path"`

	Metadata struct {
		Type string `json:"type"`

		// Properties for "dino" type.
		Address string `json:"address"`

		// Authorize client connections with this key, if non-empty.
		AuthKey string `json:"auth_key"`

		// Properties for "dynamodb" type.
		Profile string `json:"profile"`
		Region  string `json:"region"`
		Table   string `json:"table"`
	} `json:"metadata"`

	Blobs struct {
		Type string `json:"type"`

		// Properties for "dino" type.
		Address string `json:"address"`

		// Properties for "s3" type.
		Profile string `json:"profile"`
		Region  string `json:"region"`
		Bucket  string `json:"bucket"`
	} `json:"blobs"`
}

func loadConfig(pathname string) (*config, error) {
	f, err := os.Open(pathname)
	if os.IsNotExist(err) {
		// Hack: If it doesn't exist, treat pathname as a shortcut.
		f, err = os.Open(os.ExpandEnv(fmt.Sprintf("$HOME/lib/dino/fs-%s.config", pathname)))
	}
	if err != nil {
		return nil, err
	}
	var c *config
	err = rjson.NewDecoder(f).Decode(&c)
	return c, err
}

func (c *config) applyDefaultsForMissingProperties() {
	if c.Mountpoint == "" {
		c.Mountpoint = "/n/dino"
	}
	if c.Name == "" {
		c.Name = "dinofs"
	}
	if c.LogPath == "" {
		c.LogPath = "$HOME/lib/dino/log"
	}
	if c.DataPath == "" {
		c.DataPath = "$HOME/lib/dino/data"
	}
}
