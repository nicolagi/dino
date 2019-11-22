package main

import (
	"errors"
	"os"

	"github.com/rogpeppe/rjson"
)

var errIncompleteKeyPair = errors.New("must specify both cert file and key file or neither")

type options struct {
	Name           string `json:"name"`
	MetadataServer string `json:"metadata_server"`
	Debug          bool   `json:"debug"`
	KeyPair        struct {
		CertFile string `json:"cert_file"`
		KeyFile  string `json:"key_file"`
	} `json:"key_pair"`
}

func loadOptions(pathname string) (*options, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	var opts *options
	err = rjson.NewDecoder(f).Decode(&opts)
	if err != nil {
		return nil, err
	}
	if (opts.KeyPair.CertFile != "" && opts.KeyPair.KeyFile == "") || (opts.KeyPair.CertFile == "" && opts.KeyPair.KeyFile != "") {
		return nil, errIncompleteKeyPair
	}
	return opts, nil
}
