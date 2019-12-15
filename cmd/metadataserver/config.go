package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"golang.org/x/crypto/bcrypt"
)

var errIncompleteKeyPair = errors.New("must specify both cert file and key file or neither")

type options struct {
	Debug         bool
	ListenAddress string `toml:"listen_address"`

	// The backend property defines the backend  where the data handled by
	// the server is actually stored. This can be any key-value store,
	// defined in the Stores property.
	Backend string

	// The two properties below are for TLS. Specify both or none (in case
	// you don't want TLS).
	CertFile string `toml:"cert_file"`
	KeyFile  string `toml:"key_file"`

	// If non-empty, the server will require a successful exchange of auth
	// messages before any put/get messages.
	AuthHash string `toml:"auth_hash"`

	// Stores defines any number of storage.Store implementations that are
	// referenced by name in the rest of the configuration. The configuration is
	// handled by github.com/nicolagi/dino/storage.Builder. Also see the init
	// functions in that package, to see what configuration properties are
	// available/needed for each individual store.
	Stores map[string]interface{}
}

func loadOptionsFromFile(pathname string) (*options, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return loadOptions(f)
}

func loadOptions(r io.Reader) (*options, error) {
	var opts options
	decoderMetadata, err := toml.DecodeReader(r, &opts)
	if err != nil {
		return nil, err
	}
	var undecoded []toml.Key
	for _, k := range decoderMetadata.Undecoded() {
		if !strings.HasPrefix(k.String(), "stores") {
			undecoded = append(undecoded, k)
		}
	}
	if ulen := len(undecoded); ulen != 0 {
		return nil, fmt.Errorf("%d undecoded keys: %v", ulen, undecoded)
	}
	if (opts.CertFile != "" && opts.KeyFile == "") || (opts.CertFile == "" && opts.KeyFile != "") {
		return nil, errIncompleteKeyPair
	}
	if opts.AuthHash != "" {
		// There's no explicit validation of hashes in the bcrypt
		// package.  But I suspect bcrypt.Cost() will return an error
		// on invalid hashes, so let's use that as a proxy for
		// "validate".
		if _, err := bcrypt.Cost([]byte(opts.AuthHash)); err != nil {
			return nil, fmt.Errorf("invalid auth hash: %w", err)
		}
	}
	return &opts, nil
}
