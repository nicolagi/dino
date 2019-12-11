package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

// RemoteStore implements Store. It requires to connect to a blobserver.
type RemoteStore struct {
	address string
}

func init() {
	registerBuilder("remote", func(builder *Builder, config map[string]interface{}) (store Store, err error) {
		address, err := builder.getString(config, "address")
		if err != nil {
			return nil, err
		}
		return NewRemoteStore(address), nil
	})
}

func NewRemoteStore(address string) *RemoteStore {
	return &RemoteStore{address: address}
}

func (r *RemoteStore) Put(key, value []byte) (err error) {
	url := r.pathFor(key)
	request, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(value))
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if response != nil && response.Body != nil {
		defer func() {
			_ = response.Body.Close()
		}()
	}
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}
	return nil
}

func (r *RemoteStore) Get(key []byte) (value []byte, err error) {
	url := r.pathFor(key)
	response, err := http.Get(url)
	if response != nil && response.Body != nil {
		defer func() {
			_ = response.Body.Close()
		}()
	}
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, errors.New(string(body))
	}
	return body, nil
}

func (r *RemoteStore) pathFor(key []byte) string {
	return fmt.Sprintf("http://%s/%x", r.address, key)
}
