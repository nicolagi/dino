package storage

import (
	"fmt"
)

type builderFunc func(builder *Builder, config map[string]interface{}) (Store, error)

var builderByType = make(map[string]builderFunc)

func registerBuilder(storeType string, storeBuilder builderFunc) {
	builderByType[storeType] = storeBuilder
}

type Builder struct {
	config map[string]interface{}
	stores map[string]Store
	errors map[string]error
}

func NewBuilder(config map[string]interface{}) *Builder {
	return &Builder{
		config: config,
		stores: make(map[string]Store),
		errors: make(map[string]error),
	}
}

func (b *Builder) StoreByName(name string) (Store, error) {
	if err := b.errors[name]; err != nil {
		return nil, err
	}
	if store := b.stores[name]; store != nil {
		return store, nil
	}
	store, err := b.storeByName(name)
	if err != nil {
		b.errors[name] = err
	} else {
		b.stores[name] = store
	}
	return store, err
}

func (b *Builder) storeByName(name string) (Store, error) {
	storeConfig, err := b.getMap(b.config, name)
	if err != nil {
		return nil, err
	}
	storeType, err := b.getString(storeConfig, "type")
	if err != nil {
		return nil, err
	}
	storeBuilder := builderByType[storeType]
	if storeBuilder == nil {
		return nil, fmt.Errorf("don't know how to build stores of type %q", storeType)
	}
	return storeBuilder(b, storeConfig)
}

func (*Builder) getMap(hash map[string]interface{}, key string) (map[string]interface{}, error) {
	v := hash[key]
	if v == nil {
		return nil, fmt.Errorf("missing key: %q", key)
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("key is not a map: %q", key)
	}
	return m, nil
}

func (*Builder) getString(hash map[string]interface{}, key string) (string, error) {
	v := hash[key]
	if v == nil {
		return "", fmt.Errorf("missing key: %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("key is not a string: %q", key)
	}
	return s, nil
}
