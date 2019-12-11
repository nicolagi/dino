package storage

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

// BoltStore is an implementation of Store whose backend is a Bolt database.
type BoltStore bolt.DB

var (
	bucketName = []byte("nodes")
)

func init() {
	registerBuilder("boltdb", func(builder *Builder, config map[string]interface{}) (store Store, err error) {
		file, err := builder.getString(config, "file")
		if err != nil {
			return nil, err
		}
		file = os.ExpandEnv(file)
		db, err := bolt.Open(file, 0600, nil)
		if err != nil {
			return nil, fmt.Errorf("could not open database %q: %w", file, err)
		}
		return NewBoltStore(db)
	})
}

func NewBoltStore(db *bolt.DB) (*BoltStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return fmt.Errorf("could not ensure bucket %q exists: %w", bucketName, err)
		}
		return nil
	})
	return (*BoltStore)(db), err
}

func (s *BoltStore) Put(key []byte, value []byte) error {
	return (*bolt.DB)(s).Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketName).Put(key, value); err != nil {
			return fmt.Errorf("could not put %.40q with %.40q: %w", key, value, err)
		}
		return nil
	})
}

func (s *BoltStore) Get(key []byte) (value []byte, err error) {
	err = (*bolt.DB)(s).View(func(tx *bolt.Tx) error {
		value = tx.Bucket(bucketName).Get(key)
		if value == nil {
			return fmt.Errorf("%.40q: %w", key, ErrNotFound)
		}
		return nil
	})
	return value, err
}
