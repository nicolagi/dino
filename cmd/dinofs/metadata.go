package main

import (
	"bytes"
	"errors"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/nicolagi/dino/bits"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

func (node *dinoNode) serialize() []byte {
	// Could use a pool of buffers, to be reused, instead of putting pressure on
	// the GC.
	size := 24 + len(node.contentKey)
	for attr, value := range node.xattrs {
		size += 4 + len(attr) + len(value)
	}
	for childName := range node.children {
		size += 4 + nodeKeyLen + len(childName)
	}
	buf := make([]byte, size)
	b := buf
	b = bits.Put32(b, node.user)
	b = bits.Put32(b, node.group)
	b = bits.Put32(b, node.mode)
	b = bits.Put64(b, uint64(node.time.UnixNano()))
	b = bits.Putb(b, node.contentKey)
	b = bits.Put16(b, uint16(len(node.xattrs)))
	for attr, value := range node.xattrs {
		b = bits.Puts(b, attr)
		b = bits.Putb(b, value)
	}
	for childName, childNode := range node.children {
		b = bits.Puts(b, childName)
		b = bits.Putb(b, childNode.key[:])
	}
	return buf
}

func (node *dinoNode) unserialize(b []byte) {
	node.user, b = bits.Get32(b)
	node.group, b = bits.Get32(b)
	node.mode, b = bits.Get32(b)
	var unixnano uint64
	unixnano, b = bits.Get64(b)
	node.time = time.Unix(0, int64(unixnano))
	node.contentKey, b = bits.Getb(b)
	if node.mode&fuse.S_IFDIR != 0 {
		node.children = make(map[string]*dinoNode)
	}
	var nxattr uint16
	nxattr, b = bits.Get16(b)
	if nxattr > 0 {
		node.xattrs = make(map[string][]byte)
	}
	for ; nxattr > 0; nxattr-- {
		var attr string
		var value []byte
		attr, b = bits.Gets(b)
		value, b = bits.Getb(b)
		node.xattrs[attr] = value
	}
	if len(b) > 0 {
		var childName string
		var childKey []byte
		for len(b) > 0 {
			childName, b = bits.Gets(b)
			childKey, b = bits.Getb(b)
			var key [nodeKeyLen]byte
			copy(key[:], childKey)
			node.children[childName] = node.factory.existingNode(childName, key)
		}
	}
}

func (node *dinoNode) saveMetadata() error {
	value := node.serialize()
	err := node.factory.metadata.Put(node.version+1, node.key[:], value)
	if err != nil {
		return err
	}
	node.version++
	return nil
}

func (node *dinoNode) loadMetadata(key [nodeKeyLen]byte) error {
	version, b, err := node.factory.metadata.Get(key[:])
	if err != nil {
		return err
	}
	node.key = key
	node.version = version
	node.unserialize(b)
	return nil
}

func (node *dinoNode) sync() syscall.Errno {
	if node.shouldSaveContent {
		var err error
		prev := node.contentKey
		node.contentKey, err = node.factory.blobs.Put(node.content)
		if err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("Could not save content")
			return syscall.EIO
		}
		node.shouldSaveContent = false
		if !bytes.Equal(prev, node.contentKey) {
			node.shouldSaveMetadata = true
		}
	}
	if node.shouldSaveMetadata {
		err := node.saveMetadata()
		if err != nil {
			if errors.Is(err, storage.ErrStalePut) {
				node.shouldReloadMetadata = true
			}
			log.WithFields(log.Fields{
				"err": err,
			}).Error("Could not save metadata")
			return syscall.EIO
		}
		node.shouldSaveMetadata = false
	}
	return fs.OK
}
