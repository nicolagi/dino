package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/google/gops/agent"
	"github.com/nicolagi/dino/metadata/server"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

func main() {
	optsFile := flag.String("config", os.ExpandEnv("$HOME/lib/dino/metadataserver.config"), "location of configuration file")
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	opts, err := loadOptions(*optsFile)
	if err != nil {
		log.Fatalf("Loading configuration from %q: %v", *optsFile, err)
	}

	if opts.Debug {
		log.SetLevel(log.DebugLevel)
	}

	if err := agent.Listen(agent.Options{}); err != nil {
		log.WithField("err", err).Warn("Could not start gops agent")
	} else {
		defer agent.Close()
	}

	dir := os.ExpandEnv("$HOME/lib/dino")
	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Fatalf("Could not ensure directory %q exists: %v", dir, err)
	}
	file := filepath.Join(dir, fmt.Sprintf("storage-%s.db", opts.Name))
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		log.Fatalf("Could not open database %q: %v", file, err)
	}
	store, err := storage.NewBoltStore(db)
	if err != nil {
		log.Fatalf("Could not instantiate boltdb store at %q: %v", file, err)
	}
	metadataStore := storage.NewVersionedWrapper(store)
	defer func() {
		if err := db.Close(); err != nil {
			log.Warnf("Could not close boltdb database: %v", err)
		}
	}()

	srvOpts := []server.Option{
		server.WithAddress(opts.MetadataServer),
		server.WithVersionedStore(metadataStore),
	}
	if opts.KeyPair.KeyFile != "" {
		srvOpts = append(srvOpts, server.WithKeyPair(
			os.ExpandEnv(opts.KeyPair.CertFile),
			os.ExpandEnv(opts.KeyPair.KeyFile),
		))
	}
	srv := server.New(srvOpts...)
	addr, err := srv.Listen()
	if err != nil {
		log.Fatal(err)
	}
	log.WithFields(log.Fields{"addr": addr}).Info("Listening")

	// Before we call srv.Serve(), which never returns unless srv.Shutdown() is
	// called, we need to install a signal handler to call srv.Shutdown().
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		sig := <-c
		log.WithField("signal", sig).Info("Shutting down server")
		// Will make srv.Serve() return, and allow deferred clean-up functions to
		// execute.
		if err := srv.Shutdown(); err != nil {
			log.WithFields(log.Fields{"err": err}).Warn("Could not shut down the server cleanly")
		}
	}()

	if err := srv.Serve(); err != nil {
		log.Error(err)
	}
}
