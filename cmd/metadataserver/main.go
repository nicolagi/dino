package main

import (
	"flag"
	"os"
	"os/signal"

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

	opts, err := loadOptionsFromFile(*optsFile)
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

	store, err := storage.NewBuilder(opts.Stores).StoreByName(opts.Backend)
	if err != nil {
		log.Fatalf("Could not instantiate backend store: %v", err)
	}

	metadataStore := storage.NewVersionedWrapper(store)

	srvOpts := []server.Option{
		server.WithAddress(opts.ListenAddress),
		server.WithVersionedStore(metadataStore),
	}

	if opts.KeyFile != "" {
		srvOpts = append(srvOpts, server.WithKeyPair(
			os.ExpandEnv(opts.CertFile),
			os.ExpandEnv(opts.KeyFile),
		))
	}
	if opts.AuthHash != "" {
		srvOpts = append(srvOpts, server.WithAuthHash(opts.AuthHash))
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
