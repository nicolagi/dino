package server

import (
	"crypto/tls"
	"errors"
	"net"
	"sync"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
)

var ErrPasswordWithoutTLS = errors.New("must use TLS if authorization is required")

type Option func(*options)

type options struct {
	address string
	store   storage.VersionedStore

	tls      bool
	certFile string
	keyFile  string

	// If non-empty, the server will require a successful auth message exchange
	// before any other message on a client connection. Only TLS connections can
	// be used in this case.
	authHash string
}

func WithAddress(value string) Option {
	return func(o *options) {
		o.address = value
	}
}

func WithVersionedStore(value storage.VersionedStore) Option {
	return func(o *options) {
		o.store = value
	}
}

func WithKeyPair(certFile, keyFile string) Option {
	return func(o *options) {
		o.tls = true
		o.certFile = certFile
		o.keyFile = keyFile
	}
}

func WithAuthHash(value string) Option {
	return func(o *options) {
		o.authHash = value
	}
}

type Server struct {
	opts    options
	ln      net.Listener
	connIDs *message.MonotoneTags
	mu      sync.Mutex
	conns   []*serverConn
}

func New(opts ...Option) *Server {
	s := &Server{
		connIDs: message.NewMonotoneTags(),
	}
	s.opts.address = ":6660"
	for _, o := range opts {
		o(&s.opts)
	}
	return s
}

func (s *Server) Listen() (addr string, err error) {
	if s.opts.tls {
		var c tls.Certificate
		c, err = tls.LoadX509KeyPair(s.opts.certFile, s.opts.keyFile)
		if err == nil {
			s.ln, err = tls.Listen("tcp", s.opts.address, &tls.Config{
				Certificates: []tls.Certificate{c},
			})
		}
	} else {
		if s.opts.authHash != "" {
			return "", ErrPasswordWithoutTLS
		}
		s.ln, err = net.Listen("tcp", s.opts.address)
	}
	if err != nil {
		return
	}
	addr = s.ln.Addr().String()
	return
}

// Serve listens and spawns a server goroutine for each incoming connection. The
// function will return (some time after) shutdown is called.
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			var noe *net.OpError
			if errors.As(err, &noe) {
				if noe.Err.Error() == "use of closed network connection" {
					// shutdown must've been called. Interrupt the accept loop.
					break
				}
			}
			log.Error(err)
			continue
		}
		sc := s.wrapConn(conn)
		log.WithFields(log.Fields{
			"id":     sc.id,
			"remote": sc.conn.RemoteAddr(),
			"local":  sc.conn.LocalAddr(),
		}).Info("Client attached")
		s.mu.Lock()
		s.conns = append(s.conns, sc)
		s.mu.Unlock()
		// The goroutine will exit when the connection is closed.
		go sc.handleInput()
	}
	return nil
}

func (s *Server) removeConn(sc *serverConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Should consider using a map instead.
	var newConns []*serverConn
	for _, conn := range s.conns {
		if conn.id != sc.id {
			newConns = append(newConns, conn)
		}
	}
	s.conns = newConns
}

func (s *Server) broadcast(excluded uint16, m message.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	broadcastMessage := m.ForBroadcast()
	for _, conn := range s.conns {
		if excluded == conn.id {
			continue
		}
		if s.opts.authHash != "" && !conn.authorized {
			continue
		}
		logger := log.WithFields(log.Fields{
			"message":   broadcastMessage,
			"sender":    excluded,
			"recipient": conn.id,
			"local":     conn.conn.LocalAddr(),
			"remote":    conn.conn.RemoteAddr(),
		})
		// Note: We're re-encoding for all conns, that's a waste.
		// This calls for a refactoring.
		err := conn.encoder.Encode(conn.conn, broadcastMessage)
		if err != nil {
			// Never mind if a client didn't get the message. They are simply more likely
			// to send stale puts as a consequence of the missed update. They would also
			// see potentially stale content. Note: We should probably have a retry
			// queue.
			logger.WithField("err", err).Warn("Could not notify")
		} else {
			logger.Debug("Notified")
		}
	}
}

// Shutdown instructs the server to shutdown. This method will return
// immediately, while the server will have to be considered shut down only when
// Serve returns.
func (s *Server) Shutdown() error {
	// Stop accepting
	err := s.ln.Close()
	s.connIDs.Stop()
	// Stop accepted
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		conn.close()
	}
	return err
}
