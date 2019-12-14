package server

import (
	"io"
	"net"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/storage"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/bcrypt"
)

type serverConn struct {
	id     uint16
	server *Server

	conn    net.Conn
	encoder *message.Encoder
	decoder *message.Decoder

	authorized bool
}

func (s *Server) wrapConn(conn net.Conn) *serverConn {
	return &serverConn{
		id:      s.connIDs.Next(),
		server:  s,
		conn:    conn,
		encoder: new(message.Encoder),
		decoder: new(message.Decoder),
	}
}

// To be run in a separate goroutine, which will exit when the connection is
// closed or reset.
func (sc *serverConn) handleInput() {
	for {
		var input message.Message
		if err := sc.decoder.Decode(sc.conn, &input); err != nil {
			// The following happens when the connection is closed on the client side.
			if err == io.EOF {
				log.WithFields(log.Fields{
					"err":    err,
					"id":     sc.id,
					"remote": sc.conn.RemoteAddr(),
					"local":  sc.conn.LocalAddr(),
				}).Info("Client detached")
				break
			}
			if operr, ok := err.(*net.OpError); ok {
				if operr.Err.Error() == "use of closed network connection" {
					log.WithFields(log.Fields{
						"err":    operr,
						"id":     sc.id,
						"remote": sc.conn.RemoteAddr(),
						"local":  sc.conn.LocalAddr(),
					}).Info("Client detached")
					break
				}
			}
			log.Warn(err)
			continue
		}
		var output message.Message
		if sc.server.opts.authHash != "" && !sc.authorized {
			switch {
			case input.Kind() != message.KindAuth:
				output = message.NewErrorMessage(input.Tag(), "go away, bad message type")
			case bcrypt.CompareHashAndPassword([]byte(sc.server.opts.authHash), []byte(input.Value())) == nil:
				sc.authorized = true
				output = message.NewAuthMessage(input.Tag(), "")
			default:
				output = message.NewErrorMessage(input.Tag(), "go away, bad password")
			}
		} else {
			output = storage.ApplyMessage(sc.server.opts.store, input)
		}
		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithFields(log.Fields{
				"input":  input,
				"output": output,
			}).Debug("Handled message")
		}
		if err := sc.encoder.Encode(sc.conn, output); err != nil {
			log.Warn(err)
		}
		if input.Kind() == message.KindPut && output.Kind() == message.KindPut {
			// All these goroutines will serialize on the fan-out mutex. It might be
			// better to use a buffered channel to write to here instead of piling up
			// goroutines.
			go sc.server.broadcast(sc.id, output)
		}
	}
	// Since we're no longer handling input, deregister this connection from
	// notification.
	sc.server.removeConn(sc)
}

func (sc *serverConn) close() {
	if err := sc.conn.Close(); err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Warn("Could not close connection")
	}
}
