package server

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"testing/quick"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/storage"
	"github.com/stretchr/testify/assert"
)

// fakeConn implements net.Conn.
type fakeConn struct {
	encoder  message.Encoder
	decoder  message.Decoder
	enqueuer bytes.Buffer
	reader   io.Reader
	writer   bytes.Buffer
}

func (fc *fakeConn) sendMessage(t *testing.T, m message.Message) {
	if err := fc.encoder.Encode(&fc.enqueuer, m); err != nil {
		t.Log(m)
		t.Fatal(err)
	}
}

func (fc *fakeConn) freeze() {
	fc.reader = bytes.NewReader(fc.enqueuer.Bytes())
}

func (fc *fakeConn) receiveMessages(t *testing.T) (responses []message.Message) {
	outputReader := bytes.NewReader(fc.writer.Bytes())
	for {
		var r message.Message
		if err := fc.decoder.Decode(outputReader, &r); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		responses = append(responses, r)
	}
	return
}

func (fc *fakeConn) Read(b []byte) (n int, err error) {
	return fc.enqueuer.Read(b)
}

func (fc *fakeConn) Write(b []byte) (n int, err error) {
	return fc.writer.Write(b)
}

func (fc *fakeConn) Close() error {
	return nil
}

func (fc *fakeConn) LocalAddr() net.Addr {
	return &net.IPAddr{}
}

func (fc *fakeConn) RemoteAddr() net.Addr {
	return &net.IPAddr{}
}

func (fc *fakeConn) SetDeadline(t time.Time) error {
	return nil
}

func (fc *fakeConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (fc *fakeConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestServerConn(t *testing.T) {
	t.Run("when authorization is not required", func(t *testing.T) {
		t.Run("can do put/get right away", func(t *testing.T) {
			conn := fakeConn{}
			sc := serverConn{
				conn:    &conn,
				encoder: &message.Encoder{},
				decoder: &message.Decoder{},
				server: &Server{
					opts: options{
						store: storage.NewVersionedWrapper(storage.NewInMemoryStore()),
					},
				},
			}

			conn.sendMessage(t, message.NewPutMessage(1, "name", "tony", 1))
			conn.sendMessage(t, message.NewGetMessage(2, "name"))
			conn.sendMessage(t, message.NewGetMessage(3, "surname"))
			conn.freeze()

			assert.False(t, sc.authorized)
			sc.handleInput()
			assert.False(t, sc.authorized)

			responses := conn.receiveMessages(t)
			assert.Len(t, responses, 3)
			assert.Equal(t, message.NewPutMessage(1, "name", "tony", 1), responses[0])
			assert.Equal(t, message.NewPutMessage(2, "name", "tony", 1), responses[1])
			assert.Equal(t, message.NewErrorMessage(3, `"surname": not found`), responses[2])
		})
		t.Run("auth messages get an error response", func(t *testing.T) {
			conn := fakeConn{}
			sc := serverConn{
				conn:    &conn,
				encoder: &message.Encoder{},
				decoder: &message.Decoder{},
				server: &Server{
					opts: options{
						store: storage.NewVersionedWrapper(storage.NewInMemoryStore()),
					},
				},
			}

			conn.sendMessage(t, message.NewAuthMessage(1, "hello"))
			conn.sendMessage(t, message.NewAuthMessage(2, "world"))
			conn.freeze()

			assert.False(t, sc.authorized)
			sc.handleInput()
			assert.False(t, sc.authorized)

			responses := conn.receiveMessages(t)
			assert.Len(t, responses, 2)
			assert.Equal(t, message.NewErrorMessage(1, "messages of kind AUTH cannot be applied"), responses[0])
			assert.Equal(t, message.NewErrorMessage(2, "messages of kind AUTH cannot be applied"), responses[1])
		})
	})
	t.Run("when authorization is required", func(t *testing.T) {
		t.Run("error responses for non-auth message or auth message with non-matching password", func(t *testing.T) {
			err := quick.Check(func(request message.Message) bool {
				conn := fakeConn{}
				sc := serverConn{
					conn:    &conn,
					encoder: &message.Encoder{},
					decoder: &message.Decoder{},
					server: &Server{
						opts: options{
							authHash: "non empty",
						},
					},
				}
				conn.sendMessage(t, request)
				conn.freeze()
				sc.handleInput()
				responses := conn.receiveMessages(t)
				return responses[0].Kind() == message.KindError
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
		})
		t.Run("after successful auth message, can put and get", func(t *testing.T) {
			conn := fakeConn{}
			sc := serverConn{
				conn:    &conn,
				encoder: &message.Encoder{},
				decoder: &message.Decoder{},
				server: &Server{
					opts: options{
						store: storage.NewVersionedWrapper(storage.NewInMemoryStore()),
						// A possible hash for "foobar".
						authHash: "$2a$10$xdMaS2UL7abbg2sgnjhR3.aOXpKlg4R3z2XRQoA9MRRTF0I5NrDNy",
					},
				},
			}

			conn.sendMessage(t, message.NewAuthMessage(1, "foobar"))
			conn.sendMessage(t, message.NewPutMessage(2, "name", "tony", 1))
			conn.freeze()

			assert.False(t, sc.authorized)
			sc.handleInput()
			assert.True(t, sc.authorized)

			responses := conn.receiveMessages(t)
			assert.Len(t, responses, 2)
			assert.Equal(t, message.NewAuthMessage(1, ""), responses[0])
			assert.Equal(t, message.NewPutMessage(2, "name", "tony", 1), responses[1])
		})
	})
}
