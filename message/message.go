package message

import (
	"fmt"
	"math/rand"
	"reflect"
	"unicode"
)

// Kind is a number representing the kind of a message—get, put, or error.
type Kind uint8

const (
	// KindGet is a message from the client to the server, stating the client wants
	// to know the latest version of the value for a given key. It is never sent
	// from the server to the client. This kind of message should only be issued
	// when the client does not have a version of the value for the given key, or
	// the client knows the version is stale, e.g., because of a put error.
	KindGet Kind = iota

	// KindPut is a message that can be sent both by the client and the server. It
	// is used by clients to update a key's corresponding value with a new version.
	// The server responds with the exact same put message if the put is accepted,
	// or with and error message. The server also fans out accepted put messages to
	// all clients that are connected, so they can keep up to date. This way,
	// clients should not need to issue get messages often.
	KindPut

	// KindError is a message that is only sent from the server to the client. That
	// can be in response to a get message (in case the requested key is not known)
	// or in response to a put message (in case the put version number is wrong).
	// The version number in a put message should match the one in the server,
	// proving that the client is up to date. If the put is stale, the client may
	// be overwriting some value, so the client should get the latest version and
	// possibly redo the put with the correct version, or give up the put). Other
	// error conditions might arise.
	KindError

	// KindAuth is sent with a password value from client to server (only over a
	// TLS connection) to authorize the client. It is sent with an empty value
	// from server to client upon successful authorization. If the password does
	// not match, the server response will be of KindError.
	KindAuth

	kindCount
)

// String implements fmt.Stringer.
func (k Kind) String() string {
	switch k {
	case KindGet:
		return "GET"
	case KindPut:
		return "PUT"
	case KindError:
		return "ERROR"
	case KindAuth:
		return "AUTH"
	default:
		return "UNKNOWN"
	}
}

type Message struct {
	// The kind of the message. Meaningful for all messages.
	kind Kind

	// Correlates requests with responses for a given client. (Surely one won't
	// have more than 65336 requests waiting for a response?) Messages from other
	// clients will be tagged zero. Meaningful for all messages. The zero tag is
	// reserved for broadcast messages (those that are not responses to requests).
	tag uint16

	// The key to get or put. Meaningful for get and put messages only.
	key string

	// The value for a put message; doubles as a textual description of the error
	// for error messages, and as the password in auth messages.
	value string

	// Version of the value. Meaningful only for put messages.
	version uint64
}

func repr(any string) string {
	const max = 11
	for i, r := range any {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			// Not printable.
			return repr(fmt.Sprintf("%x", any))
		}
		if i > max {
			// Printable, but too long.
			return any[:max-3] + "..."
		}
	}
	// Printable and short!
	return any
}

// String implements fmt.Stringer. Keys and values will be printed in hex form
// if they contain any non-printable character. Also, they will be clipped at 10
// runes (not necessarily 10 bytes).
func (m Message) String() string {
	switch m.kind {
	case KindGet:
		return fmt.Sprintf("kind=%v tag=%d key=%s", m.kind, m.tag, repr(m.key))
	case KindError:
		return fmt.Sprintf("kind=%v tag=%d value=%s", m.kind, m.tag, repr(m.value))
	case KindAuth:
		return fmt.Sprintf("kind=%v tag=%d value=%t", m.kind, m.tag, m.value != "")
	default:
		// KindPut and unknown messages use all fields.
		return fmt.Sprintf("kind=%v tag=%d key=%s value=%s version=%d", m.kind, m.tag, repr(m.key), repr(m.value), m.version)
	}
}

// Kind returns the kind of a message, which should inform how the message
// should be used.
func (m Message) Kind() Kind {
	return m.kind
}

// Tag returns the tag of a message (call for all message kinds). Used to
// correlate requests with responses.
func (m Message) Tag() uint16 {
	return m.tag
}

// Key returns a key-value pair's key from the message. Call only for
// KindGet and KindPut, else it'll panic.
func (m Message) Key() string {
	switch m.kind {
	case KindGet, KindPut:
		return m.key
	default:
		panic(m.accessorPanic("Key"))
	}
}

// Value returns a key-value pair's value from the message. Call only for
// KindAuth, KindError and KindPut, else it'll panic.
func (m Message) Value() string {
	switch m.kind {
	case KindAuth, KindError, KindPut:
		return m.value
	default:
		panic(m.accessorPanic("Value"))
	}
}

// Version returns the version of a key-value pair. Call only for KindPut
// messages, or it'll panic.
func (m Message) Version() uint64 {
	switch m.kind {
	case KindPut:
		return m.version
	default:
		panic(m.accessorPanic("Version"))
	}
}

func (m Message) accessorPanic(accessorName string) string {
	return fmt.Sprintf("cannot call .%s for message of kind %v", accessorName, m.kind)
}

// NewGetMessage constructs a message of KindGet kind.
func NewGetMessage(tag uint16, key string) Message {
	return Message{
		kind: KindGet,
		tag:  tag,
		key:  key,
	}
}

// NewPutMessage constructs a message of KindPut kind.
func NewPutMessage(tag uint16, key string, value string, version uint64) Message {
	return Message{
		kind:    KindPut,
		tag:     tag,
		key:     key,
		value:   value,
		version: version,
	}
}

// NewErrorMessage constructs a message of KindError kind.
func NewErrorMessage(tag uint16, message string) Message {
	return Message{
		kind:  KindError,
		tag:   tag,
		value: message,
	}
}

func NewAuthMessage(tag uint16, password string) Message {
	return Message{
		kind:  KindAuth,
		tag:   tag,
		value: password,
	}
}

// ForBroadcast returns a copy of the message that's suitable to be broadcasted to
// many connections.
func (m Message) ForBroadcast() Message {
	if m.kind != KindPut {
		panic(fmt.Sprintf("attempting to broadcast a message of kind: %v", m.kind))
	}
	m.tag = 0
	return m
}

// RandomTag is a test helper.
func RandomTag() uint16 {
	return uint16(rand.Int() % 65536)
}

// RandomBytes is a test helper.
func RandomBytes() []byte {
	size := rand.Int() % 64
	key := make([]byte, size)
	rand.Read(key)
	return key
}

// RandomString is a test helper.
func RandomString() string {
	return string(RandomBytes())
}

// RandomVersion is a test helper.
func RandomVersion() uint64 {
	return rand.Uint64()
}

// Generate implements quick.Generator.
func (Message) Generate(rand *rand.Rand, size int) reflect.Value {
	var m Message
	b := make([]byte, size)
	m.kind = Kind(rand.Uint32()) % kindCount
	m.tag = uint16(rand.Uint32() % 65536)
	switch m.kind {
	case KindGet:
		rand.Read(b)
		m.key = string(b)
	case KindPut:
		rand.Read(b)
		m.key = string(b)
		rand.Read(b)
		m.value = string(b)
		m.version = rand.Uint64()
	case KindAuth, KindError:
		rand.Read(b)
		m.value = string(b)
	default:
		panic("programmer error")
	}
	return reflect.ValueOf(m)
}
