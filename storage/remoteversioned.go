package storage

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nicolagi/dino/message"
	"github.com/nicolagi/dino/metadata/client"
	log "github.com/sirupsen/logrus"
)

var (
	ErrTimeout = errors.New("request timed out")
)

type options struct {
	requestTimeout  time.Duration
	responseBackoff time.Duration
	listener        ChangeListener
	authKey         string
}

var defaultOptions = options{
	requestTimeout:  time.Second,
	responseBackoff: time.Second,
}

type Option func(*options)

func WithRequestTimeout(value time.Duration) Option {
	return func(o *options) {
		o.requestTimeout = value
	}
}

func WithResponseBackoff(value time.Duration) Option {
	return func(o *options) {
		o.responseBackoff = value
	}
}

func WithChangeListener(value ChangeListener) Option {
	return func(o *options) {
		o.listener = value
	}
}

func WithAuthKey(value string) Option {
	return func(o *options) {
		o.authKey = value
	}
}

type ChangeListener func(message.Message)

type remoteCall struct {
	tag      uint16
	request  message.Message
	response message.Message
	done     chan struct{}

	prev *remoteCall
	next *remoteCall
}

// RemoteVersionedStore is an implementation of VersionedStore, via a client to a remote
// metadataserver process.
type RemoteVersionedStore struct {
	tags   *message.MonotoneTags
	remote *client.Client
	local  VersionedStore

	opts options

	// Keeps track of goroutines waiting for a response in the do method, and the
	// goroutine running the receive loop. Used to ensure all of those method calls
	// return when Shutdown is called.
	doing sync.WaitGroup

	mu        sync.Mutex
	firstCall *remoteCall
	lastCall  *remoteCall
	stopped   bool

	authorized bool
}

func NewRemoteVersionedStore(remote *client.Client, options ...Option) *RemoteVersionedStore {
	var rs RemoteVersionedStore
	rs.tags = message.NewMonotoneTags()
	rs.remote = remote
	rs.local = NewVersionedWrapper(NewInMemoryStore())
	rs.opts = defaultOptions
	for _, o := range options {
		o(&rs.opts)
	}
	return &rs
}

func (rs *RemoteVersionedStore) newCall(tag uint16, in message.Message) *remoteCall {
	return &remoteCall{
		request: in,
		tag:     tag,
		done:    make(chan struct{}),
	}
}

func (rs *RemoteVersionedStore) linkCall(rc *remoteCall) {
	if rs.lastCall != nil {
		rs.lastCall.next = rc
	} else {
		rs.firstCall = rc
	}
	rs.lastCall = rc
}

func (rs *RemoteVersionedStore) unlinkCall(rc *remoteCall) {
	switch {
	case rc.next == nil && rc.prev == nil:
		rs.firstCall = nil
		rs.lastCall = nil
	case rc.next == nil:
		rs.lastCall = rc.prev
		rc.prev.next = nil
		rc.prev = nil
	case rc.prev == nil:
		rs.firstCall = rc.next
		rc.next.prev = nil
		rc.next = nil
	default:
		rc.prev.next = rc.next
		rc.prev = nil
		rc.next.prev = rc.prev
		rc.next = nil
	}
}

func (rs *RemoteVersionedStore) Start() {
	go rs.receiveLoop()
}

func (rs *RemoteVersionedStore) Stop() {
	rs.mu.Lock()
	rs.stopped = true
	rs.mu.Unlock()

	// The goroutines waiting for a response will timeout (and return
	// ErrCancelledRendezvous). The receive loop will fail the receive because
	// of the connection being closed, and will see the stopped flag is set, and
	// exit.
	rs.remote.Close()
	rs.doing.Wait()

	rs.tags.Stop()
}

func (rs *RemoteVersionedStore) pairResponse(tag uint16, response message.Message) {
	rs.mu.Lock()
	call := rs.firstCall
	for call.tag != tag && call != nil {
		call = call.next
	}
	if call != nil {
		call.response = response
		close(call.done)
		rs.unlinkCall(call)
	} else {
		log.WithFields(log.Fields{
			"message": response,
		}).Debug("Response for no request?")
	}
	rs.mu.Unlock()
}

// do sends a request and waits up to a second for its response.
func (rs *RemoteVersionedStore) do(request message.Message) (response message.Message, err error) {
	rs.doing.Add(1)
	defer rs.doing.Done()
	tag := request.Tag()
	r := rs.newCall(tag, request)
	rs.mu.Lock()
	rs.linkCall(r)
	rs.mu.Unlock()
	if err := rs.remote.Send(request); err != nil {
		rs.mu.Lock()
		rs.unlinkCall(r)
		rs.mu.Unlock()
		return response, err
	}
	select {
	case <-r.done:
		return r.response, nil
	case <-time.After(rs.opts.requestTimeout):
		rs.mu.Lock()
		rs.unlinkCall(r)
		rs.mu.Unlock()
		// Not ideal: The request might be taking longer not because of a
		// networking issue.
		rs.remote.Close()
		return response, ErrTimeout
	}
}

func (rs *RemoteVersionedStore) Put(version uint64, key []byte, value []byte) (err error) {
	if err := rs.ensureAuthorized(); err != nil {
		return err
	}
	request := message.NewPutMessage(rs.tags.Next(), string(key), string(value), version)
	response, err := rs.do(request)
	if err != nil {
		return err
	}
	switch response.Kind() {
	case message.KindPut:
		if request != response {
			log.WithFields(log.Fields{
				"request":  request,
				"response": response,
			}).Error("request and response do not match")
			return fmt.Errorf("request and response do not match")
		}
		return nil
	case message.KindError:
		v := response.Value()
		if v == ErrStalePut.Error() {
			return ErrStalePut
		}
		if strings.Contains(v, "go away") {
			rs.authorized = false
		}
		return errors.New(v)
	default:
		return fmt.Errorf("unexpected response kind: %v", response.Kind())
	}
}

func (rs *RemoteVersionedStore) Get(key []byte) (version uint64, value []byte, err error) {
	if err := rs.ensureAuthorized(); err != nil {
		return 0, nil, err
	}
	version, value, err = rs.local.Get(key)
	if err == nil {
		return
	}
	response, err := rs.do(message.NewGetMessage(rs.tags.Next(), string(key)))
	if err != nil {
		return 0, nil, err
	}
	switch response.Kind() {
	case message.KindPut:
		return response.Version(), []byte(response.Value()), nil
	case message.KindError:
		v := response.Value()
		if strings.HasSuffix(v, "not found") {
			return 0, nil, ErrNotFound
		}
		if strings.Contains(v, "go away") {
			rs.authorized = false
		}
		return 0, nil, errors.New(v)
	default:
		fmt.Println(response.String())
		return 0, nil, fmt.Errorf("unexpected response kind: %v", response.Kind())
	}
}

func (rs *RemoteVersionedStore) ensureAuthorized() error {
	if rs.opts.authKey == "" || rs.authorized {
		return nil
	}
	request := message.NewAuthMessage(rs.tags.Next(), rs.opts.authKey)
	response, err := rs.do(request)
	if err != nil {
		return fmt.Errorf("not authorized: %w", err)
	}
	if response.Kind() == message.KindAuth {
		rs.authorized = true
		return nil
	}
	// Fail closed.
	if response.Kind() == message.KindError {
		return fmt.Errorf("not authorized: %v", response.Value())
	}
	return fmt.Errorf("not authorized, got response of kind %v", response.Kind())
}

func (rs *RemoteVersionedStore) receiveLoop() {
	rs.doing.Add(1)
	defer rs.doing.Done()
	for {
		rs.mu.Lock()
		stopped := rs.stopped
		rs.mu.Unlock()
		if stopped {
			break
		}
		var m message.Message
		if err := rs.remote.Receive(&m); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Error("Receive error")
			rs.mu.Lock()
			stopped := rs.stopped
			rs.mu.Unlock()
			if stopped {
				break
			}
			time.Sleep(rs.opts.responseBackoff)
			continue
		}
		tag := m.Tag()
		if tag != 0 {
			log.WithField("message", m).Debug("Received response")
			rs.pairResponse(tag, m)
		}
		if tag == 0 && m.Kind() == message.KindPut {
			log.WithField("message", m).Debug("Received broadcast")
			lres := ApplyMessage(rs.local, m)
			if lres.Kind() == message.KindError {
				log.WithFields(log.Fields{
					"err": lres,
				}).Error("Could not apply locally")
			} else if rs.opts.listener != nil {
				log.WithField("message", m).Debug("Notifying listener")
				rs.opts.listener(lres)
			}
		}
	}
}
