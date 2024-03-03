package watch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/mandelsoft/engine/pkg/utils"
)

type Client[R, E any] struct {
	dialer ws.Dialer
	url    string
}

func NewClient[R, E any](url string, dialer ...ws.Dialer) *Client[R, E] {
	return &Client[R, E]{
		dialer: utils.OptionalDefaulted(ws.DefaultDialer, dialer...),
		url:    url,
	}
}

func (c *Client[R, E]) Dial(ctx context.Context) (net.Conn, error) {
	conn, _, _, err := c.dialer.Dial(ctx, c.url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *Client[R, E]) RequestWatch(conn net.Conn, req R) (*Watch[E], error) {
	data, _ := json.Marshal(req)
	err := wsutil.WriteClientMessage(conn, ws.OpBinary, data)
	if err != nil {
		return nil, err
	}
	return &Watch[E]{conn: conn}, nil
}

func (c *Client[R, E]) Watch(ctx context.Context, req R) (*Watch[E], error) {
	conn, _, _, err := c.dialer.Dial(ctx, c.url)
	if err != nil {
		return nil, err
	}
	return c.RequestWatch(conn, req)
}

func (c *Client[R, E]) Register(ctx context.Context, req R, h EventHandler[E]) (Syncher, error) {
	w, err := c.Watch(ctx, req)

	if err != nil {
		return nil, err
	}

	s := &syncher{
		wait: &sync.WaitGroup{},
		err:  nil,
	}
	s.wait.Add(1)

	go func() {
		select {
		case <-ctx.Done():
			w.Close()
		}
	}()

	go func() {
		defer s.wait.Done()
		for {
			events, err := w.Receive()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					s.err = err
				}
				w.Close()
				break
			}
			for _, e := range events {
				h.Handle(e)
			}
		}
	}()
	return s, nil
}

////////////////////////////////////////////////////////////////////////////////

type Syncher interface {
	Wait() error
}

type syncher struct {
	wait *sync.WaitGroup
	err  error
}

func (s *syncher) Wait() error {
	s.wait.Wait()
	return s.err
}

////////////////////////////////////////////////////////////////////////////////

type Watch[E any] struct {
	conn net.Conn
}

func (w *Watch[E]) Receive() ([]E, error) {
	msgs, err := wsutil.ReadServerMessage(w.conn, nil)
	if err != nil {
		return nil, err
	}

	var events []E
	for _, m := range msgs {
		var evt E

		err := json.Unmarshal(m.Payload, &evt)
		if err != nil {
			return nil, err
		}
		events = append(events, evt)
	}
	return events, nil
}

func (w *Watch[E]) Close() error {
	return w.conn.Close()
}
