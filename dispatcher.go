package iorpc

import (
	"errors"
	"io"
)

var ErrUnknownService = errors.New("unknown service")

type Service uint32

type DispatchHeaders interface {
	IsEmpty() bool
	Marshal(io.Writer) error
	Unmarshal(io.ReadCloser) (any, error)
}

type NoopHeaders struct{}

func (h NoopHeaders) IsEmpty() bool {
	return true
}

func (h NoopHeaders) Marshal(io.Writer) error {
	return nil
}

func (h NoopHeaders) Unmarshal(r io.ReadCloser) (any, error) {
	return nil, r.Close()
}

type DispatchRequest[T DispatchHeaders] struct {
	Service Service
	Headers T
	Size    uint64
	Body    io.ReadCloser
}

type DispatchResponse[T DispatchHeaders] struct {
	Headers T
	Size    uint64
	Body    io.ReadCloser
}

type DispatchHandlerFunc[T, U DispatchHeaders] func(clientAddr string, request DispatchRequest[T]) (response *DispatchResponse[U], err error)

type Dispatcher struct {
	services       []HandlerFunc
	serviceNameMap map[string]Service
}

type DispatchClient[T, U DispatchHeaders] struct {
	service Service
	client  *Client
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		services:       make([]HandlerFunc, 0),
		serviceNameMap: make(map[string]Service),
	}
}

func NewDispatchClient[T, U DispatchHeaders](service Service, client *Client) *DispatchClient[T, U] {
	return &DispatchClient[T, U]{
		service: service,
		client:  client,
	}
}

func Dispatch[T, U DispatchHeaders](dispatch DispatchHandlerFunc[T, U]) HandlerFunc {
	return func(clientAddr string, request Request) (*Response, error) {
		req := DispatchRequest[T]{
			Service: request.Service,
			Size:    request.Size,
			Body:    request.Body,
		}
		if request.Headers != nil && request.Headers.Len() > 0 {
			h, err := req.Headers.Unmarshal(request.Headers)
			if err != nil {
				return nil, err
			}
			headers, ok := h.(T)
			if !ok {
				return nil, errors.New("invalid headers")
			}
			req.Headers = headers
		}

		resp, err := dispatch(clientAddr, req)
		if resp == nil || err != nil {
			return nil, err
		}

		response := &Response{
			Size: resp.Size,
			Body: resp.Body,
		}

		if !req.Headers.IsEmpty() {
			response.Headers = GetBuffer()
			if err := req.Headers.Marshal(response.Headers); err != nil {
				return nil, err
			}
		}
		return response, nil
	}
}

func (d *Dispatcher) AddService(name string, handler HandlerFunc) (srv Service, conflict bool) {
	if _, conflict = d.serviceNameMap[name]; conflict {
		return 0, conflict
	}

	srv = Service(len(d.services))
	d.serviceNameMap[name] = srv
	d.services = append(d.services, handler)
	return srv, false
}

func (d *Dispatcher) GetService(name string) (Service, bool) {
	svc, ok := d.serviceNameMap[name]
	return svc, ok
}

func (d *Dispatcher) MustGetService(name string) Service {
	svc, ok := d.GetService(name)
	if !ok {
		panic("service not found: " + name)
	}
	return svc
}

func (d *Dispatcher) HandlerFunc() HandlerFunc {
	return func(clientAddr string, request Request) (response *Response, err error) {
		service := request.Service
		if int(service) >= len(d.services) {
			return nil, ErrUnknownService
		}
		return d.services[service](clientAddr, request)
	}
}

func (c *DispatchClient[T, U]) Call(header T, bodySize uint64, body io.ReadCloser) (response *DispatchResponse[U], err error) {
	request := Request{
		Service: c.service,
		Size:    bodySize,
		Body:    body,
	}

	if !header.IsEmpty() {
		request.Headers = GetBuffer()
		if err := header.Marshal(request.Headers); err != nil {
			return nil, err
		}
	}

	resp, err := c.client.Call(request)
	if err != nil {
		return nil, err
	}

	response = &DispatchResponse[U]{
		Size: resp.Size,
		Body: resp.Body,
	}

	if resp.Headers != nil && resp.Headers.Len() > 0 {
		h, err := response.Headers.Unmarshal(resp.Headers)
		if err != nil {
			return nil, err
		}
		headers, ok := h.(U)
		if !ok {
			return nil, errors.New("invalid headers")
		}
		response.Headers = headers
	}

	return response, nil
}

func (c *DispatchClient[T, U]) RawClient() *Client {
	return c.client
}
