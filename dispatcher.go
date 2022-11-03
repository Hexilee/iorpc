package iorpc

import "errors"

var ErrUnknownService = errors.New("unknown service")
var ErrInvalidHeadersType = errors.New("invalid headers type")

type Service uint32

type GenericRequest[T Headers] struct {
	Addr    string
	Service Service
	Headers T
	Body    Body
}

type GenericResponse[T Headers] struct {
	Headers T
	Body    Body
}

type GenericHandler[T, U Headers] func(request GenericRequest[T]) (response *GenericResponse[U], err error)

type Dispatcher struct {
	services       []Handler
	serviceNameMap map[string]Service
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		services:       make([]Handler, 0),
		serviceNameMap: make(map[string]Service),
	}
}

func (d *Dispatcher) AddServiceFunc(name string, handler HandlerFunc) (srv Service, conflict bool) {
	return d.AddService(name, handler)
}

func (d *Dispatcher) AddService(name string, handler Handler) (srv Service, conflict bool) {
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

func (h GenericHandler[T, U]) Handle(clientAddr string, request Request) (response *Response, err error) {
	reqHeaders, ok := request.Headers.(T)
	if !ok {
		return nil, ErrInvalidHeadersType
	}
	resp, err := h(GenericRequest[T]{
		Addr:    clientAddr,
		Service: request.Service,
		Headers: reqHeaders,
		Body:    request.Body,
	})
	if err != nil {
		return nil, err
	}
	return &Response{
		Headers: resp.Headers,
		Body:    resp.Body,
	}, nil
}

func (d *Dispatcher) Handle(clientAddr string, request Request) (response *Response, err error) {
	service := request.Service
	if int(service) >= len(d.services) {
		return nil, ErrUnknownService
	}
	return d.services[service].Handle(clientAddr, request)
}
