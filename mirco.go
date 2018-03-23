package mirco

import (
	"sync"
	"strings"
	"strconv"
	"time"
	"log"

	"github.com/sumaig/mirco/registry"

	"github.com/pborman/uuid"
)

type Server interface {
	Options() Options
	Register() error
	Deregister() error
	Start()
	Stop()
	String() string
}

type rpcServer struct {
	sync.RWMutex
	exit		chan bool
	opts		Options
	registered	bool

}

var (
	DefaultAddress        = ":0"
	DefaultName           = "go-mirco"
	DefaultVersion        = "1.0.0"
	DefaultId             = uuid.NewUUID().String()
	DefaultServer  		  = newrpcServer()
)

func newrpcServer(opts ...Option) Server {
	options := newOptions(opts...)
	return &rpcServer{
		opts: options,
		exit: make(chan bool),
	}
}

func NewRpcServer(opts ...Option) Server {
	return newrpcServer(opts...)
}

func (s *rpcServer) Options() Options {
	s.Lock()
	defer s.Unlock()
	opts := s.opts
	return opts
}

func (s *rpcServer) String() string {
	return "rpc"
}

func (s *rpcServer) Register() error {
	config := s.opts
	var (
		advt, host string
		port int
		endpoints []*registry.Endpoint
	)

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	parts := strings.Split(advt, ":")
	if len(parts) > 1 {
		host = strings.Join(parts[:len(parts)-1], ":")
		port, _ = strconv.Atoi(parts[len(parts)-1])
	} else {
		host = parts[0]
	}

	addr, err := extractAddress(host)
	if err != nil {
		return err
	}

	// register service
	node := &registry.Node{
		Id:       config.Name + "-" + config.Id,
		Address:  addr,
		Port:     port,
		Metadata: config.Metadata,
	}

	service := &registry.Service{
		Name:      config.Name,
		Version:   config.Version,
		Nodes:     []*registry.Node{node},
		Endpoints: endpoints,
	}

	s.Lock()
	registered := s.registered
	s.Unlock()

	if !registered {
		log.Printf("Registering node: %s", node.Id)
	}

	// create registry options
	rOpts := []registry.RegisterOption{registry.RegisterTTL(config.RegisterTTL)}

	if err := config.Registry.Register(service, rOpts...); err != nil {
		return err
	}

	if registered {
		return nil
	}

	s.Lock()
	defer s.Unlock()
	s.registered = true

	return nil
}

func (s *rpcServer) Deregister() error {
	config := s.Options()
	var advt, host string
	var port int

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	parts := strings.Split(advt, ":")
	if len(parts) > 1 {
		host = strings.Join(parts[:len(parts)-1], ":")
		port, _ = strconv.Atoi(parts[len(parts)-1])
	} else {
		host = parts[0]
	}

	addr, err := extractAddress(host)
	if err != nil {
		return err
	}

	node := &registry.Node{
		Id:      config.Name + "-" + config.Id,
		Address: addr,
		Port:    port,
	}

	service := &registry.Service{
		Name:    config.Name,
		Version: config.Version,
		Nodes:   []*registry.Node{node},
	}

	log.Printf("Deregistering node: %s", node.Id)
	if err := config.Registry.Deregister(service); err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()

	if !s.registered {
		return nil
	}

	s.registered = false

	return nil
}

func (s *rpcServer) Start() {
	go s.run(s.exit)
}

func (s *rpcServer) run(exit chan bool) {
	if s.opts.RegisterInterval <= time.Duration(0) {
		return
	}

	t := time.NewTicker(s.opts.RegisterInterval)

	for {
		select {
		case <-t.C:
			s.Register()
		case <-exit:
			t.Stop()
			return
		}
	}
}

func(s *rpcServer) Stop() {
	s.Deregister()
	close(s.exit)
}
