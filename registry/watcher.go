package registry

import (
	"context"
	"errors"
	"time"

	"github.com/coreos/etcd/clientv3"
)

// Watcher is an interface that returns updates
type Watcher interface {
	// Next is a blocking call
	Next() (*Result, error)
	Stop()
}

// Result is returned by a call to Next on
// the watcher. Actions can be create, update, delete
type Result struct {
	Action  string
	Service *Service
}

type etcdv3Watcher struct {
	stop    chan bool
	w       clientv3.WatchChan
	client  *clientv3.Client
	timeout time.Duration
}

func newEtcdv3Watcher(r *etcdv3Registry, timeout time.Duration) (Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan bool, 1)

	go func() {
		<-stop
		cancel()
	}()

	return &etcdv3Watcher{
		stop:    stop,
		w:       r.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV()),
		client:  r.client,
		timeout: timeout,
	}, nil
}

func (ew *etcdv3Watcher) Next() (*Result, error) {
	for wresp := range ew.w {
		if wresp.Err() != nil {
			return nil, wresp.Err()
		}
		for _, ev := range wresp.Events {
			service := decode(ev.Kv.Value)
			var action string

			switch ev.Type {
			case clientv3.EventTypePut:
				if ev.IsCreate() {
					action = "create"
				} else if ev.IsModify() {
					action = "update"
				}
			case clientv3.EventTypeDelete:
				action = "delete"

				// get service from prevKv
				service = decode(ev.PrevKv.Value)
			}

			if service == nil {
				continue
			}
			return &Result{
				Action:  action,
				Service: service,
			}, nil
		}
	}
	return nil, errors.New("could not get next")
}

func (ew *etcdv3Watcher) Stop() {
	select {
	case <-ew.stop:
		return
	default:
		close(ew.stop)
	}
}
